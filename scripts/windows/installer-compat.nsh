!include "LogicLib.nsh"

!macro ResetInstallDirToCurrentLayout
  StrCpy $INSTDIR "$LOCALAPPDATA\Programs\${APP_FILENAME}"
!macroend

!macro customInit
  ${If} $installMode == "CurrentUser"
    ${If} $INSTDIR == "$LOCALAPPDATA\Programs\introduction-to-classical-music"
      !insertmacro ResetInstallDirToCurrentLayout
    ${EndIf}
  ${EndIf}
!macroend

!macro RunLegacyInstallCleanup LEGACY_INSTALL_DIR
  InitPluginsDir
  File "/oname=$PLUGINSDIR\legacy-install-cleanup.ps1" "${PROJECT_DIR}\scripts\windows\legacy-install-cleanup.ps1"
  nsExec::ExecToLog '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "$PLUGINSDIR\legacy-install-cleanup.ps1" -LegacyInstallDir "${LEGACY_INSTALL_DIR}" -AppDataDir "$APPDATA\Introduction to Classical Music"'
  Pop $R9
!macroend

!macro customUnInstallCheck
  ${If} $R0 != 0
    StrCpy $R8 "$LOCALAPPDATA\Programs\introduction-to-classical-music"
    IfFileExists "$R8\不全书.exe" 0 done
    DetailPrint "Legacy install cleanup fallback: $R8"
    !insertmacro RunLegacyInstallCleanup "$R8"
    ${If} $R9 == 0
      DeleteRegKey HKCU "${INSTALL_REGISTRY_KEY}"
      DeleteRegKey HKCU "${UNINSTALL_REGISTRY_KEY}"
      !ifdef UNINSTALL_REGISTRY_KEY_2
        DeleteRegKey HKCU "${UNINSTALL_REGISTRY_KEY_2}"
      !endif
      !insertmacro ResetInstallDirToCurrentLayout
      ClearErrors
      StrCpy $R0 0
    ${EndIf}
  done:
  ${EndIf}
!macroend
