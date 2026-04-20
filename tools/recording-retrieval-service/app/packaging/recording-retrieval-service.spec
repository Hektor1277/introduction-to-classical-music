# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

project_root = Path(SPECPATH).resolve().parent
ui_root = project_root / "app" / "ui"
materials_root = project_root / "materials"
config_root = project_root / "config"
safe_config_files = [
    config_root / ".gitkeep",
    config_root / "llm.example.json",
    config_root / "platform-search.example.json",
]

a = Analysis(
    [str(project_root / "app" / "main.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=[
        (str(ui_root), "app/ui"),
        (str(materials_root), "materials"),
        *[(str(path), f"config/{path.name}") for path in safe_config_files if path.is_file()],
    ],
    hiddenimports=[
        "playwright",
        "playwright.async_api",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="recording-retrieval-service",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="recording-retrieval-service",
)
