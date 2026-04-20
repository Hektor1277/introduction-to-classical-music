const pages = [...document.querySelectorAll("[data-page]")];
const libraryName = document.querySelector("#launcher-library-name");
const detailTitle = document.querySelector("#launcher-detail-title");
const statusHost = document.querySelector("#launcher-library-status");
const feedback = document.querySelector("#launcher-feedback");
const actionButtons = [...document.querySelectorAll("[data-launch-action]")];
const libraryActionButtons = [...document.querySelectorAll("[data-library-action]")];
const viewButtons = [...document.querySelectorAll("[data-view-action]")];
const windowButtons = [...document.querySelectorAll("[data-window-action]")];

const compact = (value) => String(value ?? "").trim();

const formatDateTime = (value) => {
  const normalized = compact(value);
  if (!normalized) {
    return "未构建";
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return normalized;
  }
  return parsed.toLocaleString("zh-CN", { hour12: false });
};

const buildCountSummary = (counts = {}) => {
  const items = [
    ["作曲家", counts.composers],
    ["人物", counts.people],
    ["作品", counts.works],
    ["版本", counts.recordings],
  ];
  return items
    .filter(([, value]) => Number.isFinite(Number(value)))
    .map(([label, value]) => `${label} ${value}`)
    .join(" / ");
};

const setView = (name) => {
  pages.forEach((page) => {
    const active = page.dataset.page === name;
    page.hidden = !active;
    page.classList.toggle("launcher-page--active", active);
  });
};

const renderLibraryStatus = async () => {
  try {
    const summary = await window.desktopLauncher.getLibraryStatus();
    const name = summary?.manifest?.libraryName || "未命名库";
    libraryName.textContent = name;
    detailTitle.textContent = name;

    const detailItems = [
      { label: "库名称", value: name },
      { label: "协议版本", value: summary?.manifest?.schemaVersion || "未知" },
      { label: "运行模式", value: summary?.mode || "bundle" },
      { label: "最近构建", value: formatDateTime(summary?.lastBuiltAt) },
      { label: "条目概览", value: buildCountSummary(summary?.counts || {}) || "暂无条目", wide: true },
      { label: "库路径", value: summary?.rootDir || "未设置", wide: true },
    ];

    statusHost.innerHTML = detailItems
      .map(
        (item) => `
          <div${item.wide ? ' data-wide="true"' : ""}>
            <dt>${item.label}</dt>
            <dd>${item.value}</dd>
          </div>
        `,
      )
      .join("");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    libraryName.textContent = "读取失败";
    detailTitle.textContent = "当前活动库";
    statusHost.innerHTML = `
      <div>
        <dt>状态</dt>
        <dd>${message}</dd>
      </div>
    `;
  }
};

const runAction = async (action) => {
  feedback.textContent = "正在启动，请稍候。";
  try {
    if (action === "library") {
      await window.desktopLauncher.openLibrary();
      feedback.textContent = "本地不全书已打开。";
      await renderLibraryStatus();
      return;
    }
    if (action === "owner") {
      await window.desktopLauncher.openOwner();
      feedback.textContent = "维护工具已打开。";
      return;
    }
    await window.desktopLauncher.openRetrieval();
    feedback.textContent = "版本检索工具已打开。";
  } catch (error) {
    feedback.textContent = error instanceof Error ? error.message : String(error);
  }
};

const runLibraryAction = async (action) => {
  try {
    if (action === "import") {
      feedback.textContent = "正在导入库，请稍候。";
      const result = await window.desktopLauncher.importLibrary();
      if (result?.cancelled) {
        feedback.textContent = "已取消导入。";
        return;
      }
      await renderLibraryStatus();
      feedback.textContent = `已导入：${result?.manifest?.libraryName || "未命名库"}`;
      return;
    }
    if (action === "export") {
      feedback.textContent = "正在导出库，请稍候。";
      const result = await window.desktopLauncher.exportLibrary();
      if (result?.cancelled) {
        feedback.textContent = "已取消导出。";
        return;
      }
      feedback.textContent = `已导出到：${result?.exportedRoot || ""}`;
      return;
    }
    const result = await window.desktopLauncher.openLibraryFolder();
    feedback.textContent = result?.rootDir ? `已打开库目录：${result.rootDir}` : "已打开库目录。";
  } catch (error) {
    feedback.textContent = error instanceof Error ? error.message : String(error);
  }
};

actionButtons.forEach((button) => {
  button.addEventListener("click", () => {
    runAction(button.dataset.launchAction);
  });
});

libraryActionButtons.forEach((button) => {
  button.addEventListener("click", () => {
    runLibraryAction(button.dataset.libraryAction);
  });
});

viewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setView(button.dataset.viewAction === "details" ? "details" : "main");
  });
});

windowButtons.forEach((button) => {
  button.addEventListener("click", async () => {
    if (button.classList.contains("launcher-window-button--disabled")) {
      return;
    }
    try {
      await window.desktopLauncher.windowControl(button.dataset.windowAction);
    } catch (error) {
      feedback.textContent = error instanceof Error ? error.message : String(error);
    }
  });
});

setView("main");

requestAnimationFrame(() => {
  window.setTimeout(() => {
    void renderLibraryStatus();
  }, 40);
});
