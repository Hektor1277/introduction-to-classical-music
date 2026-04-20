import { describe, expect, it } from "vitest";
import { promises as fs } from "node:fs";
import path from "node:path";

import {
  buildBatchWorkOptionLabel,
  buildBatchRelationOptions,
  buildBatchPreviewShellHtml,
  buildBatchResultSummary,
  buildSearchResultBadges,
  buildRecordingLinkChipLabel,
  buildRecordingLinkEditorHtml,
  filterMergeTargetOptions,
  getProposalModeAttributes,
  resolveProposalActionContext,
  buildPreferredWorkLabel,
  createEmptyActiveEntity,
  selectBatchSessionAfterRefresh,
} from "../../apps/owner/web/ui-helpers.js";

describe("owner ui helpers", () => {
  it("builds batch preview shell with the preview list above the detail panel", () => {
    const html = buildBatchPreviewShellHtml("<div>preview</div>", "<div>detail</div>");

    expect(html.indexOf("owner-batch-preview-shell__list")).toBeLessThan(html.indexOf("owner-batch-preview-shell__detail"));
  });

  it("returns a blank active entity context for create mode", () => {
    expect(createEmptyActiveEntity()).toEqual({ type: "", id: "" });
  });

  it("summarizes batch analyze results without leaking the full session payload", () => {
    const summary = buildBatchResultSummary("analyze", {
      session: {
        id: "batch-1",
        status: "analyzed",
        draftEntities: {
          composers: [{ entity: { id: "composer-1" } }],
          people: [{ entity: { id: "person-1" } }, { entity: { id: "person-2" } }],
          works: [{ entity: { id: "work-1" } }],
          recordings: [{ entity: { id: "recording-1" } }],
        },
        warnings: ["warning-a"],
      },
      run: { id: "run-1" },
    });

    expect(summary).toEqual({
      action: "analyze",
      sessionId: "batch-1",
      status: "analyzed",
      counts: {
        composers: 1,
        people: 2,
        works: 1,
        recordings: 1,
      },
      warnings: ["warning-a"],
      runId: "run-1",
    });
    expect("session" in summary).toBe(false);
  });

  it("keeps the batch panel empty after abandoning the current session", () => {
    const nextSession = selectBatchSessionAfterRefresh(
      [
        { id: "batch-old", status: "analyzed" },
        { id: "batch-new", status: "checked" },
      ],
      "batch-new",
      true,
    );

    expect(nextSession).toBeNull();
  });

  it("prefers the current batch session when auto-restoring is allowed", () => {
    const nextSession = selectBatchSessionAfterRefresh(
      [
        { id: "batch-old", status: "analyzed" },
        { id: "batch-new", status: "checked" },
      ],
      "batch-new",
      false,
    );

    expect(nextSession).toEqual({ id: "batch-new", status: "checked" });
  });

  it("numbers duplicate recording link chips by platform while preserving unique labels", () => {
    const links = [
      { platform: "YouTube", url: "https://example.com/1" },
      { platform: "bilibili", url: "https://example.com/2" },
      { platform: "YouTube", url: "https://example.com/3" },
    ];

    expect(buildRecordingLinkChipLabel(links[0], 0, links)).toBe("YouTube1");
    expect(buildRecordingLinkChipLabel(links[1], 1, links)).toBe("bilibili");
    expect(buildRecordingLinkChipLabel(links[2], 2, links)).toBe("YouTube2");

    const html = buildRecordingLinkEditorHtml(links);
    expect(html).toContain('data-recording-link-index="0"');
    expect(html).toContain(">YouTube1<");
    expect(html).toContain(">bilibili<");
    expect(html).toContain(">YouTube2<");
  });

  it("builds batch relation options from draft entities first and preserves missing current relations", () => {
    const options = buildBatchRelationOptions(
      "work",
      "composerId",
      {
        composers: [{ id: "composer-library", name: "Library Composer CN", nameLatin: "Library Composer" }],
      },
      {
        composers: [{ entity: { id: "composer-draft", name: "Draft Composer CN", nameLatin: "Draft Composer" } }],
      },
      "composer-legacy",
    );

    expect(options).toEqual([
      { value: "", label: "请选择" },
      { value: "composer-draft", label: "Draft Composer CN / Draft Composer" },
      { value: "composer-library", label: "Library Composer CN / Library Composer" },
      { value: "composer-legacy", label: "当前关联（composer-legacy）" },
    ]);
  });

  it("builds preferred work labels with bilingual titles, catalogue and composer context", () => {
    const label = buildPreferredWorkLabel(
      {
        id: "work-mahler-5",
        composerId: "composer-mahler",
        title: "Mahler No. 5 in C-sharp minor",
        titleLatin: "Symphony No. 5 in C-sharp minor",
        catalogue: "",
      },
      [{ id: "composer-mahler", name: "Mahler", nameLatin: "Gustav Mahler" }],
    );

    expect(label).toBe("Mahler No. 5 in C-sharp minor / Symphony No. 5 in C-sharp minor / Mahler / Gustav Mahler");
  });
  it("builds batch work labels without composer names", () => {
    const label = buildBatchWorkOptionLabel({
      id: "work-beethoven-5",
      composerId: "composer-beethoven",
      title: "第五交响曲",
      titleLatin: "Symphony No. 5 in C minor",
      catalogue: "Op. 67",
    });

    expect(label).toBe("第五交响曲 / Symphony No. 5 in C minor / Op. 67");
  });

  it("deduplicates catalogue text when the original title already contains the catalogue", () => {
    const preferredLabel = buildPreferredWorkLabel(
      {
        id: "bruckner-7",
        composerId: "composer-bruckner",
        title: "第七交响曲",
        titleLatin: "Symphony No.7 in E major, WAB 107",
        catalogue: "WAB 107",
      },
      [{ id: "composer-bruckner", name: "安东·布鲁克纳", nameLatin: "Anton Bruckner" }],
    );
    const batchLabel = buildBatchWorkOptionLabel({
      id: "bruckner-7",
      composerId: "composer-bruckner",
      title: "第七交响曲",
      titleLatin: "Symphony No.7 in E major, WAB 107",
      catalogue: "WAB 107",
    });

    expect(preferredLabel).toBe("第七交响曲 / Symphony No.7 in E major / WAB 107 / 安东·布鲁克纳 / Anton Bruckner");
    expect(batchLabel).toBe("第七交响曲 / Symphony No.7 in E major / WAB 107");
  });

  it("keeps proposal ids on cards and uses a separate target attribute on action buttons", () => {
    const attributes = getProposalModeAttributes();

    expect(attributes.proposalIdAttr).toBe("data-owner-proposal-id");
    expect(attributes.proposalTargetIdAttr).toBe("data-owner-proposal-target-id");
  });

  it("resolves unified proposal action context from the card instead of the button wrapper", () => {
    const context = resolveProposalActionContext(
      {
        dataset: {
          ownerProposalAction: "confirm",
          ownerProposalTargetId: "proposal-1",
          ownerProposalId: "stale-button-id",
        },
      },
      {
        dataset: {
          ownerProposalId: "proposal-1",
          ownerProposalMode: "batch",
          ownerRunId: "run-1",
        },
      },
    );

    expect(context).toEqual({
      proposalId: "proposal-1",
      action: "confirm",
      mode: "batch",
      runId: "run-1",
    });
  });

  it("derives owner search badges from entity roles instead of collapsing groups into generic people", () => {
    expect(buildSearchResultBadges({ type: "composer" })).toEqual(["人物", "作曲家"]);
    expect(buildSearchResultBadges({ type: "composer", roles: ["composer", "conductor"] })).toEqual(["人物", "作曲家", "指挥"]);
    expect(buildSearchResultBadges({ type: "person", roles: ["orchestra"] })).toEqual(["团体", "乐团"]);
    expect(buildSearchResultBadges({ type: "person", roles: ["conductor", "soloist"] })).toEqual(["人物", "指挥", "独奏"]);
  });

  it("filters merge target options with a search query", () => {
    const options = filterMergeTargetOptions(
      [
        { value: "bpo", label: "Berlin Philharmonic Orchestra" },
        { value: "cso", label: "Chicago Symphony Orchestra" },
        { value: "rso", label: "Rundfunk-Sinfonieorchester Berlin" },
      ],
      "berlin",
    );

    expect(options).toEqual([
      { value: "bpo", label: "Berlin Philharmonic Orchestra" },
      { value: "rso", label: "Rundfunk-Sinfonieorchester Berlin" },
    ]);
  });

  it("keeps dialog centering rules in the owner stylesheet", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-dialog\s*\{[\s\S]*margin:\s*auto/i);
    expect(css).toMatch(/\.owner-dialog\[open\][\s\S]*display:\s*grid/i);
  });

  it("stretches inline-check content to fill the detail workspace without restoring the old stacked layout", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-card--detail\s+\.owner-inline-check\s*\{[\s\S]*height:\s*100%/i);
    expect(css).toMatch(/\.owner-card--detail\s+#owner-inline-check-panel\s*\{[\s\S]*height:\s*100%/i);
    expect(css).toMatch(/\.owner-card--detail\s+\.owner-inline-check__summary\s*\{[\s\S]*display:\s*flex/i);
  });

  it("treats inline auto-check as a replacement workspace instead of stacking the entity form underneath", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(script).toContain('detailCard?.classList.add("is-inline-check-active")');
    expect(script).toContain('detailCard?.classList.remove("is-inline-check-active")');
    expect(css).toMatch(
      /\.owner-card--detail\.is-inline-check-active\s+\.owner-tab-panels\s*\{[\s\S]*display:\s*none/i,
    );
  });

  it("uses generic entity action labels and removes the preview action from owner forms at runtime", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain('resetButton.textContent = "新建条目"');
    expect(script).toContain('saveButton.textContent = "保存条目"');
    expect(script).toContain('deleteButton.textContent = "删除条目"');
    expect(script).toContain("previewButton.remove()");
  });

  it("keeps merge search results inside a bounded dropdown panel", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-merge-combobox__results\s*\{[\s\S]*max-height:\s*18rem/i);
    expect(css).toMatch(/\.owner-merge-combobox__results\s*\{[\s\S]*overflow:\s*auto/i);
    expect(css).toMatch(/\.owner-merge-combobox__panel\[hidden\]\s*\{[\s\S]*display:\s*none\s*!important/i);
  });

  it("normalizes the detail tabs into person and group views while keeping composers reachable through the person tab", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain("const getManagedComposers = (library = state.library) => {");
    expect(script).toContain('composerButton.hidden = true');
    expect(script).toContain('groupButton.dataset.detailTab = "group"');
    expect(script).toContain('personButton.textContent = "人物"');
    expect(script).toContain('? "group" : "person"');
    expect(script).toContain('if (entityType === "composer") return getManagedComposers(library);');
    expect(script).toContain('panel: entityType === "composer" ? "person" : undefined');
  });

  it("shows composer as a manageable role in owner forms", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");

    expect(script).toContain('const PERSON_ROLE_VALUES = new Set(["composer", "conductor", "soloist", "singer", "instrumentalist"])');
    expect(script).toContain('<option value="composer">');
    expect(html).toContain('value="composer"');
    expect(html).toContain("作曲家");
  });

  it("shows structured recording fields and explains that slug and sortKey are auto-managed", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");

    expect(html).toContain('name="workTypeHint"');
    expect(html).toContain('name="conductorPersonId"');
    expect(html).toContain('name="orchestraPersonId"');
    expect(html).toContain("保存时自动生成");
  });

  it("refreshes the owner library after confirming batch drafts so new coarse entries become searchable immediately", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toMatch(/const confirmBatchCreate = async \(\) => \{[\s\S]*await refreshLibrary\(\);[\s\S]*setResult\(buildBatchResultSummary\("confirm-create"/i);
  });

  it("adds a structured multi-credit editor to the recording form instead of relying only on the legacy textarea", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(html).toContain("data-recording-credit-editor");
    expect(html).toContain("data-recording-credit-add");
    expect(script).toContain("renderRecordingCreditEditor");
    expect(script).toContain("syncRecordingCreditsField");
  });

  it("keeps recording credits in a hidden canonical payload while primary conductor and orchestra selects only sync shortcut rows", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(html).toContain('name="credits" type="hidden"');
    expect(script).toContain("readRecordingCreditsFromEditor");
    expect(script).toContain("upsertPrimaryRecordingCredit(form, \"conductor\"");
    expect(script).toContain("upsertPrimaryRecordingCredit(form, \"orchestra\"");
    expect(script).toContain("syncPrimaryRecordingCreditSelects");
  });

  it("keeps slug and sortKey as hidden managed fields instead of visible manual inputs on entity forms", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");

    expect(html).toMatch(/data-entity-form="composer"[\s\S]*?<input type="hidden" name="slug"/i);
    expect(html).toMatch(/data-entity-form="composer"[\s\S]*?<input type="hidden" name="sortKey"/i);
    expect(html).toMatch(/data-entity-form="person"[\s\S]*?<input type="hidden" name="slug"/i);
    expect(html).toMatch(/data-entity-form="person"[\s\S]*?<input type="hidden" name="sortKey"/i);
    expect(html).toMatch(/data-entity-form="work"[\s\S]*?<input type="hidden" name="slug"/i);
    expect(html).toMatch(/data-entity-form="work"[\s\S]*?<input type="hidden" name="sortKey"/i);
    expect(html).toMatch(/data-entity-form="recording"[\s\S]*?<input type="hidden" name="slug"/i);
    expect(html).toMatch(/data-entity-form="recording"[\s\S]*?<input type="hidden" name="sortKey"/i);
  });

  it("hides internal work/article routing fields and explains that they are auto-generated", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(html).toMatch(/data-entity-form="work"[\s\S]*?<input type="hidden" name="groupPath"/i);
    expect(html).not.toContain('label>类型路径（用 / 分隔）<input name="groupPath"');
    expect(html).toMatch(/id="article-form"[\s\S]*?<input type="hidden" name="slug"/i);
    expect(html).not.toContain('label>路径别名（Slug）<input name="slug"');
    expect(script).toContain("buildInferredWorkGroupPath");
    expect(script).toContain("createSlugLike(compact(articleForm.elements.title.value) || \"article\")");
  });

  it("exposes the full recording credit role set for multi-person and multi-group versions", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain('["conductor", "指挥"]');
    expect(script).toContain('["orchestra", "乐团"]');
    expect(script).toContain('["chorus", "合唱"]');
    expect(script).toContain('["ensemble", "组合"]');
    expect(script).toContain('["soloist", "独奏"]');
    expect(script).toContain('["singer", "歌手"]');
    expect(script).toContain('["instrumentalist", "器乐"]');
  });

  it("keeps empty review content aligned to the top instead of pushing controls downward", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-card--detail\s+\.owner-inline-check__summary\s*\{[\s\S]*justify-content:\s*flex-start/i);
    expect(css).not.toMatch(/\.owner-card--detail\s+\.owner-inline-check__summary\s*>\s*\.owner-job-detail__section:last-child[\s\S]*margin-top:\s*auto/i);
  });

  it("keeps the top-level owner cards pinned to the top edge instead of vertically centering sparse review content", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");

    expect(css).toMatch(/\.owner-panel--main\s*\{[\s\S]*grid-template-rows:\s*auto\s+minmax\(0,\s*1fr\)/i);
    expect(html).toContain('class="owner-panel-main-scroll"');
    expect(css).toMatch(/\.owner-panel-main-scroll\s*\{[\s\S]*overflow-y:\s*auto/i);
    expect(css).toMatch(/\.owner-view\.is-active,\s*\.owner-panel__inner\.is-active\s*\{[\s\S]*align-content:\s*start/i);
    expect(css).toMatch(/\.owner-panel--main\s*>\s*\.owner-tabs\s*\{[\s\S]*position:\s*relative/i);
    expect(css).toMatch(/\.owner-panel--main\s*>\s*\.owner-tabs\s*\{[\s\S]*background:\s*rgba\(252,\s*247,\s*238,\s*0\.98\)/i);
    expect(css).toMatch(/\.owner-view\s+\.owner-card\s*\{[\s\S]*align-content:\s*start/i);
  });

  it("renders owner search result cards with content-driven height instead of a fixed tall shell", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-result-list\s*\{[\s\S]*display:\s*flex/i);
    expect(css).toMatch(/\.owner-result-list\s*\{[\s\S]*flex-direction:\s*column/i);
    expect(css).toMatch(/\.owner-card--search\s+\.owner-result-item\s*\{[\s\S]*display:\s*block/i);
    expect(css).toMatch(/\.owner-card--search\s+\.owner-result-item\s*\{[\s\S]*(flex:\s*0\s+0\s+auto|flex-shrink:\s*0)/i);
    expect(css).toMatch(/\.owner-result-item\s*\{[\s\S]*height:\s*auto/i);
    expect(css).toMatch(/\.owner-result-item\s*\{[\s\S]*min-height:\s*0/i);
    expect(css).not.toMatch(/\.owner-result-item\s*\{[\s\S]*min-height:\s*12\.5rem/i);
  });

  it("shows role checkboxes in a single wrapped row and adds related-entity jump controls to entity forms", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(css).toMatch(/\.owner-role-grid\s*\{[\s\S]*display:\s*grid/i);
    expect(css).toMatch(/\.owner-role-grid\s*\{[\s\S]*grid-template-columns:\s*repeat\(5,\s*minmax\(0,\s*1fr\)\)/i);
    expect(html).toContain("owner-role-grid");
    expect(script).toContain("data-related-entity-select");
    expect(script).toContain("data-related-entity-jump");
  });

  it("renders related-entity controls ahead of merge controls and groups selectable relations", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(script).toContain("mergeHost.before(host)");
    expect(script).toContain("<optgroup");
    expect(script).toContain("groupLabel");
    expect(ownerServer).toContain('groupLabel: "作曲家"');
    expect(ownerServer).toContain('return "作品 / " + getRecordingWorkTypeHintLabel(resolvedWorkType);');
    expect(ownerServer).toContain("getRelatedRecordingGroupLabel");
    expect(ownerServer).toContain('return "版本 / " + composerLabel + " / " + workLabel;');
  });

  it("refreshes review and log panels when switching views instead of leaving stale proposal cards on screen", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain('if (viewName === "review")');
    expect(script).toContain("void renderReviewRun().catch");
    expect(script).toContain('if (viewName === "logs")');
    expect(script).toContain("void renderLogRun().catch");
  });

  it("shows pending review counts in the run selector so applied runs are not mislabeled as active candidates", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain("run.summary.pending");
  });

  it("marks blocked proposals in the review UI and checks them before bulk apply", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");
    const ownerServer = await fs.readFile(path.resolve("apps/owner/server/owner-app.ts"), "utf8");

    expect(script).toContain("getBlockedProposalsForReviewAction");
    expect(script).toContain("buildBlockedReviewActionMessage");
    expect(script).toContain("owner-proposal--blocked");
    expect(script).toContain("阻止应用");
    expect(css).toMatch(/\.owner-proposal--blocked\s*\{/i);
    expect(ownerServer).toContain("collectAutomationProposalApplyBlockers");
  });

  it("treats info panel collection links as a dedicated structured-link host instead of falling back to recording links", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");

    expect(html).toContain("data-info-panel-link-host");
    expect(script).toContain('const getInfoPanelLinksField = (host) =>');
    expect(script).toContain('resolveInfoPanelForm(host)?.elements?.infoPanelCollectionLinks');
    expect(script).toMatch(/const getStructuredLinkMode = \(host\) =>[\s\S]*data-info-panel-link-host/);
    expect(script).toMatch(/const getStructuredLinkMode = \(host\) =>[\s\S]*getInfoPanelLinksField\(host\)/);
    expect(script).toContain('const resolveInfoPanelForm = (host) => (host?.elements ? host : host?.closest?.("form")) || null;');
    expect(script).toMatch(/if \(mode === "info-panel"\) \{[\s\S]*const form = resolveInfoPanelForm\(host\);[\s\S]*renderInfoPanelState\(form,/);
    expect(script).toMatch(/if \(infoPanelLinkButton\) \{[\s\S]*event\.stopPropagation\(\);[\s\S]*openRecordingLinkDialog\(resolveRecordingLinkHost\(infoPanelLinkButton\), Number\(infoPanelLinkButton\.dataset\.infoPanelLinkEdit \|\| -1\)\);[\s\S]*return;/);
    expect(html).toContain('/styles.css?v=20260415a');
    expect(html).toContain('/app.js?v=20260415a');
  });

  it("keeps composer translated in the runtime role label map instead of falling back to the raw english value", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toMatch(/const PERSON_FORM_ROLE_LABELS = \{[\s\S]*composer:/);
  });

  it("removes the dead auto-check current summary row and turns sidebar issues into direct jump buttons", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(html).not.toContain('id="automation-job-current"');
    expect(script).toContain('class="owner-issue-item owner-issue-item--jump"');
    expect(script).toContain('data-issue-entity-type="${escapeHtml(issue.entityType)}"');
    expect(script).toContain('data-issue-entity-id="${escapeHtml(issue.entityId)}"');
    expect(script).toContain('issuesPanel.addEventListener("click"');
    expect(script).toContain('await loadEntity(button.dataset.issueEntityType, button.dataset.issueEntityId);');
    expect(css).toMatch(/\.owner-issue-item--jump:hover,/i);
  });

  it("keeps the recording detail form within the card width and moves scrollbars closer to the panel edge", async () => {
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(html).toContain('class="owner-form__recording-sidecar"');
    expect(html).toContain('class="owner-form__recording-meta"');
    expect(css).toMatch(/\.owner-panel__inner\.is-active\s*\{[\s\S]*width:\s*100%/i);
    expect(css).toMatch(/\.owner-panel__inner\.is-active\s*\{[\s\S]*justify-self:\s*stretch/i);
    expect(css).toMatch(/\.owner-form\s*\{[\s\S]*width:\s*100%/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s*\{[\s\S]*grid-template-columns:\s*repeat\(2,\s*minmax\(0,\s*1fr\)\)/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s*\{[\s\S]*width:\s*100%/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s*\{[\s\S]*gap:\s*0\.9rem\s+1rem/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+>\s*\*\s*\{[\s\S]*min-width:\s*0/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__hero-fields\s*\{[\s\S]*display:\s*contents/i);
    expect(css).toMatch(/\.owner-form__recording-sidecar\s*\{[\s\S]*grid-template-columns:\s*minmax\(0,\s*1fr\)\s+11rem/i);
    expect(css).toMatch(/\.owner-form__recording-sidecar\s*\{[\s\S]*grid-row:\s*4/i);
    expect(css).toMatch(/\.owner-form__recording-sidecar\s*\{[\s\S]*grid-column:\s*1\s*\/\s*-1/i);
    expect(css).toMatch(/\.owner-form__recording-meta\s*\{[\s\S]*display:\s*grid/i);
    expect(css).toMatch(/\.owner-form__recording-meta\s*\{[\s\S]*grid-template-rows:\s*repeat\(3,\s*minmax\(0,\s*1fr\)\)/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__hero-field:nth-child\(7\)\s*\{[\s\S]*grid-row:\s*5/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__hero-field:nth-child\(8\)\s*\{[\s\S]*grid-column:\s*2/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__hero-field:nth-child\(9\)\s*\{[\s\S]*grid-column:\s*1\s*\/\s*-1/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__hero-field:nth-child\(10\)\s*\{[\s\S]*grid-column:\s*1\s*\/\s*-1/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__media\s*\{[\s\S]*grid-column:\s*2/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__media\s*\{[\s\S]*justify-self:\s*end/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-form__media\s*\{[\s\S]*width:\s*11rem/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-image-preview\s*\{[\s\S]*width:\s*11rem/i);
    expect(css).toMatch(/\.owner-form__hero--recording\s+\.owner-image-preview\s*\{[\s\S]*min-height:\s*11rem/i);
    expect(css).toMatch(/@media\s*\(max-width:\s*1180px\)\s*\{[\s\S]*\.owner-form__hero--recording\s*\{[\s\S]*grid-template-columns:\s*1fr/i);
    expect(css).toMatch(/@media\s*\(max-width:\s*1180px\)\s*\{[\s\S]*\.owner-form__recording-sidecar\s*\{[\s\S]*grid-column:\s*1/i);
    expect(css).toMatch(/\.owner-form select[\s\S]*text-overflow:\s*ellipsis/i);
    expect(css).toMatch(/\.owner-card--detail\s+\.owner-tab-panels\s*\{[\s\S]*overflow:\s*auto/i);
    expect(css).toMatch(/\.owner-card--detail\s+\.owner-tab-panels\s*\{[\s\S]*padding-right:\s*0\.03rem/i);
    expect(css).toMatch(/\.owner-card--detail\s+\.owner-tab-panels\s*\{[\s\S]*scrollbar-gutter:\s*stable/i);
    expect(css).toMatch(/\.owner-panel--sidebar\s*\{[\s\S]*overflow-y:\s*auto/i);
    expect(css).toMatch(/\.owner-panel-main-scroll\s*\{[\s\S]*overflow-y:\s*auto/i);
    expect(css).toMatch(/\.owner-panel--sidebar,\s*\.owner-panel--main\s*\{[\s\S]*overflow-x:\s*hidden/i);
    expect(css).toMatch(/\.owner-panel--sidebar\s*\{[\s\S]*padding-right:\s*0\.9rem/i);
    expect(css).toMatch(/\.owner-panel--main\s*\{[\s\S]*overflow:\s*hidden/i);
    expect(css).toMatch(/\.owner-card--detail\s*\{[\s\S]*max-height:\s*min\(76vh,\s*56rem\)/i);
    expect(css).toMatch(/\.owner-card--detail\s*\{[\s\S]*overflow:\s*hidden/i);
    expect(css).toMatch(/\.owner-recording-credit-row\s*\{[\s\S]*grid-template-columns:\s*minmax\(5\.25rem,\s*0\.72fr\)\s+minmax\(0,\s*1\.1fr\)\s+minmax\(0,\s*1fr\)\s+minmax\(5\.25rem,\s*5\.25rem\)/i);
    expect(css).toMatch(/\.owner-recording-credit-row\s*>\s*\*\s*\{[\s\S]*min-width:\s*0/i);
    expect(css).toMatch(/\.owner-recording-credit-row__remove\s*\{[\s\S]*width:\s*5\.25rem/i);
    expect(css).toMatch(/\.owner-recording-credit-row__remove\s*\{[\s\S]*white-space:\s*nowrap/i);
    expect(css).toMatch(/\.owner-recording-credit-row__remove\s*\{[\s\S]*writing-mode:\s*horizontal-tb/i);
    expect(css).toMatch(/\.owner-recording-credit-row select,\s*\.owner-recording-credit-row input\s*\{[\s\S]*font-size:\s*0\.96rem/i);
  });

  it("adds consistent outer spacing between the main workspace, dashboard sections and the bottom result panel", async () => {
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(css).toMatch(/\.owner-shell\s*\{[\s\S]*width:\s*min\(1680px,\s*calc\(100vw\s*-\s*2\.8rem\)\)/i);
    expect(css).toMatch(/\.owner-grid\s*\+\s*\.owner-panel\s*\{[\s\S]*margin-top:\s*0\.95rem/i);
    expect(css).toMatch(/\.owner-job-board\s*\{[\s\S]*margin-top:\s*1\.1rem/i);
    expect(css).toMatch(/\.owner-job-board\s*\{[\s\S]*padding-top:\s*1\.1rem/i);
    expect(css).toMatch(/\.owner-job-board\s*\{[\s\S]*border-top:\s*1px\s+solid/i);
    expect(css).toMatch(/\.owner-panel--sidebar\s*\{[\s\S]*padding-right:\s*0\.9rem/i);
    expect(css).toMatch(/\.owner-panel-main-scroll\s*\{[\s\S]*padding-right:\s*1rem/i);
    expect(css).toMatch(/\.owner-panel-main-scroll\s*\{[\s\S]*overflow-y:\s*auto/i);
    expect(css).toMatch(/\.owner-panel--sidebar::-webkit-scrollbar,\s*\.owner-panel-main-scroll::-webkit-scrollbar,\s*\.owner-card--detail\s+\.owner-tab-panels::-webkit-scrollbar[\s\S]*width:\s*0\.48rem/i);
    expect(css).toMatch(/\.owner-panel--main\s*>\s*\.owner-tabs::after\s*\{[\s\S]*right:\s*-1rem/i);
    expect(css).toMatch(/textarea::-webkit-scrollbar[\s\S]*width:\s*0\.48rem/i);
    expect(css).toMatch(/\.owner-panel-main-scroll::-webkit-scrollbar-track,\s*[\s\S]*textarea::-webkit-scrollbar-track\s*\{[\s\S]*margin-block:\s*0\.75rem/i);
    expect(css).toMatch(/scrollbar-color:\s*rgba\(113,\s*86,\s*54,\s*0\.72\)\s+transparent/i);
  });

  it("surfaces external recording retrieval status and routes candidate links into the editable dialog flow", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");
    const html = await fs.readFile(path.resolve("apps/owner/web/index.html"), "utf8");
    const css = await fs.readFile(path.resolve("apps/owner/web/styles.css"), "utf8");

    expect(script).toContain("已调用外部版本检索工具");
    expect(script).toContain("未检测到外部版本检索工具状态");
    expect(script).toContain('data-proposal-link-candidate-edit');
    expect(script).toContain('data-proposal-link-candidate-confidence');
    expect(script).toContain('data-proposal-link-candidate-source-label');
    expect(script).not.toContain('data-proposal-link-candidate-open');
    expect(script).toContain("const formatProposalFieldOriginalValue = (field) => {");
    expect(script).toContain('formatProposalFieldInputValue({ ...field, after: undefined })');
    expect(script).toContain("linkDialogMeta.textContent = details.join");
    expect(html).toContain('id="owner-link-dialog-meta"');
    expect(css).toMatch(/\.owner-proposal__candidate-link-list\s*\{[\s\S]*display:\s*flex/i);
    expect(css).toMatch(/\.owner-proposal__candidate-link-list\s*\{[\s\S]*flex-wrap:\s*wrap/i);
    expect(script).toContain("放弃候选链接");
  });
  it("routes remote recording proposal images through the owner proxy preview endpoint", async () => {
    const script = await fs.readFile(path.resolve("apps/owner/web/app.js"), "utf8");

    expect(script).toContain("const buildOwnerRemoteImagePreviewUrl = (value) => {");
    expect(script).toContain('return `/api/remote-image?url=${encodeURIComponent(normalized)}`;');
    expect(script).toContain('<img src="${escapeHtml(buildOwnerRemoteImagePreviewUrl(candidate.src))}"');
  });
});
