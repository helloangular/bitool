const { test, expect } = require("/Users/aaryakulkarni/playwright/node_modules/@playwright/test");
const { execFileSync } = require("child_process");

const BASE_URL = process.env.BITOOL_URL || "http://127.0.0.1:8080";
const PG_URL = process.env.BITOOL_PG_URL || "postgresql://postgres:postgres@localhost/bitool";
const GRAPH_ID = 2247;
const SOURCE_NODE_ID = "2";
const ENDPOINT_NAME = "fleet/vehicles";

function psqlValue(sql) {
  return execFileSync("psql", [PG_URL, "-Atc", sql], { encoding: "utf8" }).trim();
}

function psqlInt(sql) {
  return Number.parseInt(psqlValue(sql), 10);
}

function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function resolveModelRunId(executeBody) {
  if (executeBody?.model_run_id) return Number(executeBody.model_run_id);
  if (!executeBody?.request_id) return Number(executeBody?.run_id || 0);
  return Number(psqlValue(`select model_run_id from compiled_model_run where execution_request_id = ${sqlString(executeBody.request_id)} order by created_at_utc desc limit 1`));
}

async function responseJsonOrThrow(response, label) {
  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  expect(response.ok(), `${label} failed with HTTP ${response.status()}: ${text}`).toBeTruthy();
  return body;
}

async function responseJson(response) {
  const text = await response.text();
  try {
    return text ? JSON.parse(text) : null;
  } catch {
    return text;
  }
}

async function expectDialogMessage(page, trigger, expectedMessage) {
  const dialogPromise = page.waitForEvent("dialog");
  await trigger();
  const dialog = await dialogPromise;
  expect(dialog.message()).toContain(expectedMessage);
  await dialog.accept();
}

async function expectDialogPattern(page, trigger, pattern) {
  const dialogPromise = page.waitForEvent("dialog");
  await trigger();
  const dialog = await dialogPromise;
  expect(dialog.message()).toMatch(pattern);
  await dialog.accept();
}

async function expectAlertMessage(page, trigger, pattern) {
  await page.evaluate(() => {
    window.__lastAlertMessage = null;
    window.alert = (msg) => {
      window.__lastAlertMessage = String(msg);
    };
  });
  await trigger();
  await expect.poll(async () => page.evaluate(() => window.__lastAlertMessage)).toMatch(pattern);
}

async function openExistingGraph(page, gid) {
  await page.goto(BASE_URL);
  await page.waitForSelector("tree-component", { timeout: 10000 });
  await page.evaluate(async (graphId) => {
    const res = await fetch("/graph", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ gid: graphId }),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`/graph failed: ${res.status} ${text}`);
    const data = text ? JSON.parse(text) : [];
    window.data.panelItems = Array.isArray(data) ? data : [data];
    window.data.gid = graphId;
    window.toggleModeling();
  }, gid);
  await page.waitForSelector("modeling-console", { state: "visible", timeout: 10000 });
}

async function createFreshGraphAndOpenModeling(page) {
  const graphName = `pw-empty-${Date.now()}`;
  await page.goto(BASE_URL);
  const shell = await page.evaluate(async (nextName) => {
    const res = await fetch("/newgraph", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ graphname: nextName }),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`/newgraph failed: ${res.status} ${text}`);
    return text ? JSON.parse(text) : null;
  }, graphName);
  const gid = Number(psqlValue(`select id from graph where name = '${graphName}' order by version desc limit 1`));
  expect(gid).toBeGreaterThan(0);
  await page.evaluate(({ graphId, graphShell }) => {
    window.data.panelItems = Array.isArray(graphShell) ? graphShell : [graphShell];
    window.data.gid = graphId;
    window.toggleModeling();
  }, { graphId: gid, graphShell: shell });
  await page.waitForSelector("modeling-console", { state: "visible", timeout: 10000 });
  return gid;
}

async function reloadAndOpenProposal(page, layerName, proposalId) {
  await openExistingGraph(page, GRAPH_ID);
  const host = page.locator("modeling-console");
  await host.locator("#layerSelect").selectOption(layerName);
  await openProposalFromList(host, proposalId);
  return host;
}

async function clickConfirm(page, messagePart) {
  const overlay = page.locator("body > div").filter({ hasText: messagePart }).last();
  await overlay.waitFor({ state: "visible", timeout: 10000 });
  await overlay.locator("button").filter({ hasText: "Yes" }).click();
}

async function selectValues(host, selector) {
  return host.locator(selector).evaluate((el) => Array.from(el.options).map((option) => option.value));
}

async function selectTexts(host, selector) {
  return host.locator(selector).evaluate((el) => Array.from(el.options).map((option) => option.textContent.trim()));
}

function detailAction(host, label) {
  return host.locator("#detailActions button").filter({ hasText: new RegExp(`^${label}$`) });
}

function proposalRow(host, proposalId) {
  return host.locator("#proposalsBody tr").filter({ hasText: new RegExp(`^\\s*${proposalId}\\b`) }).first();
}

function releaseRow(host, releaseId) {
  return host.locator("#releasesBody tr").filter({ hasText: new RegExp(`^\\s*${releaseId}\\b`) }).first();
}

function releaseRowForProposal(host, proposalId) {
  return host.locator("#releasesBody tr").filter({ hasText: new RegExp(`#${proposalId}\\b`) }).first();
}

function reviewPipelineStep(host, stepName) {
  const order = ["propose", "compile", "validate", "review", "publish"];
  const idx = order.indexOf(stepName);
  if (idx < 0) throw new Error(`Unknown review pipeline step: ${stepName}`);
  return host.locator("#reviewPipeline .pipeline-step").nth(idx);
}

async function expectStepClass(locator, expected) {
  const cls = await locator.getAttribute("class");
  const safe = String(cls || "");
  expect(safe.includes("done")).toBe(expected === "done");
  expect(safe.includes("current")).toBe(expected === "current");
  expect(safe.includes("error")).toBe(expected === "error");
}

async function expectDetailPipelineState(host, expectations) {
  const allSteps = ["propose", "edit", "compile", "validate", "review", "publish", "execute"];
  for (const step of allSteps) {
    const locator = host.locator(`#pipelineSteps .pipeline-step[data-step="${step}"]`);
    const expected = expectations?.[step] || "idle";
    await expectStepClass(locator, expected);
  }
}

async function expectReviewPipelineState(host, expectations) {
  const allSteps = ["propose", "compile", "validate", "review", "publish"];
  for (const step of allSteps) {
    const locator = reviewPipelineStep(host, step);
    const expected = expectations?.[step] || "idle";
    await expectStepClass(locator, expected);
  }
}

async function expectVisibleActions(host, labels) {
  for (const label of labels) {
    await expect(detailAction(host, label)).toBeVisible({ timeout: 15000 });
  }
}

async function expectHiddenActions(host, labels) {
  for (const label of labels) {
    await expect(detailAction(host, label)).toHaveCount(0);
  }
}

async function openProposalFromList(host, proposalId) {
  await host.locator('.tab[data-tab="proposals"]').click();
  const row = proposalRow(host, proposalId);
  await expect(row).toBeVisible({ timeout: 15000 });
  await row.locator("button").filter({ hasText: "Open" }).click();
  await expect(host.locator("#detailTitle")).toContainText(`Proposal #${proposalId}`, { timeout: 15000 });
}

async function openProposalFromRowClick(host, proposalId) {
  await host.locator('.tab[data-tab="proposals"]').click();
  const row = proposalRow(host, proposalId);
  await expect(row).toBeVisible({ timeout: 15000 });
  await row.click();
  await expect(host.locator("#detailTitle")).toContainText(`Proposal #${proposalId}`, { timeout: 15000 });
}

async function waitForProposalListReload(page, layerName = "silver") {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  return page.waitForResponse((r) => r.url().includes(`/${layerName}Proposals`) && r.request().method() === "GET");
}

async function waitForReleaseListReload(page, layerName = "silver") {
  return waitForProposalListReload(page, layerName);
}

async function resetProposalListFilters(page, host, layerName = "silver") {
  await host.locator('.tab[data-tab="proposals"]').click();
  if ((await host.locator("#filterStatus").inputValue()) !== "") {
    const reloadPromise = waitForProposalListReload(page, layerName);
    await host.locator("#filterStatus").selectOption("");
    await reloadPromise;
  }
}

async function expectProposalListStatus(host, proposalId, status) {
  await host.locator('.tab[data-tab="proposals"]').click();
  const row = proposalRow(host, proposalId);
  await expect(row).toBeVisible({ timeout: 15000 });
  await expect(row.locator("td").nth(4)).toContainText(status, { timeout: 15000 });
}

async function refreshProposalList(page, host, layerName = "silver") {
  await host.locator('.tab[data-tab="proposals"]').click();
  const reloadPromise = waitForProposalListReload(page, layerName);
  await host.locator("#refreshProposalsBtn").click();
  await reloadPromise;
}

async function expectExecutionSucceeded(host, runRef) {
  await expect(host.locator('.tab[data-tab="releases"].active')).toBeVisible({ timeout: 15000 });
  await expect(host.locator("#executionCard")).toBeVisible({ timeout: 15000 });
  if (runRef != null) {
    await expect
      .poll(async () => String((await host.locator("#execRunId").textContent()) || "").trim())
      .not.toBe("");
  }
  await expect(host.locator("#execStatus")).toContainText("SUCCEEDED", { timeout: 30000 });
}

async function expectExecutionMonitorDetails(host) {
  await expect(host.locator("#execStarted")).not.toHaveText("--");
  await expect(host.locator("#execDuration")).not.toHaveText("--");
  await expect(host.locator("#execRowCount")).not.toHaveText("--");
  await expect(host.locator("#execBackend")).not.toHaveText("--");
}

async function currentDetailStatus(host) {
  const text = await host.locator("#detailMeta").textContent();
  const match = String(text || "").match(/Status:\s+([a-z_]+)/i);
  return match ? match[1].toLowerCase() : "";
}

async function currentProposalId(host) {
  return Number((await host.locator("#detailTitle").textContent()).match(/Proposal #(\d+)/)?.[1] || 0);
}

async function openSqlPreviewAndCheck(page, host, expectedRegex = /(SELECT|MERGE|INSERT|CREATE|UPPER)/i) {
  await host.locator('.tab[data-tab="sql"]').click();
  await expect(host.locator("#copySqlBtn")).toBeVisible({ timeout: 15000 });
  for (const subtab of ["ddl", "dml", "full"]) {
    await host.locator(`#sqlTabs .tab[data-sqltab="${subtab}"]`).click();
    await expect(host.locator(`#sqlTabs .tab[data-sqltab="${subtab}"]`)).toHaveClass(/active/);
  }
  await expect(host.locator("#sqlContent")).toContainText(expectedRegex, { timeout: 15000 });
  await expect(host.locator("#sqlContent")).toBeVisible();
}

async function expectCompileCardRendered(host) {
  await expect(host.locator("#compileCard")).toBeVisible({ timeout: 15000 });
  await expect(host.locator("#compileInfo")).toContainText(/Target table:/i);
  await expect(host.locator("#compileInfo")).toContainText(/SQL length:/i);
}

async function expectValidationCardRendered(host, expectedStatus = "VALID") {
  await expect(host.locator("#validationCard")).toBeVisible({ timeout: 15000 });
  await expect(host.locator("#validationResults")).toContainText(new RegExp(`Validation status:\\s+${expectedStatus}`, "i"));
}

async function manualPollUntilStatus(page, host, layerName = "silver", terminalStatus = "succeeded", maxAttempts = 12) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const pollPromise = page.waitForResponse((r) => r.url().endsWith(`/poll${suffix}ModelRun`) && r.request().method() === "POST");
    await host.locator("#pollRunBtn").click();
    const pollBody = await responseJsonOrThrow(await pollPromise, `poll${suffix}ModelRun`);
    const status = String(pollBody.status || pollBody.run_status || "").toLowerCase();
    if (status === terminalStatus) {
      return pollBody;
    }
    await page.waitForTimeout(500);
  }
  throw new Error(`Run did not reach ${terminalStatus} within ${maxAttempts} manual poll attempts`);
}

async function previewExecutionData(page, host) {
  const previewPromise = page.waitForResponse((r) => r.url().includes("/previewTargetData") && r.request().method() === "GET");
  await host.locator("#previewDataBtn").click();
  await responseJsonOrThrow(await previewPromise, "previewTargetData");
  await expect(host.locator("#execPreview")).toBeVisible({ timeout: 15000 });
  await expect(host.locator("#execPreview table")).toBeVisible({ timeout: 15000 });
}

async function saveProcessingPolicy({ page, host, layerName = "silver", businessKeys, orderingStrategy, eventTimeColumn, sequenceColumn = "", lateDataMode = "", tooLateBehavior = "", lateToleranceValue = "", lateToleranceUnit = "", reprocessValue = "", reprocessUnit = "" }) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  if (businessKeys != null) {
    await host.locator("#policyBusinessKeys").fill(businessKeys);
  }
  if (orderingStrategy != null) {
    await host.locator("#policyOrderingStrategy").selectOption(orderingStrategy);
  }
  if (eventTimeColumn != null) {
    await host.locator("#policyEventTime").selectOption(eventTimeColumn);
  }
  if (sequenceColumn != null) {
    await host.locator("#policySequence").selectOption(sequenceColumn);
  }
  if (lateDataMode != null) {
    await host.locator("#policyLateMode").selectOption(lateDataMode);
  }
  if (tooLateBehavior != null) {
    await host.locator("#policyTooLate").selectOption(tooLateBehavior);
  }
  if (lateToleranceValue !== null) {
    await host.locator("#policyLateToleranceValue").fill(String(lateToleranceValue || ""));
  }
  if (lateToleranceUnit != null) {
    await host.locator("#policyLateToleranceUnit").selectOption(lateToleranceUnit);
  }
  if (reprocessValue !== null) {
    await host.locator("#policyReprocessValue").fill(String(reprocessValue || ""));
  }
  if (reprocessUnit != null) {
    await host.locator("#policyReprocessUnit").selectOption(reprocessUnit);
  }

  const updatePromise = page.waitForResponse((r) => r.url().endsWith(`/update${suffix}Proposal`) && r.request().method() === "POST");
  await host.locator("#saveSchemaBtn").click();
  return responseJsonOrThrow(await updatePromise, `update${suffix}Proposal`);
}

async function compileCurrentProposal(page, host, layerName = "silver") {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const compilePromise = page.waitForResponse((r) => r.url().endsWith(`/compile${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Compile").click();
  return responseJsonOrThrow(await compilePromise, `compile${suffix}Proposal`);
}

async function validateCurrentProposal(page, host, layerName = "silver") {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const validatePromise = page.waitForResponse((r) => r.url().endsWith(`/validate${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Validate").click();
  return responseJsonOrThrow(await validatePromise, `validate${suffix}Proposal`);
}

async function warehouseValidateCurrentProposal(page, host, layerName = "silver") {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const validatePromise = page.waitForResponse((r) => r.url().endsWith(`/validate${suffix}ProposalWarehouse`) && r.request().method() === "POST");
  await detailAction(host, "Warehouse Validate").click();
  const response = await validatePromise;
  return {
    statusCode: response.status(),
    body: await responseJson(response),
  };
}

async function applyMappingTransform({ page, host, targetColumn, transformType, params = "" }) {
  await host.locator(`[data-edit-transform="${targetColumn}"]`).click();
  const editor = page.locator("transform-editor");
  await expect(editor).toBeVisible({ timeout: 10000 });
  await editor.locator("select.transformation-type").selectOption(transformType);
  if (params) {
    await editor.locator("input.parameters").fill(params);
  }
  await editor.locator("button.btn-add").click();
  await expect(editor.locator(".transform-list")).toContainText(transformType);
  await editor.locator("#saveButton").click();
}

async function applyMappingExpression({ page, host, targetColumn, expression }) {
  await host.locator(`[data-edit-expression="${targetColumn}"]`).click();
  await page.waitForFunction(() => {
    const root = document.querySelector("modeling-console")?.shadowRoot;
    return root?.querySelector("#expressionModal.open") && root?.querySelector("#expressionMount expression-component");
  });
  await page.evaluate(({ nextExpression }) => {
    const root = document.querySelector("modeling-console").shadowRoot;
    const editor = root.querySelector("#expressionMount expression-component");
    editor.expressionArea.setTextContent(nextExpression);
  }, { nextExpression: expression });
  await page.locator("modeling-console").evaluate((hostEl) => {
    hostEl.shadowRoot.querySelector("#expressionSaveBtn").click();
  });
}

async function submitReviewDecision({ page, host, layerName = "silver", decision, notes = "" }) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  await host.locator('.tab[data-tab="review"]').click();
  await host.locator("#reviewDecision").selectOption(decision);
  if (notes !== null) {
    await host.locator("#reviewNotes").fill(notes);
  }
  const reviewPromise = page.waitForResponse((r) => r.url().endsWith(`/review${suffix}Proposal`) && r.request().method() === "POST");
  await host.locator("#submitReviewBtn").click();
  return responseJsonOrThrow(await reviewPromise, `review${suffix}Proposal`);
}

async function publishCurrentProposal({ page, host, layerName = "silver" }) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const publishPromise = page.waitForResponse((r) => r.url().endsWith(`/publish${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Publish Release").click();
  await clickConfirm(page, "Publish this proposal as a release?");
  return responseJsonOrThrow(await publishPromise, `publish${suffix}Proposal`);
}

async function executeCurrentProposal({ page, host, layerName = "silver" }) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const executePromise = page.waitForResponse((r) => r.url().endsWith(`/execute${suffix}Release`) && r.request().method() === "POST");
  await detailAction(host, "Execute Release").click();
  await clickConfirm(page, "Execute release #");
  return responseJsonOrThrow(await executePromise, `execute${suffix}Release`);
}

async function createSilverProposal({ page, host }) {
  await host.locator("#layerSelect").selectOption("silver");
  await host.locator('.tab[data-tab="proposals"]').click();
  await host.locator("#newProposalBtn").click();
  await host.locator("#propNodeId").selectOption(SOURCE_NODE_ID);
  await expect(host.locator("#propEndpoint")).toBeEnabled();
  await host.locator("#propEndpoint").selectOption(ENDPOINT_NAME);

  const proposePromise = page.waitForResponse((r) => r.url().endsWith("/proposeSilverSchema") && r.request().method() === "POST");
  await host.locator("#submitProposalBtn").click();
  const proposeBody = await responseJsonOrThrow(await proposePromise, "proposeSilverSchema");
  const proposalId = proposeBody.proposal_id;
  expect(proposalId).toBeTruthy();

  await expect(host.locator("#detailMeta")).toContainText(/Status: (draft|proposed)/, { timeout: 15000 });
  const initialStatus = await currentDetailStatus(host);
  if (initialStatus === "proposed") {
    await expectDetailPipelineState(host, {
      propose: "done",
      edit: "done",
      compile: "current",
    });
  } else {
    await expectDetailPipelineState(host, {
      propose: "current",
    });
  }
  await expectVisibleActions(host, ["Compile", "Synthesize Graph"]);
  await expectHiddenActions(host, ["Validate", "Publish Release", "Execute Release"]);
  await resetProposalListFilters(page, host, "silver");
  await refreshProposalList(page, host, "silver");
  await expectProposalListStatus(host, proposalId, initialStatus || "draft");
  await openProposalFromList(host, proposalId);

  await page.waitForFunction(() => {
    const select = document.querySelector("modeling-console")?.shadowRoot?.querySelector("#policyEventTime");
    return Boolean(select && select.options && select.options.length > 1 && select.value);
  });
  const eventTimeOption = await page.evaluate(() =>
    document.querySelector("modeling-console")?.shadowRoot?.querySelector("#policyEventTime")?.value || ""
  );
  expect(eventTimeOption).toBeTruthy();

  const updateBody = await saveProcessingPolicy({
    page,
    host,
    businessKeys: "data_items_id",
    orderingStrategy: "latest_event_time_wins",
    eventTimeColumn: eventTimeOption,
    lateDataMode: "merge",
    tooLateBehavior: "quarantine",
    lateToleranceValue: "10",
    lateToleranceUnit: "minutes",
    reprocessValue: "24",
    reprocessUnit: "hours",
  });

  expect(updateBody.proposal.processing_policy.business_keys).toEqual(["data_items_id"]);
  expect(updateBody.proposal.processing_policy.ordering_strategy).toBe("latest_event_time_wins");
  expect(updateBody.proposal.processing_policy.event_time_column).toBe(eventTimeOption);
  expect(updateBody.proposal.processing_policy.too_late_behavior).toBe("quarantine");
  expect(updateBody.proposal.processing_policy.reprocess_window).toEqual({ value: 24, unit: "hours" });

  await openProposalFromList(host, proposalId);
  await expect(host.locator("#policyBusinessKeys")).toHaveValue("data_items_id");
  await expect(host.locator("#policyOrderingStrategy")).toHaveValue("latest_event_time_wins");
  await expect(host.locator("#policyEventTime")).toHaveValue(eventTimeOption);
  await expect(host.locator("#policyTooLate")).toHaveValue("quarantine");
  await expect(host.locator("#policyLateToleranceValue")).toHaveValue("10");
  await expect(host.locator("#policyReprocessValue")).toHaveValue("24");

  return { proposalId, eventTimeOption, initialStatus };
}

async function setFieldValue(locator, value, { allowSyntheticSelectOption = false } = {}) {
  const tagName = await locator.evaluate((el) => el.tagName.toLowerCase());
  if (tagName === "select") {
    const stringValue = String(value);
    if (allowSyntheticSelectOption) {
      await locator.evaluate((el, nextValue) => {
        if (!Array.from(el.options).some((opt) => opt.value === nextValue)) {
          const opt = document.createElement("option");
          opt.value = nextValue;
          opt.textContent = nextValue;
          el.appendChild(opt);
        }
        el.value = nextValue;
        el.dispatchEvent(new Event("change", { bubbles: true }));
      }, stringValue);
    } else {
      await locator.selectOption(stringValue);
    }
    return;
  }
  await locator.fill(String(value));
}

async function createGoldProposal({ page, host, silverProposalId }) {
  await host.locator("#layerSelect").selectOption("gold");
  await host.locator('.tab[data-tab="proposals"]').click();
  await host.locator("#newProposalBtn").click();
  await setFieldValue(host.locator("#propSilverProposalId"), silverProposalId);

  const goldProposePromise = page.waitForResponse((r) => r.url().endsWith("/proposeGoldSchema") && r.request().method() === "POST");
  await host.locator("#submitProposalBtn").click();
  const goldProposeBody = await responseJsonOrThrow(await goldProposePromise, "proposeGoldSchema");
  const goldProposalId = goldProposeBody.proposal_id;
  expect(goldProposalId).toBeTruthy();

  await expect(host.locator("#detailMeta")).toContainText(/Status: (draft|proposed)/, { timeout: 15000 });
  const initialStatus = await currentDetailStatus(host);
  if (initialStatus === "proposed") {
    await expectDetailPipelineState(host, {
      propose: "done",
      edit: "done",
      compile: "current",
    });
  } else {
    await expectDetailPipelineState(host, {
      propose: "current",
    });
  }
  await expectVisibleActions(host, ["Compile", "Synthesize Graph"]);
  await expectHiddenActions(host, ["Validate", "Publish Release", "Execute Release"]);
  await resetProposalListFilters(page, host, "gold");
  await refreshProposalList(page, host, "gold");
  await expectProposalListStatus(host, goldProposalId, initialStatus || "draft");
  await openProposalFromList(host, goldProposalId);

  return { goldProposalId, initialStatus };
}

async function runCurrentProposalLifecycle({ page, host, layerName, checkWarehouseValidate = false, checkSqlPreview = false }) {
  const suffix = layerName.charAt(0).toUpperCase() + layerName.slice(1);
  const proposalId = await currentProposalId(host);
  expect(proposalId).toBeGreaterThan(0);

  const compilePromise = page.waitForResponse((r) => r.url().endsWith(`/compile${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Compile").click();
  const compileBody = await responseJsonOrThrow(await compilePromise, `compile${suffix}Proposal`);
  expect(compileBody.compiled_sql || compileBody.select_sql || compileBody.sql_ir).toBeTruthy();
  await expect(host.locator("#detailMeta")).toContainText("Status: compiled", { timeout: 15000 });
  await expectDetailPipelineState(host, {
    propose: "done",
    edit: "done",
    compile: "done",
    validate: "current",
  });
  await expectVisibleActions(host, ["Validate", "Warehouse Validate", "Go to Review"]);
  await expectHiddenActions(host, ["Publish Release", "Execute Release"]);
  await refreshProposalList(page, host, layerName);
  await expectProposalListStatus(host, proposalId, "compiled");
  await openProposalFromList(host, proposalId);

  let warehouseValidateBody = null;
  if (checkWarehouseValidate) {
    warehouseValidateBody = await warehouseValidateCurrentProposal(page, host, layerName);
    expect([200, 400].includes(warehouseValidateBody.statusCode)).toBeTruthy();
    await openProposalFromList(host, proposalId);
  }

  if (checkSqlPreview) {
    await openSqlPreviewAndCheck(page, host);
    await host.locator('.tab[data-tab="detail"]').click();
  }

  await expect(detailAction(host, "Validate")).toBeVisible({ timeout: 15000 });
  const validatePromise = page.waitForResponse((r) => r.url().endsWith(`/validate${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Validate").click();
  const validateBody = await responseJsonOrThrow(await validatePromise, `validate${suffix}Proposal`);
  expect(validateBody.status).toBe("valid");
  await expect(host.locator("#detailMeta")).toContainText("Status: validated", { timeout: 15000 });
  await expectDetailPipelineState(host, {
    propose: "done",
    edit: "done",
    compile: "done",
    validate: "done",
    review: "current",
  });
  await expectVisibleActions(host, ["Go to Review"]);
  await expectHiddenActions(host, ["Publish Release", "Execute Release"]);
  await refreshProposalList(page, host, layerName);
  await expectProposalListStatus(host, proposalId, "validated");
  await openProposalFromList(host, proposalId);

  await host.locator('.tab[data-tab="review"]').click();
  await expectReviewPipelineState(host, {
    propose: "done",
    compile: "done",
    validate: "done",
    review: "current",
  });
  await host.locator("#reviewDecision").selectOption("approved");
  page.once("dialog", (dialog) => dialog.accept());
  const reviewPromise = page.waitForResponse((r) => r.url().endsWith(`/review${suffix}Proposal`) && r.request().method() === "POST");
  await host.locator("#submitReviewBtn").click();
  const reviewBody = await responseJsonOrThrow(await reviewPromise, `review${suffix}Proposal`);
  expect(reviewBody.status).toBe("approved");
  await expect(host.locator('.tab[data-tab="detail"].active')).toBeVisible({ timeout: 15000 });
  await expect(host.locator("#detailMeta")).toContainText("Status: approved", { timeout: 15000 });
  await expect(host.locator("#detailStatus")).toContainText("Review submitted: approved. Next step: Publish Release.", { timeout: 15000 });
  await expectDetailPipelineState(host, {
    propose: "done",
    edit: "done",
    compile: "done",
    validate: "done",
    review: "done",
    publish: "current",
  });
  await expect(detailAction(host, "Publish Release")).toBeVisible({ timeout: 15000 });
  await host.locator('.tab[data-tab="review"]').click();
  await expectReviewPipelineState(host, {
    propose: "done",
    compile: "done",
    validate: "done",
    review: "done",
    publish: "current",
  });

  await refreshProposalList(page, host, layerName);
  await expectProposalListStatus(host, reviewBody.proposal_id, "approved");
  await openProposalFromList(host, reviewBody.proposal_id);
  await host.locator('.tab[data-tab="review"]').click();
  await expect(host.locator("#reviewDecision")).toHaveValue("approved");
  host = await reloadAndOpenProposal(page, layerName, reviewBody.proposal_id);
  await host.locator('.tab[data-tab="review"]').click();
  await expect(host.locator("#reviewDecision")).toHaveValue("approved");
  await expectReviewPipelineState(host, {
    propose: "done",
    compile: "done",
    validate: "done",
    review: "done",
    publish: "current",
  });

  await host.locator('.tab[data-tab="detail"]').click();
  await expect(detailAction(host, "Publish Release")).toBeVisible({ timeout: 15000 });
  const publishPromise = page.waitForResponse((r) => r.url().endsWith(`/publish${suffix}Proposal`) && r.request().method() === "POST");
  await detailAction(host, "Publish Release").click();
  await clickConfirm(page, "Publish this proposal as a release?");
  const publishBody = await responseJsonOrThrow(await publishPromise, `publish${suffix}Proposal`);
  expect(publishBody.release_id).toBeTruthy();
  await expect(host.locator("#detailMeta")).toContainText("Status: published", { timeout: 15000 });
  await expectDetailPipelineState(host, {
    propose: "done",
    edit: "done",
    compile: "done",
    validate: "done",
    review: "done",
    publish: "done",
    execute: "current",
  });
  await expectVisibleActions(host, ["Execute Release", "View Releases"]);
  await refreshProposalList(page, host, layerName);
  await expectProposalListStatus(host, proposalId, "published");
  host = await reloadAndOpenProposal(page, layerName, proposalId);
  await host.locator('.tab[data-tab="review"]').click();
  await expect(host.locator("#reviewDecision")).toHaveValue("approved");
  await expectReviewPipelineState(host, {
    propose: "done",
    compile: "done",
    validate: "done",
    review: "done",
    publish: "done",
  });
  await host.locator('.tab[data-tab="detail"]').click();

  await expect(detailAction(host, "Execute Release")).toBeVisible({ timeout: 15000 });
  const executePromise = page.waitForResponse((r) => r.url().endsWith(`/execute${suffix}Release`) && r.request().method() === "POST");
  await detailAction(host, "Execute Release").click();
  await clickConfirm(page, "Execute release #");
  const executeBody = await responseJsonOrThrow(await executePromise, `execute${suffix}Release`);
  expect(executeBody.request_id || executeBody.model_run_id || executeBody.run_id).toBeTruthy();
  await expectExecutionSucceeded(host, executeBody.request_id || executeBody.model_run_id || executeBody.run_id);
  const modelRunId = resolveModelRunId(executeBody);
  expect(modelRunId).toBeGreaterThan(0);

  return {
    proposalId,
    compileBody,
    warehouseValidateBody,
    validateBody,
    reviewBody,
    publishBody,
    executeBody,
    modelRunId,
  };
}

test.describe.serial("modeling console lifecycle", () => {
  test("empty states render on a fresh graph", async ({ page }) => {
    test.setTimeout(120000);
    await createFreshGraphAndOpenModeling(page);
    const host = page.locator("modeling-console");

    await host.locator('.tab[data-tab="proposals"]').click();
    await expect(host.locator("#proposalsEmpty")).toBeVisible();
    await expect(host.locator("#proposalsEmptyText")).toContainText(/Create a new proposal/i);

    await host.locator('.tab[data-tab="detail"]').click();
    await expect(host.locator("#detailEmpty")).toBeVisible();

    await host.locator('.tab[data-tab="review"]').click();
    await expect(host.locator("#reviewEmpty")).toBeVisible();

    await host.locator('.tab[data-tab="releases"]').click();
    await expect(host.locator("#releasesEmpty")).toBeVisible();
  });

  test("silver console controls and proposal form behave correctly", async ({ page }) => {
    test.setTimeout(120000);
    await openExistingGraph(page, GRAPH_ID);
    let host = page.locator("modeling-console");

    await expect(host).toBeVisible();
    await expect(host.locator('.tab[data-tab="proposals"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="detail"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="review"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="releases"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="sql"]')).toBeVisible();
    await expect(host.locator("#layerSelect")).toHaveValue("silver");

    await host.locator("#layerSelect").selectOption("gold");
    await expect(host.locator("#newProposalTitle")).toContainText("Create New Gold Proposal");
    await expect(host.locator("#newProposalHelp")).toContainText("existing Silver proposal");
    await host.locator("#newProposalBtn").click();
    await expect(host.locator("#goldProposalFields")).toBeVisible();
    await expect(host.locator("#silverProposalFields")).toBeHidden();
    await host.locator("#cancelProposalBtn").click();
    await host.locator("#layerSelect").selectOption("silver");
    await expect(host.locator("#newProposalTitle")).toContainText("Create New Silver Proposal");
    await expect(host.locator("#newProposalHelp")).toContainText("source node and endpoint");
    await host.locator("#newProposalBtn").click();
    await expect(host.locator("#silverProposalFields")).toBeVisible();
    await expect(host.locator("#goldProposalFields")).toBeHidden();

    await host.locator("#closeBtn").click();
    await expect(host).toBeHidden();
    await page.evaluate(() => window.toggleModeling());
    host = page.locator("modeling-console");
    await expect(host).toBeVisible();

    await host.locator("#newProposalBtn").click();
    await expect(host.locator("#newProposalCard")).toBeVisible();
    await expect(host.locator("#newProposalTitle")).toContainText("Create New Silver Proposal");
    await expect(host.locator("#propNodeId")).toBeVisible();
    await expect(host.locator("#propEndpoint")).toBeVisible();
    await expect(host.locator("#submitProposalBtn")).toBeVisible();
    await expect(host.locator("#propNodeId")).toBeEnabled();
    await expect(host.locator("#propEndpoint")).toBeEnabled();

    const sourceOptionTexts = await selectTexts(host, "#propNodeId");
    expect(sourceOptionTexts[0]).toContain("Select a Bronze source node");
    expect(sourceOptionTexts.some((text) => text.includes("#2"))).toBeTruthy();

    await host.locator("#cancelProposalBtn").click();
    await expect(host.locator("#newProposalCard")).toBeHidden();
    await host.locator("#newProposalBtn").click();
    await expect(host.locator("#propNodeId")).not.toHaveValue("");

    await host.locator("#propNodeId").selectOption(SOURCE_NODE_ID);
    await expect(host.locator("#propEndpoint")).toBeEnabled();

    const endpointOptionTexts = await selectTexts(host, "#propEndpoint");
    expect(endpointOptionTexts[0]).toContain("Select");
    expect(endpointOptionTexts.some((text) => text === ENDPOINT_NAME)).toBeTruthy();
    await host.locator("#propEndpoint").selectOption("");

    await expectDialogMessage(page, async () => {
      await host.evaluate((el) => {
        setTimeout(() => el.shadowRoot.querySelector("#submitProposalBtn").click(), 0);
      });
    }, "Endpoint name is required.");

    const filterStatusValues = await selectValues(host, "#filterStatus");
    expect(filterStatusValues).toEqual(["", "draft", "proposed", "compiled", "validated", "approved", "published", "rejected"]);
    for (const value of filterStatusValues) {
      const reloadPromise = waitForProposalListReload(page, "silver");
      await host.locator("#filterStatus").selectOption(value);
      await reloadPromise;
    }

    const filterLimitValues = await selectValues(host, "#filterLimit");
    expect(filterLimitValues).toEqual(["25", "50", "100"]);
    for (const value of filterLimitValues) {
      const reloadPromise = waitForProposalListReload(page, "silver");
      await host.locator("#filterLimit").selectOption(value);
      await reloadPromise;
    }

    const refreshPromise = waitForProposalListReload(page, "silver");
    await host.locator("#refreshProposalsBtn").click();
    await refreshPromise;
  });

  test("silver detail controls, processing policy controls, and negative review rules behave correctly", async ({ page }) => {
    test.setTimeout(240000);
    await openExistingGraph(page, GRAPH_ID);
    let host = page.locator("modeling-console");

    const { proposalId, eventTimeOption } = await createSilverProposal({ page, host });
    console.log("silver-detail: created proposal", proposalId);

    await host.locator('.tab[data-tab="proposals"]').click();
    await openProposalFromRowClick(host, proposalId);
    await host.locator("#backToList").click();
    await expect(host.locator('.tab[data-tab="proposals"].active')).toBeVisible({ timeout: 15000 });
    await openProposalFromList(host, proposalId);

    await expect(host.locator("#mappingCard")).toBeVisible();
    await expect(host.locator("#mappingCard thead")).toContainText("Source Field");
    await expect(host.locator("#mappingCard thead")).toContainText("Target Column");
    await expect(host.locator("#mappingCard thead")).toContainText("Transform");
    await expect(host.locator("#mappingCard thead")).toContainText("Cast");
    console.log("silver-detail: navigation and mapping checks passed");

    await expect(host.locator("#saveSchemaBtn")).toBeDisabled();
    const initialRowCount = await host.locator("#schemaBody tr").count();
    await host.locator("#addColumnBtn").click();
    await expect(host.locator("#schemaBody tr")).toHaveCount(initialRowCount + 1);
    await expect(host.locator("#saveSchemaBtn")).toBeEnabled();
    const addedRow = host.locator("#schemaBody tr").nth(initialRowCount);
    const addedRowTypeOptions = await addedRow.locator('[data-field="data_type"]').evaluate((el) =>
      Array.from(el.options).map((option) => option.value).filter(Boolean)
    );
    const selectedInlineType = addedRowTypeOptions.includes("STRING") ? "STRING" : addedRowTypeOptions[0];
    await addedRow.locator('[data-field="data_type"]').selectOption(selectedInlineType);
    await host.evaluate((el, rowIndex) => {
      const row = el.shadowRoot.querySelectorAll("#schemaBody tr")[rowIndex];
      if (!row) throw new Error("Inline schema row not found");
      const setValue = (selector, value) => {
        const input = row.querySelector(selector);
        input.value = value;
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      };
      const setChecked = (selector, checked) => {
        const input = row.querySelector(selector);
        input.checked = checked;
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      };
      setValue('[data-field="column_name"]', "qa_inline_col");
      setChecked('[data-field="nullable"]', false);
      setChecked('[data-field="primary_key"]', true);
      setValue('[data-field="description"]', "Playwright inline edit");
      setValue('[data-field="source_expression"]', "bronze.data_items_id");
    }, initialRowCount);
    const inlineSnapshot = await host.evaluate((el, rowIndex) => {
      const row = el.shadowRoot.querySelectorAll("#schemaBody tr")[rowIndex];
      return {
        dataType: row.querySelector('[data-field="data_type"]').value,
        nullable: row.querySelector('[data-field="nullable"]').checked,
        primaryKey: row.querySelector('[data-field="primary_key"]').checked,
        description: row.querySelector('[data-field="description"]').value,
        sourceExpression: row.querySelector('[data-field="source_expression"]').value,
      };
    }, initialRowCount);
    expect(inlineSnapshot).toEqual({
      dataType: selectedInlineType,
      nullable: false,
      primaryKey: true,
      description: "Playwright inline edit",
      sourceExpression: "bronze.data_items_id",
    });
    console.log("silver-detail: inline schema controls exercised");

    await host.locator('[data-edit-expression]').first().click();
    await expect(host.locator("#expressionModal")).toHaveClass(/open/);
    await expect(host.locator("#expressionCancelBtn")).toBeVisible();
    await expect(host.locator("#expressionSaveBtn")).toBeVisible();
    await host.locator("#expressionModal").click({ position: { x: 4, y: 4 } });
    await expect(host.locator("#expressionModal")).not.toHaveClass(/open/);
    console.log("silver-detail: expression modal backdrop close passed");

    const firstTransformButton = host.locator('[data-edit-transform]').first();
    if (await firstTransformButton.isEnabled()) {
      await firstTransformButton.click();
      const editor = page.locator("transform-editor");
      await expect(editor).toBeVisible({ timeout: 10000 });
      await page.locator("transform-editor").evaluate((el) => {
        const btn = el.shadowRoot.getElementById("closeButton");
        btn.click();
      });
      await expect(editor).toBeHidden({ timeout: 10000 });
    }

    expect(await selectValues(host, "#policyOrderingStrategy")).toEqual(["", "latest_event_time_wins", "latest_sequence_wins", "event_time_then_sequence", "append_only"]);
    expect(await selectValues(host, "#policyLateMode")).toEqual(["", "merge", "append"]);
    expect(await selectValues(host, "#policyTooLate")).toEqual(["", "accept", "quarantine", "drop"]);
    expect(await selectValues(host, "#policyLateToleranceUnit")).toEqual(["", "minutes", "hours", "days"]);
    expect(await selectValues(host, "#policyReprocessUnit")).toEqual(["", "minutes", "hours", "days"]);

    const policyEventOptions = await selectValues(host, "#policyEventTime");
    expect(policyEventOptions[0]).toBe("");
    expect(policyEventOptions.length).toBeGreaterThan(1);
    const policySequenceOptions = await selectValues(host, "#policySequence");
    expect(policySequenceOptions[0]).toBe("");

    const invalidSaveBody = await saveProcessingPolicy({
      page,
      host,
      businessKeys: "",
      orderingStrategy: "latest_event_time_wins",
      eventTimeColumn: "",
      sequenceColumn: "",
      lateDataMode: "merge",
      tooLateBehavior: "quarantine",
      lateToleranceValue: "10",
      lateToleranceUnit: "minutes",
      reprocessValue: "24",
      reprocessUnit: "hours",
    });
    expect(invalidSaveBody.proposal.processing_policy.ordering_strategy).toBe("latest_event_time_wins");
    expect(invalidSaveBody.proposal.processing_policy.business_keys || []).toEqual([]);
    expect(invalidSaveBody.proposal.processing_policy.event_time_column || "").toBe("");
    expect(invalidSaveBody.proposal.columns.find((col) => col.target_column === "qa_inline_col")).toMatchObject({
      target_column: "qa_inline_col",
      type: selectedInlineType,
      nullable: false,
      role: "business_key",
      description: "Playwright inline edit",
      expression: "bronze.data_items_id",
    });
    console.log("silver-detail: inline schema persistence passed");
    await host.locator("#schemaBody tr").nth(initialRowCount).locator('[data-remove]').click();

    console.log("silver-detail: starting invalid compile branch");
    await expect(detailAction(host, "Compile")).toBeVisible({ timeout: 15000 });
    const invalidCompilePromise = page.waitForResponse((r) => r.url().endsWith("/compileSilverProposal") && r.request().method() === "POST");
    await detailAction(host, "Compile").click();
    const invalidCompileResponse = await invalidCompilePromise;
    console.log("silver-detail: invalid compile response", invalidCompileResponse.status());
    expect(invalidCompileResponse.status()).toBe(400);
    const invalidCompileBody = await invalidCompileResponse.json();
    expect(String(invalidCompileBody.error || "")).toContain("failed static validation");
    expect(JSON.stringify(invalidCompileBody.data?.errors || [])).toMatch(/business_keys|event_time_column|Business Keys|event time/i);
    console.log("silver-detail: invalid compile branch passed");

    const successSilver = await createSilverProposal({ page, host });
    host = page.locator("modeling-console");
    console.log("silver-detail: fresh proposal for success path", successSilver.proposalId);
    console.log("silver-detail: starting success compile");
    const compileBody = await compileCurrentProposal(page, host, "silver");
    expect(compileBody.compiled_sql || compileBody.select_sql).toBeTruthy();
    console.log("silver-detail: success compile returned");
    await expectCompileCardRendered(host);
    console.log("silver-detail: compile card rendered");
    await host.locator("#viewSqlBtn").click();
    await expect(host.locator('.tab[data-tab="sql"].active')).toBeVisible({ timeout: 15000 });
    console.log("silver-detail: switched to sql tab");
    await openSqlPreviewAndCheck(page, host);
    console.log("silver-detail: sql preview checked");
    await page.evaluate(() => {
      Object.defineProperty(navigator, "clipboard", {
        configurable: true,
        value: {
          writeText: async () => undefined,
        },
      });
      window.__lastSqlCopyAlert = null;
      window.alert = (msg) => {
        window.__lastSqlCopyAlert = String(msg);
      };
    });
    await host.locator("#copySqlBtn").click();
    await expect.poll(async () => page.evaluate(() => window.__lastSqlCopyAlert)).toMatch(/SQL copied to clipboard\.|Failed to copy\./);
    console.log("silver-detail: copy sql clicked");
    await host.locator('.tab[data-tab="detail"]').click();
    console.log("silver-detail: compile/sql card checks passed");

    const warehouseValidateBody = await warehouseValidateCurrentProposal(page, host, "silver");
    expect([200, 400].includes(warehouseValidateBody.statusCode)).toBeTruthy();
    if (warehouseValidateBody.statusCode === 200) {
      expect(psqlValue(`select validation_kind from model_validation_result where proposal_id = ${successSilver.proposalId} order by validation_id desc limit 1`)).toBe("silver_warehouse_sql");
    } else {
      expect(JSON.stringify(warehouseValidateBody.body)).toMatch(/validation job|target|warehouse/i);
    }
    console.log("silver-detail: warehouse validate path passed");

    await host.locator('.tab[data-tab="review"]').click();
    await expect(host.locator("#reviewDecision")).toBeDisabled();
    await expect(host.locator("#submitReviewBtn")).toBeDisabled();
    await expect(host.locator("#reviewContent")).toContainText(/Review is not available yet/i);
    await host.locator('.tab[data-tab="detail"]').click();

    const validValidateBody = await validateCurrentProposal(page, host, "silver");
    expect(validValidateBody.status).toBe("valid");
    await expect(host.locator("#detailMeta")).toContainText("Status: validated", { timeout: 15000 });
    await expectValidationCardRendered(host, "VALID");
    await expect(host.locator("#validationResults")).toContainText(/PASS|WARN|FAIL/);
    console.log("silver-detail: validation card checks passed");

    await host.locator('.tab[data-tab="review"]').click();
    expect(await selectValues(host, "#reviewDecision")).toEqual(["", "approved", "changes_requested", "rejected"]);
    await expect(host.locator("#submitReviewBtn")).toBeDisabled();
    console.log("silver-detail: entered review tab after validation");

    await host.locator("#reviewDecision").selectOption("changes_requested");
    await expect(host.locator("#submitReviewBtn")).toBeEnabled();
    console.log("silver-detail: changes_requested option selected");
    await expectAlertMessage(page, async () => {
      await host.evaluate((el) => {
        setTimeout(() => el.shadowRoot.querySelector("#submitReviewBtn").click(), 0);
      });
    }, /Notes are required for rejection or change requests\./);
    console.log("silver-detail: missing-notes guard for changes_requested passed");

    await host.locator("#reviewDecision").selectOption("rejected");
    await expectAlertMessage(page, async () => {
      await host.evaluate((el) => {
        setTimeout(() => el.shadowRoot.querySelector("#submitReviewBtn").click(), 0);
      });
    }, /Notes are required for rejection or change requests\./);
    console.log("silver-detail: missing-notes guard for rejected passed");

    console.log("silver-detail: submitting changes_requested review");
    const changesRequestedBody = await submitReviewDecision({
      page,
      host,
      layerName: "silver",
      decision: "changes_requested",
      notes: "Needs another edit pass",
    });
    expect(changesRequestedBody.status).toBe("changes_requested");
    console.log("silver-detail: changes_requested review submitted");
    await host.locator('.tab[data-tab="detail"]').click();
    await expect(host.locator("#detailMeta")).toContainText("Status: changes_requested", { timeout: 15000 });
    await expectVisibleActions(host, ["Compile", "Synthesize Graph"]);
    await expectDetailPipelineState(host, {
      propose: "done",
      edit: "done",
      compile: "done",
      validate: "done",
      review: "error",
    });
    host = await reloadAndOpenProposal(page, "silver", successSilver.proposalId);
    await host.locator('.tab[data-tab="review"]').click();
    await expect(host.locator("#reviewDecision")).toHaveValue("changes_requested");
    console.log("silver-detail: changes_requested persistence passed");

    await host.locator('.tab[data-tab="detail"]').click();
    await host.locator("#policyLateToleranceValue").fill("11");
    const blockedSavePromise = page.waitForResponse((r) => r.url().endsWith("/updateSilverProposal") && r.request().method() === "POST");
    await host.locator("#saveSchemaBtn").click();
    const blockedSaveResponse = await blockedSavePromise;
    expect(blockedSaveResponse.status()).toBe(409);
    const blockedSaveBody = await blockedSaveResponse.json();
    expect(String(blockedSaveBody.error || "")).toContain("cannot be edited");
    console.log("silver-detail: blocked edit after changes_requested passed");

    console.log("silver-detail: creating rejected-path silver proposal");
    const { proposalId: rejectedProposalId } = await createSilverProposal({ page, host });
    console.log("silver-detail: rejected-path proposal created", rejectedProposalId);
    await compileCurrentProposal(page, host, "silver");
    console.log("silver-detail: rejected-path compile passed");
    const rejectedValidateBody = await validateCurrentProposal(page, host, "silver");
    expect(rejectedValidateBody.status).toBe("valid");
    console.log("silver-detail: rejected-path validate passed");
    const rejectedBody = await submitReviewDecision({
      page,
      host,
      layerName: "silver",
      decision: "rejected",
      notes: "Rejecting for lifecycle regression check",
    });
    expect(rejectedBody.status).toBe("rejected");
    console.log("silver-detail: rejected review submitted");
    await expect(host.locator("#detailMeta")).toContainText("Status: rejected", { timeout: 15000 });
    await expect(host.locator("#detailActions button")).toHaveCount(0);

    host = await reloadAndOpenProposal(page, "silver", rejectedProposalId);
    await host.locator('.tab[data-tab="review"]').click();
    await expect(host.locator("#reviewDecision")).toHaveValue("rejected");
    console.log("silver-detail: rejected persistence passed");
  });

  test("silver proposal completes full UI lifecycle and execution", async ({ page }) => {
    test.setTimeout(180000);
    const silverRowsBefore = psqlInt("select count(*) from public.silver_fleet_vehicles");

    await openExistingGraph(page, GRAPH_ID);
    let host = page.locator("modeling-console");

    const { proposalId } = await createSilverProposal({ page, host });
    const silverRun = await runCurrentProposalLifecycle({ page, host, layerName: "silver", checkWarehouseValidate: true, checkSqlPreview: true });
    console.log("silver-lifecycle: initial lifecycle run completed", silverRun.modelRunId);

    expect(silverRun.proposalId).toBe(proposalId);
    expect(silverRun.compileBody.sql_ir.processing_policy.business_keys).toEqual(["data_items_id"]);
    expect(silverRun.compileBody.sql_ir.processing_policy.ordering_strategy).toBe("latest_event_time_wins");
    expect(silverRun.compileBody.select_sql).toContain("ROW_NUMBER() OVER");
    expect(silverRun.compileBody.select_sql).toContain("INTERVAL '24 hours'");
    expect(silverRun.compileBody.select_sql).toContain("\"data_items_id\"");
    if (silverRun.warehouseValidateBody.statusCode === 200) {
      expect(psqlValue(`select validation_kind from model_validation_result where validation_id = ${silverRun.warehouseValidateBody.body.validation_id}`)).toBe("silver_warehouse_sql");
    } else {
      expect(JSON.stringify(silverRun.warehouseValidateBody.body)).toMatch(/validation job|target|warehouse/i);
    }
    expect(psqlValue(`select status from compiled_model_run where model_run_id = ${silverRun.modelRunId}`)).toBe("succeeded");
    expect(psqlValue(`select execution_backend from compiled_model_run where model_run_id = ${silverRun.modelRunId}`)).toBe("postgresql_sql");

    host = await reloadAndOpenProposal(page, "silver", proposalId);
    await detailAction(host, "View Releases").click();
    await expect(host.locator('.tab[data-tab="releases"].active')).toBeVisible({ timeout: 15000 });
    console.log("silver-lifecycle: on releases tab");
    const releaseReloadPromise = waitForReleaseListReload(page, "silver");
    await host.locator("#refreshReleasesBtn").click();
    await releaseReloadPromise;
    console.log("silver-lifecycle: releases refreshed");
    const silverReleaseRow = releaseRowForProposal(host, proposalId);
    await expect(silverReleaseRow).toBeVisible({ timeout: 15000 });
    console.log("silver-lifecycle: release row visible");
    await silverReleaseRow.locator("button").filter({ hasText: "View" }).click();
    await expect(host.locator('.tab[data-tab="detail"].active')).toBeVisible({ timeout: 15000 });
    await expect(host.locator("#detailTitle")).toContainText(`Proposal #${proposalId}`);
    console.log("silver-lifecycle: release row view works");
    await host.locator('.tab[data-tab="releases"]').click();
    console.log("silver-lifecycle: back on releases tab for execute");
    const executeReleaseRow = releaseRowForProposal(host, proposalId);
    await expect(executeReleaseRow).toBeVisible({ timeout: 15000 });
    let delayFirstRerunPoll = true;
    const silverPollRoute = /\/pollSilverModelRun$/;
    await page.route(silverPollRoute, async (route) => {
      if (delayFirstRerunPoll) {
        delayFirstRerunPoll = false;
        await new Promise((resolve) => setTimeout(resolve, 2500));
      }
      await route.continue();
    });
    const rerunExecutePromise = page.waitForResponse((r) => r.url().endsWith("/executeSilverRelease") && r.request().method() === "POST");
    console.log("silver-lifecycle: clicking release-row execute");
    await host.evaluate((el, pid) => {
      const rows = Array.from(el.shadowRoot.querySelectorAll("#releasesBody tr"));
      const row = rows.find((tr) => tr.textContent.includes(`#${pid}`));
      if (!row) throw new Error(`Release row not found for proposal ${pid}`);
      const btn = Array.from(row.querySelectorAll("button")).find((button) => button.textContent.trim() === "Execute");
      if (!btn) throw new Error(`Execute button not found for proposal ${pid}`);
      btn.click();
    }, proposalId);
    console.log("silver-lifecycle: confirming release execute");
    await clickConfirm(page, "Execute release #");
    console.log("silver-lifecycle: waiting for execute response");
    const rerunBody = await responseJsonOrThrow(await rerunExecutePromise, "executeSilverRelease");
    const rerunId = rerunBody.request_id || rerunBody.model_run_id || rerunBody.run_id;
    expect(rerunId).toBeTruthy();
    expect(rerunId).not.toBe(silverRun.executeBody.request_id || silverRun.executeBody.model_run_id);
    console.log("silver-lifecycle: rerun submitted", rerunId);
    const firstRerunPollResponse = page.waitForResponse((r) =>
      r.url().endsWith("/pollSilverModelRun") &&
      r.request().method() === "POST" &&
      (r.request().postData() || "").includes(String(rerunId))
    );
    await expect(host.locator("#stopPollBtn")).toBeVisible({ timeout: 15000 });
    await host.locator("#stopPollBtn").click();
    await expect(host.locator("#stopPollBtn")).toBeHidden({ timeout: 15000 });
    await firstRerunPollResponse;
    await page.unroute(silverPollRoute);
    console.log("silver-lifecycle: manual polling mode");
    const manualPollBody = await manualPollUntilStatus(page, host, "silver", "succeeded");
    expect(String(manualPollBody.status || manualPollBody.run_status || "").toLowerCase()).toBe("succeeded");
    console.log("silver-lifecycle: manual poll succeeded");
    await expectExecutionSucceeded(host, rerunId);
    await expectExecutionMonitorDetails(host);
    await expect(host.locator("#previewDataBtn")).toBeVisible({ timeout: 15000 });
    await previewExecutionData(page, host);
    console.log("silver-lifecycle: execution preview works");

    host = await reloadAndOpenProposal(page, "silver", proposalId);
    const silverCloneEventTime = await host.locator("#policyEventTime").inputValue();
    const clonedSilverBody = await saveProcessingPolicy({
      page,
      host,
      layerName: "silver",
      businessKeys: "data_items_id",
      orderingStrategy: "latest_event_time_wins",
      eventTimeColumn: silverCloneEventTime,
      sequenceColumn: "",
      lateDataMode: "merge",
      tooLateBehavior: "quarantine",
      lateToleranceValue: "12",
      lateToleranceUnit: "minutes",
      reprocessValue: "24",
      reprocessUnit: "hours",
    });
    const clonedSilverId = clonedSilverBody.proposal_id || clonedSilverBody.id;
    expect(clonedSilverId).toBeGreaterThan(0);
    expect(clonedSilverId).not.toBe(proposalId);
    expect(psqlValue(`select status from model_proposal where proposal_id = ${proposalId}`)).toBe("published");
    expect(psqlValue(`select status from model_proposal where proposal_id = ${clonedSilverId}`)).toBe("draft");
    console.log("silver-lifecycle: clone on edit works", clonedSilverId);

    const silverRowsAfter = psqlInt("select count(*) from public.silver_fleet_vehicles");
    expect(silverRowsAfter).toBeGreaterThan(0);
    expect(silverRowsAfter).toBeGreaterThanOrEqual(silverRowsBefore);
  });

  test("gold proposal completes full UI lifecycle and execution", async ({ page }) => {
    test.setTimeout(240000);
  const silverRowsBefore = psqlInt("select count(*) from public.silver_fleet_vehicles");
  const goldRowsBefore = psqlInt("select count(*) from public.gold_fleet_vehicles");

  await openExistingGraph(page, GRAPH_ID);
  let host = page.locator("modeling-console");

  const { proposalId } = await createSilverProposal({ page, host });
  const silverRun = await runCurrentProposalLifecycle({ page, host, layerName: "silver" });
  expect(silverRun.compileBody.sql_ir.processing_policy.business_keys).toEqual(["data_items_id"]);
  expect(silverRun.compileBody.sql_ir.processing_policy.ordering_strategy).toBe("latest_event_time_wins");
  expect(silverRun.compileBody.select_sql).toContain("ROW_NUMBER() OVER");
  expect(silverRun.compileBody.select_sql).toContain("INTERVAL '24 hours'");
  expect(silverRun.compileBody.select_sql).toContain("\"data_items_id\"");

  const { goldProposalId } = await createGoldProposal({ page, host, silverProposalId: proposalId });

  await applyMappingExpression({
    page,
    host,
    targetColumn: "data_items_id",
    expression: 'UPPER(silver."data_items_id")',
  });

  const goldRun = await runCurrentProposalLifecycle({ page, host, layerName: "gold", checkWarehouseValidate: true, checkSqlPreview: true });
  expect(goldRun.proposalId).toBe(goldProposalId);

  expect(psqlValue(`select status from compiled_model_run where model_run_id = ${silverRun.modelRunId}`)).toBe("succeeded");
  expect(psqlValue(`select execution_backend from compiled_model_run where model_run_id = ${silverRun.modelRunId}`)).toBe("postgresql_sql");
  if (goldRun.warehouseValidateBody.statusCode === 200) {
    expect(psqlValue(`select validation_kind from model_validation_result where validation_id = ${goldRun.warehouseValidateBody.body.validation_id}`)).toBe("gold_warehouse_sql");
  } else {
    expect(JSON.stringify(goldRun.warehouseValidateBody.body)).toMatch(/validation job|target|warehouse/i);
  }
  expect(psqlValue(`select status from compiled_model_run where model_run_id = ${goldRun.modelRunId}`)).toBe("succeeded");
  expect(psqlValue(`select execution_backend from compiled_model_run where model_run_id = ${goldRun.modelRunId}`)).toBe("postgresql_sql");

  host = await reloadAndOpenProposal(page, "gold", goldProposalId);
  const goldRerunBody = await executeCurrentProposal({ page, host, layerName: "gold" });
  const goldRerunId = goldRerunBody.request_id || goldRerunBody.model_run_id || goldRerunBody.run_id;
  expect(goldRerunId).toBeTruthy();
  expect(goldRerunId).not.toBe(goldRun.executeBody.request_id || goldRun.executeBody.model_run_id);
  await expectExecutionSucceeded(host, goldRerunId);

  host = await reloadAndOpenProposal(page, "gold", goldProposalId);
  const goldCloneEventTime = await host.locator("#policyEventTime").inputValue();
  const goldCloneOrdering = await host.locator("#policyOrderingStrategy").inputValue();
  const goldCloneBusinessKeys = await host.locator("#policyBusinessKeys").inputValue();
  const clonedGoldBody = await saveProcessingPolicy({
    page,
    host,
    layerName: "gold",
    businessKeys: goldCloneBusinessKeys || "data_items_id",
    orderingStrategy: goldCloneOrdering || "",
    eventTimeColumn: goldCloneEventTime,
    sequenceColumn: "",
    lateDataMode: "merge",
    tooLateBehavior: "quarantine",
    lateToleranceValue: "13",
    lateToleranceUnit: "minutes",
    reprocessValue: "24",
    reprocessUnit: "hours",
  });
  const clonedGoldId = clonedGoldBody.proposal_id || clonedGoldBody.id;
  expect(clonedGoldId).toBeGreaterThan(0);
  expect(clonedGoldId).not.toBe(goldProposalId);
  expect(psqlValue(`select status from model_proposal where proposal_id = ${goldProposalId}`)).toBe("published");
  expect(psqlValue(`select status from model_proposal where proposal_id = ${clonedGoldId}`)).toBe("draft");

  const silverRowsAfter = psqlInt("select count(*) from public.silver_fleet_vehicles");
  const goldRowsAfter = psqlInt("select count(*) from public.gold_fleet_vehicles");
  expect(silverRowsAfter).toBeGreaterThan(0);
  expect(goldRowsAfter).toBeGreaterThan(0);
  expect(silverRowsAfter).toBeGreaterThanOrEqual(silverRowsBefore);
  expect(goldRowsAfter).toBeGreaterThanOrEqual(goldRowsBefore);
  });

  test("gold controls, policy, mapping edits, and persistence cover the QA checklist", async ({ page }) => {
    test.setTimeout(240000);
    await openExistingGraph(page, GRAPH_ID);
    let host = page.locator("modeling-console");

    await host.locator("#layerSelect").selectOption("gold");
    await expect(host.locator("#newProposalTitle")).toContainText("Create New Gold Proposal");
    await expect(host.locator("#newProposalHelp")).toContainText("existing Silver proposal");
    await expect(host.locator('.tab[data-tab="proposals"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="detail"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="review"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="releases"]')).toBeVisible();
    await expect(host.locator('.tab[data-tab="sql"]')).toBeVisible();

    const goldFilterStatusValues = await selectValues(host, "#filterStatus");
    expect(goldFilterStatusValues).toEqual(["", "draft", "proposed", "compiled", "validated", "approved", "published", "rejected"]);
    for (const value of goldFilterStatusValues) {
      const reloadPromise = waitForProposalListReload(page, "gold");
      await host.locator("#filterStatus").selectOption(value);
      await reloadPromise;
    }

    const goldFilterLimitValues = await selectValues(host, "#filterLimit");
    expect(goldFilterLimitValues).toEqual(["25", "50", "100"]);
    for (const value of goldFilterLimitValues) {
      const reloadPromise = waitForProposalListReload(page, "gold");
      await host.locator("#filterLimit").selectOption(value);
      await reloadPromise;
    }

    const refreshPromise = waitForProposalListReload(page, "gold");
    await host.locator("#refreshProposalsBtn").click();
    await refreshPromise;
    await resetProposalListFilters(page, host, "gold");
    if ((await host.locator("#filterLimit").inputValue()) !== "25") {
      const limitResetPromise = waitForProposalListReload(page, "gold");
      await host.locator("#filterLimit").selectOption("25");
      await limitResetPromise;
    }
    console.log("gold-controls: filters reset");

    await host.locator("#newProposalBtn").click();
    await expect(host.locator("#goldProposalFields")).toBeVisible();
    await expect(host.locator("#silverProposalFields")).toBeHidden();
    await expect(host.locator("#propSilverProposalId")).toBeVisible();
    expect(await selectValues(host, "#propSilverProposalId")).toContain("");

    await expectAlertMessage(page, async () => {
      await host.locator("#submitProposalBtn").click();
    }, /Silver Proposal ID is required\./i);

    await host.evaluate((el, nextValue) => {
      const select = el.shadowRoot.querySelector("#propSilverProposalId");
      if (!select) throw new Error("Silver proposal select not found");
      if (!Array.from(select.options).some((opt) => opt.value === nextValue)) {
        const opt = document.createElement("option");
        opt.value = nextValue;
        opt.textContent = nextValue;
        select.appendChild(opt);
      }
      select.value = nextValue;
      select.dispatchEvent(new Event("input", { bubbles: true }));
      select.dispatchEvent(new Event("change", { bubbles: true }));
    }, "999999");
    await expect(host.locator("#propSilverProposalId")).toHaveValue("999999");
    const missingSilverPromise = page.waitForResponse((r) => r.url().endsWith("/proposeGoldSchema") && r.request().method() === "POST");
    await expectAlertMessage(page, async () => {
      await host.locator("#submitProposalBtn").click();
    }, /Silver proposal not found/i);
    expect([400, 404].includes((await missingSilverPromise).status())).toBeTruthy();
    await host.locator("#cancelProposalBtn").click();
    console.log("gold-controls: missing silver negative passed");

    await host.locator("#layerSelect").selectOption("silver");
    const { proposalId: silverProposalId } = await createSilverProposal({ page, host });
    console.log("gold-controls: silver source proposal created", silverProposalId);

    await host.locator("#layerSelect").selectOption("gold");
    const { goldProposalId, initialStatus } = await createGoldProposal({ page, host, silverProposalId });
    console.log("gold-controls: gold proposal created", goldProposalId, initialStatus);
    expect(psqlValue(`select status from model_proposal where proposal_id = ${silverProposalId}`)).toMatch(/draft|proposed/);
    expect(psqlValue(`select layer from model_proposal where proposal_id = ${goldProposalId}`)).toBe("gold");
    expect(psqlValue(`select source_graph_id from model_proposal where proposal_id = ${goldProposalId}`)).toBe(String(GRAPH_ID));
    expect(psqlValue(`select status from model_proposal where proposal_id = ${goldProposalId}`)).toBe(initialStatus || "draft");
    expect(psqlInt(`select count(*) from model_proposal where proposal_id = ${goldProposalId}`)).toBe(1);

    await expect(host.locator("#saveSchemaBtn")).toBeDisabled();
    const goldInitialRowCount = await host.locator("#schemaBody tr").count();
    await host.locator("#addColumnBtn").click();
    await expect(host.locator("#schemaBody tr")).toHaveCount(goldInitialRowCount + 1);
    await expect(host.locator("#saveSchemaBtn")).toBeEnabled();
    await host.locator("#schemaBody tr").nth(goldInitialRowCount).locator('[data-remove]').click();
    await expect(host.locator("#schemaBody tr")).toHaveCount(goldInitialRowCount);

    await host.locator('[data-edit-expression="data_items_id"]').click();
    await expect(host.locator("#expressionModal")).toHaveClass(/open/);
    await host.locator("#expressionCancelBtn").click();
    await expect(host.locator("#expressionModal")).not.toHaveClass(/open/);

    const goldTransformButton = host.locator('[data-edit-transform="data_items_id"]');
    if (await goldTransformButton.isEnabled()) {
      await goldTransformButton.click();
      const editor = page.locator("transform-editor");
      await expect(editor).toBeVisible({ timeout: 10000 });
      await page.locator("transform-editor").evaluate((el) => {
        el.shadowRoot.getElementById("closeButton").click();
      });
      await expect(editor).toBeHidden({ timeout: 10000 });
    }

    await applyMappingExpression({
      page,
      host,
      targetColumn: "data_items_id",
      expression: 'UPPER(silver."data_items_id")',
    });
    await expect(host.locator("#saveSchemaBtn")).toBeEnabled();
    const goldMappingRow = host.locator("#mappingBody tr").filter({ hasText: "data_items_id" }).first();
    await expect(goldMappingRow).toContainText('UPPER(silver."data_items_id")');

    if (await goldTransformButton.isEnabled()) {
      await applyMappingTransform({
        page,
        host,
        targetColumn: "data_items_id",
        transformType: "UPPERCASE",
      });
      await expect(goldMappingRow).toContainText("UPPERCASE");
    }

    expect(await selectValues(host, "#policyOrderingStrategy")).toEqual(["", "latest_event_time_wins", "latest_sequence_wins", "event_time_then_sequence", "append_only"]);
    expect(await selectValues(host, "#policyLateMode")).toEqual(["", "merge", "append"]);
    expect(await selectValues(host, "#policyTooLate")).toEqual(["", "accept", "quarantine", "drop"]);
    expect(await selectValues(host, "#policyLateToleranceUnit")).toEqual(["", "minutes", "hours", "days"]);
    expect(await selectValues(host, "#policyReprocessUnit")).toEqual(["", "minutes", "hours", "days"]);

    const goldEventOptions = await selectValues(host, "#policyEventTime");
    const goldSequenceOptions = await selectValues(host, "#policySequence");
    expect(goldEventOptions[0]).toBe("");
    expect(goldEventOptions.length).toBeGreaterThan(1);
    expect(goldSequenceOptions[0]).toBe("");

    const eventTimeChoice = goldEventOptions.find((value) => value) || "";
    expect(eventTimeChoice).toBeTruthy();
    const saveGoldPolicyBody = await saveProcessingPolicy({
      page,
      host,
      layerName: "gold",
      businessKeys: "data_items_id",
      orderingStrategy: "latest_event_time_wins",
      eventTimeColumn: eventTimeChoice,
      sequenceColumn: "",
      lateDataMode: "merge",
      tooLateBehavior: "quarantine",
      lateToleranceValue: "10",
      lateToleranceUnit: "minutes",
      reprocessValue: "24",
      reprocessUnit: "hours",
    });
    expect(saveGoldPolicyBody.proposal.processing_policy.business_keys).toEqual(["data_items_id"]);
    expect(saveGoldPolicyBody.proposal.processing_policy.ordering_strategy).toBe("latest_event_time_wins");
    expect(saveGoldPolicyBody.proposal.processing_policy.event_time_column).toBe(eventTimeChoice);
    console.log("gold-controls: valid policy save passed");

    const invalidGoldPolicyBody = await saveProcessingPolicy({
      page,
      host,
      layerName: "gold",
      businessKeys: "",
      orderingStrategy: "latest_event_time_wins",
      eventTimeColumn: "",
      sequenceColumn: "",
      lateDataMode: "merge",
      tooLateBehavior: "quarantine",
      lateToleranceValue: "10",
      lateToleranceUnit: "minutes",
      reprocessValue: "24",
      reprocessUnit: "hours",
    });
    expect(invalidGoldPolicyBody.proposal.processing_policy.business_keys || []).toEqual([]);
    const invalidGoldCompilePromise = page.waitForResponse((r) => r.url().endsWith("/compileGoldProposal") && r.request().method() === "POST");
    await detailAction(host, "Compile").click();
    const invalidGoldCompileResponse = await invalidGoldCompilePromise;
    expect(invalidGoldCompileResponse.status()).toBe(400);
    const invalidGoldCompileBody = await invalidGoldCompileResponse.json();
    expect(String(invalidGoldCompileBody.error || "")).toContain("failed static validation");
    console.log("gold-controls: invalid compile branch passed");

    await saveProcessingPolicy({
      page,
      host,
      layerName: "gold",
      businessKeys: "data_items_id",
      orderingStrategy: "latest_event_time_wins",
      eventTimeColumn: eventTimeChoice,
      sequenceColumn: "",
      lateDataMode: "merge",
      tooLateBehavior: "quarantine",
      lateToleranceValue: "10",
      lateToleranceUnit: "minutes",
      reprocessValue: "24",
      reprocessUnit: "hours",
    });
    const goldCompileBody = await compileCurrentProposal(page, host, "gold");
    expect(goldCompileBody.compiled_sql || goldCompileBody.select_sql).toMatch(/silver\."data_items_createdAtTime"|silver\."data_items_id"/);
    await expectCompileCardRendered(host);
    await host.locator("#viewSqlBtn").click();
    await expect(host.locator('.tab[data-tab="sql"].active')).toBeVisible({ timeout: 15000 });
    await openSqlPreviewAndCheck(page, host, /SELECT|UPPER|silver\./i);
    await host.locator('.tab[data-tab="detail"]').click();
    console.log("gold-controls: compile and sql checks passed");

    const goldProposalJson = psqlValue(`select proposal_json::text from model_proposal where proposal_id = ${goldProposalId}`);
    expect(goldProposalJson).toContain('"processing_policy"');
    expect(goldProposalJson).toContain('"business_keys":["data_items_id"]');
    expect(goldProposalJson).toContain('"ordering_strategy":"latest_event_time_wins"');

    await openProposalFromList(host, goldProposalId);
    console.log("gold-controls: reopened from proposal list");
    await expect(host.locator("#policyBusinessKeys")).toHaveValue("data_items_id");
    await expect(host.locator("#policyOrderingStrategy")).toHaveValue("latest_event_time_wins");
    await expect(host.locator("#policyEventTime")).toHaveValue(eventTimeChoice);
    await expect(host.locator("#policyTooLate")).toHaveValue("quarantine");
    await expect(goldMappingRow).toContainText('UPPER(silver."data_items_id")');

    await expectProposalListStatus(host, goldProposalId, "compiled");
    console.log("gold-controls: proposal list status verified");
    host = await reloadAndOpenProposal(page, "gold", goldProposalId);
    console.log("gold-controls: hard reload reopen passed");
    await expect(host.locator("#policyBusinessKeys")).toHaveValue("data_items_id");
    await expect(host.locator("#policyOrderingStrategy")).toHaveValue("latest_event_time_wins");
    await expect(host.locator("#policyEventTime")).toHaveValue(eventTimeChoice);
    await expect(host.locator("#policyTooLate")).toHaveValue("quarantine");
    await expect(host.locator("#mappingBody tr").filter({ hasText: "data_items_id" }).first()).toContainText('UPPER(silver."data_items_id")');
  });
});
