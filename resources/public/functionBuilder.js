import "./source/modules/smart.elements.js"

import EventHandler from './library/eventHandler.js'
import { request } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
<link rel="stylesheet" href="./source/styles/smart.default.css" />
<style>
  .container {
    background: white;
    border-radius: 15px;
    box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
    overflow: auto;
  }

  .content {
    padding: 30px;
    display: flex;
    flex-direction: column;
    gap: 30px;
  }

  .form-section {
    background: #f8f9fa;
    padding: 25px;
    border-radius: 10px;
    border-left: 4px solid #667eea;
  }

  .preview-section {
    background: #2c3e50;
    color: white;
    padding: 25px;
    border-radius: 10px;
    font-family: 'Courier New', monospace;
  }

  .form-group {
    margin-bottom: 20px;
  }

  .form-group label {
    display: block;
    margin-bottom: 8px;
    font-weight: 600;
    color: #2c3e50;
  }

  smart-text-box,
  smart-drop-down-list {
    width: 100%;
    height: 40px;
    margin-bottom: 10px;
  }

  .params-container,
  .lets-container,
  .outputs-container {
    border: 2px dashed #dee2e6;
    padding: 15px;
    border-radius: 8px;
    margin-top: 10px;
    min-height: 60px;
  }

  .param-item,
  .let-item,
  .output-item {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-bottom: 10px;
    padding: 10px;
    background: white;
    border-radius: 5px;
    border: 1px solid #e9ecef;
  }

  .param-item smart-text-box,
  .let-item smart-text-box {
    flex: 1;
    height: 35px;
  }

  .param-item smart-drop-down-list {
    width: 120px;
    height: 35px;
  }

  smart-drop-down-list {
    --smart-drop-down-list-default-width: 120px;
    --smart-drop-down-list-default-height: 35px;
  }

  .btn {
    padding: 8px 16px;
    border: none;
    border-radius: 5px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.3s ease;
  }

  .btn-add {
    background: #28a745;
    color: white;
  }

  .btn-add:hover {
    background: #218838;
    transform: translateY(-1px);
  }

  .btn-remove {
    background: #dc3545;
    color: white;
    padding: 5px 10px;
  }

  .btn-remove:hover {
    background: #c82333;
  }

  .btn-generate {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 12px 30px;
    font-size: 16px;
    margin-top: 20px;
    width: 100%;
  }

  .btn-generate:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
  }

  .preview-code {
    background: #1a1a1a;
    padding: 20px;
    border-radius: 8px;
    white-space: pre-wrap;
    font-size: 14px;
    line-height: 1.6;
    overflow-x: auto;
    min-height: 200px;
  }

  .keyword { color: #ff79c6; }
  .string { color: #f1fa8c; }
  .number { color: #bd93f9; }
  .operator { color: #ff5555; }
  .variable { color: #8be9fd; }
  .function { color: #50fa7b; }

  .section-title {
    font-size: 1.3em;
    margin-bottom: 20px;
    color: #2c3e50;
    border-bottom: 2px solid #667eea;
    padding-bottom: 10px;
  }

  .space-between {
    justify-content: space-between;
    display: flex;
    align-items: center;
  }

  .columns-hint {
    background: #e8f4f8;
    border: 1px solid #b8daff;
    border-radius: 6px;
    padding: 10px 14px;
    margin-bottom: 15px;
    font-size: 12px;
    color: #004085;
  }
  .columns-hint summary { cursor: pointer; font-weight: 600; }
  .columns-hint .col-list { margin-top: 6px; max-height: 100px; overflow-y: auto; }
  .columns-hint code { background: #d1ecf1; padding: 1px 4px; border-radius: 3px; }
</style>
<div class="container">
  <div class="content">
    <div class="form-section">
      <h2 class="section-title space-between">
        Logic Function
        <div style="gap:10px">
          <smart-button content="Save" id="saveButton" class="smart-button"></smart-button>
          <smart-button content="&#9747;" id="closeButton" class="smart-button"></smart-button>
        </div>
      </h2>

      <details class="columns-hint" id="columnsHint" style="display:none">
        <summary>Available Input Columns (from parent node)</summary>
        <div class="col-list" id="columnsList"></div>
      </details>

      <div class="form-group">
        <label for="functionName">Function Name:</label>
        <smart-text-box id="functionName" placeholder="MyFormula"></smart-text-box>
      </div>

      <div class="form-group">
        <label>Parameters (bind to input columns or define custom):</label>
        <div class="params-container" id="paramsContainer"></div>
        <button class="btn btn-add" id="parameterBtn">+ Add Parameter</button>
      </div>

      <div class="form-group">
        <label>Let Expressions (intermediate computations):</label>
        <div class="lets-container" id="letsContainer"></div>
        <button class="btn btn-add" id="letExpressionBtn">+ Add Let Expression</button>
      </div>

      <div class="form-group">
        <label for="returnExpression">Return Expression:</label>
        <textarea id="returnExpression" placeholder="d*d + e*e + a*b*c"
          style="width: 100%; height: 80px; border: 1px solid #ccc; border-radius: 4px; padding: 10px; font-family: 'Courier New', monospace; resize: vertical;"></textarea>

        <div style="display: flex; gap: 10px; margin-top: 10px; align-items: center;">
          <select id="functionCategory"
            style="width: 150px; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px;">
            <option value="">Select Category</option>
            <option value="math">Math Functions</option>
            <option value="string">String Functions</option>
            <option value="array">Array Functions</option>
            <option value="logical">Logical Functions</option>
            <option value="conversion">Conversion Functions</option>
          </select>

          <select id="functionList"
            style="width: 200px; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px;">
            <option value="">Select Function</option>
          </select>

          <span style="font-size: 12px; color: #666;">Select category first, then function to insert</span>
        </div>
      </div>

      <div class="form-group">
        <label>Output Columns (what this node produces for downstream nodes):</label>
        <div class="outputs-container" id="outputsContainer"></div>
        <button class="btn btn-add" id="addOutputBtn">+ Add Output</button>
      </div>

      <button class="btn btn-generate" id="generateFunctionBtn">Preview Generated Code</button>
    </div>

    <div class="preview-section">
      <h2 class="section-title" style="color: white; border-color: #667eea;">Generated Function</h2>
      <div class="preview-code" id="previewCode">
        // Your lambda function will appear here...
      </div>
    </div>
  </div>
</div>
`;

const functionCategories = {
  math: [
    { name: 'sin', desc: 'sin(x) - Sine' },
    { name: 'cos', desc: 'cos(x) - Cosine' },
    { name: 'tan', desc: 'tan(x) - Tangent' },
    { name: 'sqrt', desc: 'sqrt(x) - Square root' },
    { name: 'pow', desc: 'pow(x, y) - Power' },
    { name: 'abs', desc: 'abs(x) - Absolute value' },
    { name: 'log', desc: 'log(x) - Natural log' },
    { name: 'log10', desc: 'log10(x) - Base 10 log' },
    { name: 'exp', desc: 'exp(x) - Exponential' },
    { name: 'min', desc: 'min(x, y) - Minimum' },
    { name: 'max', desc: 'max(x, y) - Maximum' },
    { name: 'round', desc: 'round(x) - Round' },
    { name: 'floor', desc: 'floor(x) - Floor' },
    { name: 'ceil', desc: 'ceil(x) - Ceiling' },
  ],
  string: [
    { name: 'length', desc: 'length(str) - String length' },
    { name: 'substring', desc: 'substring(str, start, end)' },
    { name: 'indexOf', desc: 'indexOf(str, search)' },
    { name: 'toLowerCase', desc: 'toLowerCase(str)' },
    { name: 'toUpperCase', desc: 'toUpperCase(str)' },
    { name: 'trim', desc: 'trim(str)' },
    { name: 'replace', desc: 'replace(str, old, new)' },
    { name: 'concat', desc: 'concat(str1, str2)' },
  ],
  array: [
    { name: 'length', desc: 'length(array)' },
    { name: 'sum', desc: 'sum(array) - Sum all' },
    { name: 'average', desc: 'average(array)' },
    { name: 'filter', desc: 'filter(array, cond)' },
    { name: 'map', desc: 'map(array, fn)' },
    { name: 'sort', desc: 'sort(array)' },
    { name: 'reverse', desc: 'reverse(array)' },
  ],
  logical: [
    { name: 'if', desc: 'if(cond, trueVal, falseVal)' },
    { name: 'and', desc: 'and(a, b)' },
    { name: 'or', desc: 'or(a, b)' },
    { name: 'not', desc: 'not(a)' },
    { name: 'equals', desc: 'equals(a, b)' },
    { name: 'isNull', desc: 'isNull(value)' },
    { name: 'isEmpty', desc: 'isEmpty(value)' },
  ],
  conversion: [
    { name: 'toString', desc: 'toString(value)' },
    { name: 'toNumber', desc: 'toNumber(value)' },
    { name: 'toBoolean', desc: 'toBoolean(value)' },
    { name: 'parseInt', desc: 'parseInt(str)' },
    { name: 'parseFloat', desc: 'parseFloat(str)' },
    { name: 'toFixed', desc: 'toFixed(num, digits)' },
  ]
};

class LambdaFunctionBuilder extends HTMLElement {
  static get observedAttributes() {
    return ['visibility'];
  }

  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this._state = { paramCount: 0, letCount: 0 };
    this._render();
    this._updateVisibility();
  }

  connectedCallback() {
    this._cacheElements();
  }

  disconnectedCallback() {
    this._removeListeners();
  }

  attributeChangedCallback(name) {
    if (name === "visibility") {
      this._updateVisibility();
      if (this.style.display === "block") {
        this._cacheElements();
        this._addListeners();
        this._loadFromRect();
      } else {
        this._removeListeners();
      }
    }
  }

  _cacheElements() {
    const sr = this.shadowRoot;
    this.parameterBtn = sr.querySelector("#parameterBtn");
    this.letExpressionBtn = sr.querySelector("#letExpressionBtn");
    this.generateFunctionBtn = sr.querySelector("#generateFunctionBtn");
    this.addOutputBtn = sr.querySelector("#addOutputBtn");
    this.saveButton = sr.querySelector("#saveButton");
    this.closeButton = sr.querySelector("#closeButton");
    this.functionCategoryEl = sr.querySelector("#functionCategory");
    this.functionListEl = sr.querySelector("#functionList");
    this.columnsHint = sr.querySelector("#columnsHint");
    this.columnsList = sr.querySelector("#columnsList");
  }

  _addListeners() {
    EventHandler.on(this.parameterBtn, "click", () => this.addParameter(), {}, "LambdaFunction");
    EventHandler.on(this.letExpressionBtn, "click", () => this.addLetExpression(), {}, "LambdaFunction");
    EventHandler.on(this.generateFunctionBtn, "click", () => this.generateFunction(), {}, "LambdaFunction");
    EventHandler.on(this.addOutputBtn, "click", () => this.addOutput(), {}, "LambdaFunction");
    EventHandler.on(this.saveButton, "click", () => this.save(), {}, "LambdaFunction");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), {}, "LambdaFunction");
    EventHandler.on(this.functionCategoryEl, "change", () => this._updateFunctionList(), {}, "LambdaFunction");
    EventHandler.on(this.functionListEl, "change", () => this._insertSelectedFunction(), {}, "LambdaFunction");
  }

  _removeListeners() {
    EventHandler.removeGroup("LambdaFunction");
  }

  _render() {
    this.shadowRoot.append(template.content.cloneNode(true));
  }

  _updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  /** Load saved function data from the selected rectangle */
  _loadFromRect() {
    const rect = (window.data?.rectangles || []).find(
      (r) => String(r.id) === String(window.data?.selectedRectangle)
    );

    const sr = this.shadowRoot;

    // Show available input columns from parent
    const items = rect?.items || [];
    if (items.length > 0) {
      this.columnsList.innerHTML = items
        .map(col => `<div><code>${col.business_name || col.technical_name}</code> (${col.data_type || ""})</div>`)
        .join("");
      this.columnsHint.style.display = "block";
    } else {
      this.columnsHint.style.display = "none";
    }

    // Clear existing
    sr.querySelector("#paramsContainer").innerHTML = "";
    sr.querySelector("#letsContainer").innerHTML = "";
    sr.querySelector("#outputsContainer").innerHTML = "";

    // Load saved data or defaults
    const fnName = rect?.fn_name || "MyFunction";
    const fnParams = rect?.fn_params || [];
    const fnLets = rect?.fn_lets || [];
    const fnReturn = rect?.fn_return || "";
    const fnOutputs = rect?.fn_outputs || [];

    sr.querySelector("#functionName").value = fnName;
    sr.querySelector("#returnExpression").value = fnReturn;

    // If no saved data, populate default example
    if (fnParams.length === 0 && fnLets.length === 0 && !fnReturn) {
      // Auto-add parent columns as params if available
      if (items.length > 0) {
        items.forEach(col => {
          const name = (col.business_name || col.technical_name || "").split(".").pop();
          const dtype = col.data_type || "varchar";
          const mappedType = dtype.includes("int") ? "int" : dtype.includes("float") || dtype.includes("numeric") || dtype.includes("double") ? "double" : "string";
          this.addParameter(mappedType, name, col.business_name || col.technical_name);
        });
      } else {
        this.addParameter('int', 'a');
        this.addParameter('int', 'b');
        this.addParameter('int', 'c');
      }
      this.addLetExpression('d', '3 * a + b');
      this.addLetExpression('e', '(d / 4) + c');
      sr.querySelector("#returnExpression").value = 'd * d + e * e + a * b * c';
      this.addOutput("result", "double");
    } else {
      fnParams.forEach(p => this.addParameter(p.param_type || "int", p.param_name || "", p.source_column || ""));
      fnLets.forEach(l => this.addLetExpression(l.variable || "", l.expression || ""));
      fnOutputs.forEach(o => this.addOutput(o.output_name || "", o.data_type || "varchar"));
    }

    this.generateFunction();
  }

  // --- Parameter management ---
  addParameter(type = 'int', name = '', sourceColumn = '') {
    const container = this.shadowRoot.querySelector('#paramsContainer');
    const paramDiv = document.createElement('div');
    paramDiv.className = 'param-item';
    paramDiv.innerHTML = `
      <select class="param-type" style="width:100px;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
        ${['int','float','double','string','bool','long','decimal'].map(t =>
          `<option value="${t}" ${type === t ? 'selected' : ''}>${t}</option>`
        ).join('')}
      </select>
      <input type="text" class="param-name" placeholder="param name" value="${name}"
        style="flex:1;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
      <input type="text" class="source-column" placeholder="source column (optional)" value="${sourceColumn}"
        style="flex:1;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;font-size:11px;color:#666;">
      <button class="btn btn-remove">x</button>
    `;
    container.appendChild(paramDiv);
    paramDiv.querySelector(".btn-remove").onclick = () => paramDiv.remove();
    this._state.paramCount++;
  }

  // --- Let expression management ---
  addLetExpression(variable = '', expression = '') {
    const container = this.shadowRoot.getElementById('letsContainer');
    const letDiv = document.createElement('div');
    letDiv.className = 'let-item';
    letDiv.innerHTML = `
      <input type="text" class="let-variable" placeholder="variable" value="${variable}"
        style="flex:1;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
      <span style="font-size:18px;font-weight:bold;color:#2c3e50;margin:0 10px;">=</span>
      <input type="text" class="let-expression" placeholder="expression" value="${expression}"
        style="flex:2;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
      <button class="btn btn-remove">x</button>
    `;
    container.appendChild(letDiv);
    letDiv.querySelector(".btn-remove").onclick = () => letDiv.remove();
    this._state.letCount++;
  }

  // --- Output column management ---
  addOutput(name = '', dataType = 'varchar') {
    const container = this.shadowRoot.querySelector('#outputsContainer');
    const outDiv = document.createElement('div');
    outDiv.className = 'output-item';
    outDiv.innerHTML = `
      <input type="text" class="output-name" placeholder="output column name" value="${name}"
        style="flex:1;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
      <select class="output-type" style="width:120px;height:35px;border:1px solid #ccc;border-radius:4px;padding:5px;">
        ${['varchar','int','double','float','boolean','date','json'].map(t =>
          `<option value="${t}" ${dataType === t ? 'selected' : ''}>${t}</option>`
        ).join('')}
      </select>
      <button class="btn btn-remove">x</button>
    `;
    container.appendChild(outDiv);
    outDiv.querySelector(".btn-remove").onclick = () => outDiv.remove();
  }

  // --- Collect structured data ---
  collectData() {
    const sr = this.shadowRoot;
    const params = [];
    sr.querySelectorAll('.param-item').forEach(item => {
      const name = item.querySelector('.param-name').value.trim();
      if (name) {
        params.push({
          param_type: item.querySelector('.param-type').value,
          param_name: name,
          source_column: item.querySelector('.source-column').value.trim()
        });
      }
    });

    const lets = [];
    sr.querySelectorAll('.let-item').forEach(item => {
      const variable = item.querySelector('.let-variable').value.trim();
      const expression = item.querySelector('.let-expression').value.trim();
      if (variable && expression) {
        lets.push({ variable, expression });
      }
    });

    const outputs = [];
    sr.querySelectorAll('.output-item').forEach(item => {
      const name = item.querySelector('.output-name').value.trim();
      if (name) {
        outputs.push({
          output_name: name,
          data_type: item.querySelector('.output-type').value
        });
      }
    });

    return {
      fn_name: sr.querySelector('#functionName').value.trim() || "MyFunction",
      fn_params: params,
      fn_lets: lets,
      fn_return: sr.querySelector('#returnExpression').value.trim(),
      fn_outputs: outputs
    };
  }

  // --- Function category dropdown ---
  _updateFunctionList() {
    const category = this.functionCategoryEl.value;
    this.functionListEl.innerHTML = '<option value="">Select Function</option>';
    if (category && functionCategories[category]) {
      functionCategories[category].forEach(func => {
        const opt = document.createElement('option');
        opt.value = func.name;
        opt.textContent = func.desc;
        this.functionListEl.appendChild(opt);
      });
    }
  }

  _insertSelectedFunction() {
    const fn = this.functionListEl.value;
    if (!fn) return;
    const returnExpr = this.shadowRoot.getElementById('returnExpression');
    const pos = returnExpr.selectionStart || returnExpr.value.length;
    const v = returnExpr.value;
    returnExpr.value = v.slice(0, pos) + fn + '()' + v.slice(pos);
    returnExpr.focus();
    returnExpr.setSelectionRange(pos + fn.length + 1, pos + fn.length + 1);
    this.functionListEl.value = '';
  }

  // --- Generate preview code ---
  generateFunction() {
    const data = this.collectData();
    const { fn_name, fn_params, fn_lets, fn_return } = data;
    let code = '';

    // Clojure style (let over lambda — the core pattern)
    code += '<span class="keyword">;; Clojure — let over lambda</span>\n';
    code += `(<span class="keyword">defn</span> <span class="function">${fn_name}</span> [`;
    code += fn_params.map(p => `<span class="variable">${p.param_name}</span>`).join(' ');
    code += ']\n';
    if (fn_lets.length > 0) {
      code += '  (<span class="keyword">let</span> [';
      fn_lets.forEach((l, i) => {
        const prefix = i === 0 ? '' : '        ';
        code += `${prefix}<span class="variable">${l.variable}</span> <span class="string">${l.expression}</span>\n`;
      });
      code += '       ]\n';
      code += `    <span class="string">${fn_return}</span>))\n\n`;
    } else {
      code += `  <span class="string">${fn_return}</span>)\n\n`;
    }

    // JavaScript
    code += '<span class="keyword">// JavaScript</span>\n';
    code += `<span class="keyword">const</span> <span class="variable">${fn_name}</span> <span class="operator">=</span> (`;
    code += fn_params.map(p => `<span class="variable">${p.param_name}</span>`).join(', ');
    code += ') <span class="operator">=></span> {\n';
    fn_lets.forEach(l => {
      code += `  <span class="keyword">const</span> <span class="variable">${l.variable}</span> <span class="operator">=</span> <span class="string">${l.expression}</span>;\n`;
    });
    code += `  <span class="keyword">return</span> <span class="string">${fn_return}</span>;\n};\n\n`;

    // Python
    code += '<span class="keyword"># Python</span>\n';
    code += `<span class="keyword">def</span> <span class="function">${fn_name}</span>(`;
    code += fn_params.map(p => `<span class="variable">${p.param_name}</span>`).join(', ');
    code += '):\n';
    fn_lets.forEach(l => {
      code += `  <span class="variable">${l.variable}</span> <span class="operator">=</span> <span class="string">${l.expression}</span>\n`;
    });
    code += `  <span class="keyword">return</span> <span class="string">${fn_return}</span>\n`;

    this.shadowRoot.getElementById('previewCode').innerHTML = code;
  }

  // --- Save to backend ---
  async save() {
    const id = window.data?.selectedRectangle;
    if (!id) {
      console.warn("No selected rectangle to save logic function.");
      return;
    }

    const data = this.collectData();

    try {
      const resp = await request("/saveLogic", {
        method: "POST",
        body: { id, ...data },
      });
      console.log("Logic function saved:", resp);
      this.generateFunction();
    } catch (err) {
      console.error("Error saving logic function:", err);
    }
  }
}

if (!customElements.get('lambda-function-builder')) {
  customElements.define('lambda-function-builder', LambdaFunctionBuilder);
}
