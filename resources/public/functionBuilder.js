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
  .lets-container {
    border: 2px dashed #dee2e6;
    padding: 15px;
    border-radius: 8px;
    margin-top: 10px;
    min-height: 60px;
  }

  .param-item,
  .let-item {
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
    min-height: 400px;
  }

  .keyword {
    color: #ff79c6;
  }

  .string {
    color: #f1fa8c;
  }

  .number {
    color: #bd93f9;
  }

  .operator {
    color: #ff5555;
  }

  .variable {
    color: #8be9fd;
  }

  .function {
    color: #50fa7b;
  }

  @media (max-width: 768px) {
    .content {
      grid-template-columns: 1fr;
    }
  }

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

  .math-functions {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(80px, 1fr));
    gap: 5px;
    margin-top: 10px;
  }

  .math-btn {
    padding: 5px 8px;
    background: #667eea;
    color: white;
    border: none;
    border-radius: 3px;
    cursor: pointer;
    font-size: 12px;
  }

  .math-btn:hover {
    background: #5a67d8;
  }
</style>
<div class="container">
  <div class="content">
    <div class="form-section">
      <h2 class="section-title space-between">
        Function Definition
        <div style="gap=10px">
          <smart-button content="Save" id="saveButton" class="smart-button"></smart-button>
          <smart-button content="&#9747;" id="closeButton" class="smart-button"></smart-button>
        </div>
      </h2>

      <div class="form-group">
        <label for="functionName">Function Name:</label>
        <smart-text-box id="functionName" placeholder="MyFormula"></smart-text-box>
      </div>

      <div class="form-group">
        <label>Parameters:</label>
        <div class="params-container" id="paramsContainer">
          <!-- Parameters will be added here -->
        </div>
        <button class="btn btn-add" id="parameterBtn">+ Add Parameter</button>
      </div>

      <div class="form-group">
        <label>Let Expressions:</label>
        <div class="lets-container" id="letsContainer">
          <!-- Let expressions will be added here -->
        </div>
        <button class="btn btn-add" id="letExpressionBtn">+ Add Let Expression</button>
      </div>

      <div class="form-group">
        <label for="returnExpression">Return Expression:</label>
        <textarea id="returnExpression" placeholder="d*d + e*e + a*b*c"
          style="width: 100%; height: 80px; border: 1px solid #ccc; border-radius: 4px; padding: 10px; font-family: 'Courier New', monospace; resize: vertical;"></textarea>

        <div style="display: flex; gap: 10px; margin-top: 10px; align-items: center;">
          <select id="functionCategory" onchange="updateFunctionList()"
            style="width: 150px; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px;">
            <option value="">Select Category</option>
            <option value="math">Math Functions</option>
            <option value="string">String Functions</option>
            <option value="array">Array Functions</option>
            <option value="logical">Logical Functions</option>
            <option value="conversion">Conversion Functions</option>
          </select>

          <select id="functionList" onchange="insertSelectedFunction()"
            style="width: 200px; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px;">
            <option value="">Select Function</option>
          </select>

          <span style="font-size: 12px; color: #666;">Select category first, then function to insert</span>
        </div>
      </div>

      <button class="btn btn-generate" id="generateFunctionBtn">Generate Lambda Function</button>
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
    { name: 'sin', desc: 'sin(x) - Sine function' },
    { name: 'cos', desc: 'cos(x) - Cosine function' },
    { name: 'tan', desc: 'tan(x) - Tangent function' },
    { name: 'sqrt', desc: 'sqrt(x) - Square root' },
    { name: 'pow', desc: 'pow(x, y) - Power function' },
    { name: 'abs', desc: 'abs(x) - Absolute value' },
    { name: 'log', desc: 'log(x) - Natural logarithm' },
    { name: 'log10', desc: 'log10(x) - Base 10 logarithm' },
    { name: 'exp', desc: 'exp(x) - Exponential function' },
    { name: 'min', desc: 'min(x, y) - Minimum value' },
    { name: 'max', desc: 'max(x, y) - Maximum value' },
    { name: 'round', desc: 'round(x) - Round to nearest integer' },
    { name: 'floor', desc: 'floor(x) - Round down' },
    { name: 'ceil', desc: 'ceil(x) - Round up' },
    { name: 'random', desc: 'random() - Random number 0-1' }
  ],
  string: [
    { name: 'length', desc: 'length(str) - String length' },
    { name: 'substring', desc: 'substring(str, start, end) - Extract substring' },
    { name: 'indexOf', desc: 'indexOf(str, search) - Find position' },
    { name: 'toLowerCase', desc: 'toLowerCase(str) - Convert to lowercase' },
    { name: 'toUpperCase', desc: 'toUpperCase(str) - Convert to uppercase' },
    { name: 'trim', desc: 'trim(str) - Remove whitespace' },
    { name: 'replace', desc: 'replace(str, old, new) - Replace text' },
    { name: 'split', desc: 'split(str, delimiter) - Split string' },
    { name: 'concat', desc: 'concat(str1, str2) - Concatenate strings' },
    { name: 'startsWith', desc: 'startsWith(str, prefix) - Check prefix' },
    { name: 'endsWith', desc: 'endsWith(str, suffix) - Check suffix' }
  ],
  array: [
    { name: 'length', desc: 'length(array) - Array length' },
    { name: 'push', desc: 'push(array, item) - Add item' },
    { name: 'pop', desc: 'pop(array) - Remove last item' },
    { name: 'slice', desc: 'slice(array, start, end) - Extract portion' },
    { name: 'indexOf', desc: 'indexOf(array, item) - Find index' },
    { name: 'join', desc: 'join(array, separator) - Join to string' },
    { name: 'reverse', desc: 'reverse(array) - Reverse order' },
    { name: 'sort', desc: 'sort(array) - Sort array' },
    { name: 'filter', desc: 'filter(array, condition) - Filter items' },
    { name: 'map', desc: 'map(array, function) - Transform items' },
    { name: 'sum', desc: 'sum(array) - Sum all numbers' },
    { name: 'average', desc: 'average(array) - Calculate average' }
  ],
  logical: [
    { name: 'if', desc: 'if(condition, trueValue, falseValue) - Conditional' },
    { name: 'and', desc: 'and(a, b) - Logical AND' },
    { name: 'or', desc: 'or(a, b) - Logical OR' },
    { name: 'not', desc: 'not(a) - Logical NOT' },
    { name: 'equals', desc: 'equals(a, b) - Check equality' },
    { name: 'greaterThan', desc: 'greaterThan(a, b) - Compare values' },
    { name: 'lessThan', desc: 'lessThan(a, b) - Compare values' },
    { name: 'isNull', desc: 'isNull(value) - Check if null' },
    { name: 'isEmpty', desc: 'isEmpty(value) - Check if empty' },
    { name: 'switch', desc: 'switch(value, case1, result1, case2, result2, default) - Multiple conditions' }
  ],
  conversion: [
    { name: 'toString', desc: 'toString(value) - Convert to string' },
    { name: 'toNumber', desc: 'toNumber(value) - Convert to number' },
    { name: 'toBoolean', desc: 'toBoolean(value) - Convert to boolean' },
    { name: 'parseInt', desc: 'parseInt(str) - Parse integer' },
    { name: 'parseFloat', desc: 'parseFloat(str) - Parse decimal' },
    { name: 'toFixed', desc: 'toFixed(num, digits) - Format decimal places' },
    { name: 'toPrecision', desc: 'toPrecision(num, digits) - Format precision' },
    { name: 'toExponential', desc: 'toExponential(num) - Scientific notation' }
  ]
};

class LambdaFunctionBuilder extends HTMLElement {
  // List of attributes to observe. Keep in sync with `get/set` props if used.
  static get observedAttributes() {
    return ['visibility'];
  }

  constructor() {
    super();
    // Attach shadow DOM
    this.attachShadow({ mode: 'open' });

    // Internal state
    this._state = {
      paramCount: 0,
      letCount: 0
    };

    this._updateVisibility();
    // Initial render
    this._render();
  }

  /* ------------------ lifecycle ------------------ */
  connectedCallback() {
    this.parameterBtn = this.shadowRoot.querySelector("#parameterBtn");
    this.letExpressionBtn = this.shadowRoot.querySelector("#letExpressionBtn");
    this.generateFunctionBtn = this.shadowRoot.querySelector("#generateFunctionBtn");
    this.saveButton = this.shadowRoot.querySelector("#saveButton");
    this.closeButton = this.shadowRoot.querySelector("#closeButton");

    this.shadowRoot.getElementById('functionName').value = 'MyFormula';
    this.addParameter('int', 'a');
    this.addParameter('int', 'b');
    this.addParameter('int', 'c');
    this.addLetExpression('d', '3 * a + b');
    this.addLetExpression('e', '(d/4) + c');
    this.shadowRoot.getElementById('returnExpression').value = 'd*d + e*e + a*b*c';
    this.generateFunction();
  }

  disconnectedCallback() {
    this._removeListeners();
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (name === "visibility") {
      this._updateVisibility();
      if (this.style.display === "block") this._addListeners();
      if (this.style.display === "none") this._removeListeners();
    }
  }

  /* ------------------ event listeners ------------------ */
  _addListeners() { 
    EventHandler.on(this.parameterBtn, "click", this.addParameter.bind(this), {}, "LambdaFunction");
    EventHandler.on(this.letExpressionBtn, "click", this.addLetExpression.bind(this), {}, "LambdaFunction");
    EventHandler.on(this.generateFunctionBtn, "click", this.generateFunction.bind(this), {}, "LambdaFunction");
    EventHandler.on(this.saveButton, "click", this.save.bind(this), false, "LambdaFunction");
    EventHandler.on(this.closeButton, "click", () => this.setAttribute("visibility", "close"), false, "LambdaFunction");
  }

  _removeListeners() {
    EventHandler.removeGroup("LambdaFunction");
   }

  /* ------------------ rendering helpers ------------------ */
  _render() {
    this.shadowRoot.append(template.content.cloneNode(true));
  }

  _updateVisibility() {
    this.style.display = this.getAttribute("visibility") === "open" ? "block" : "none";
  }

  /* ------------------ others ------------------ */
  addParameter(type = 'int', name = '') {
    const container = this.shadowRoot.querySelector('#paramsContainer');
    const paramDiv = document.createElement('div');
    paramDiv.className = 'param-item';
    paramDiv.innerHTML = `
                <select class="param-type" style="width: 120px; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px;">
                  <option value="int" ${type === 'int' ? 'selected' : ''}>int</option>
                  <option value="float" ${type === 'float' ? 'selected' : ''}>float</option>
                  <option value="double" ${type === 'double' ? 'selected' : ''}>double</option>
                  <option value="string" ${type === 'string' ? 'selected' : ''}>string</option>
                  <option value="bool" ${type === 'bool' ? 'selected' : ''}>bool</option>
                  <option value="char" ${type === 'char' ? 'selected' : ''}>char</option>
                  <option value="long" ${type === 'long' ? 'selected' : ''}>long</option>
                  <option value="short" ${type === 'short' ? 'selected' : ''}>short</option>
                  <option value="byte" ${type === 'byte' ? 'selected' : ''}>byte</option>
                  <option value="decimal" ${type === 'decimal' ? 'selected' : ''}>decimal</option>
                </select>
                <input type="text" class="param-name" placeholder="parameter name" value="${name}" style="flex: 1; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px; margin: 0 10px;">
                <button class="btn btn-remove">×</button>
            `;
    container.appendChild(paramDiv);
    const btn = paramDiv.querySelector(".btn-remove");
    btn.onclick = () => this.removeParameter(btn);
    this._state.paramCount++;
  }

  removeParameter(button) {
    button.parentElement.remove();
  }

  addLetExpression(variable = '', expression = '') {
    const container = this.shadowRoot.getElementById('letsContainer');
    const letDiv = document.createElement('div');
    letDiv.className = 'let-item';
    letDiv.innerHTML = `
                <input type="text" class="let-variable" placeholder="variable" value="${variable}" style="flex: 1; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px; margin-right: 10px;">
                <span style="font-size: 18px; font-weight: bold; color: #2c3e50; margin: 0 10px;">=</span>
                <input type="text" class="let-expression" placeholder="expression" value="${expression}" style="flex: 2; height: 35px; border: 1px solid #ccc; border-radius: 4px; padding: 5px; margin-left: 10px;">
                <button class="btn btn-remove">×</button>
            `;
    container.appendChild(letDiv);
    const btn = letDiv.querySelector(".btn-remove");
    btn.onclick = () => this.removeLetExpression(btn);
    this._state.letCount++;
  }

  removeLetExpression(button) {
    button.parentElement.remove();
  }

  updateFunctionList() {
      const categorySelect = this.shadowRoot.getElementById('functionCategory');
      const functionSelect = this.shadowRoot.getElementById('functionList');
      const selectedCategory = categorySelect.value;

      // Clear function list
      functionSelect.innerHTML = '<option value="">Select Function</option>';

      if (selectedCategory && functionCategories[selectedCategory]) {
        functionSelect.disabled = false;
        functionCategories[selectedCategory].forEach(func => {
          const option = document.createElement('option');
          option.value = func.name;
          option.textContent = func.desc;
          functionSelect.appendChild(option);
        });
      } else {
        functionSelect.disabled = true;
      }
    }

    insertSelectedFunction() {
      const functionSelect = this.shadowRoot.getElementById('functionList');
      const selectedFunction = functionSelect.value;

      if (selectedFunction) {
        insertFunction(selectedFunction);
        // Reset the dropdown after insertion
        functionSelect.value = '';
      }
    }

    insertFunction(funcName) {
      const returnExpr = this.shadowRoot.getElementById('returnExpression');
      const cursorPos = returnExpr.selectionStart || returnExpr.value.length;
      const currentValue = returnExpr.value;
      const newValue = currentValue.slice(0, cursorPos) + funcName + '()' + currentValue.slice(cursorPos);
      returnExpr.value = newValue;
      returnExpr.focus();
      returnExpr.setSelectionRange(cursorPos + funcName.length + 1, cursorPos + funcName.length + 1);
      generateFunction();
    }

    generateFunction() {
      const functionName = this.shadowRoot.getElementById('functionName').value || 'MyFunction';
      const returnExpression = this.shadowRoot.getElementById('returnExpression').value || 'return_value';

      // Get parameters
      const paramItems = this.shadowRoot.querySelectorAll('.param-item');
      const parameters = [];
      paramItems.forEach(item => {
        const type = item.querySelector('.param-type').value;
        const name = item.querySelector('.param-name').value;
        if (name) {
          parameters.push({ type, name });
        }
      });

      // Get let expressions
      const letItems = this.shadowRoot.querySelectorAll('.let-item');
      const letExpressions = [];
      letItems.forEach(item => {
        const variable = item.querySelector('.let-variable').value;
        const expression = item.querySelector('.let-expression').value;
        if (variable && expression) {
          letExpressions.push({ variable, expression });
        }
      });

      // Generate the lambda function code
      let code = '';

      // C# Lambda Style
      code += '<span class="keyword">// C# Lambda Function</span>\n';
      code += `<span class="keyword">Func</span>&lt;`;
      parameters.forEach((param, index) => {
        code += `<span class="keyword">${param.type}</span>${index < parameters.length - 1 ? ', ' : ''}`;
      });
      code += `, <span class="keyword">dynamic</span>&gt; <span class="variable">${functionName}</span> = (`;
      parameters.forEach((param, index) => {
        code += `<span class="variable">${param.name}</span>${index < parameters.length - 1 ? ', ' : ''}`;
      });
      code += ') =>\n{\n';

      letExpressions.forEach(exp => {
        code += `    <span class="keyword">var</span> <span class="variable">${exp.variable}</span> <span class="operator">=</span> <span class="string">${exp.expression}</span>;\n`;
      });

      code += `    <span class="keyword">return</span> <span class="string">${returnExpression}</span>;\n`;
      code += '};\n\n';

      // JavaScript Lambda Style
      code += '<span class="keyword">// JavaScript Lambda Function</span>\n';
      code += `<span class="keyword">const</span> <span class="variable">${functionName}</span> <span class="operator">=</span> (`;
      parameters.forEach((param, index) => {
        code += `<span class="variable">${param.name}</span>${index < parameters.length - 1 ? ', ' : ''}`;
      });
      code += ') <span class="operator">=></span> {\n';

      letExpressions.forEach(exp => {
        code += `    <span class="keyword">const</span> <span class="variable">${exp.variable}</span> <span class="operator">=</span> <span class="string">${exp.expression}</span>;\n`;
      });

      code += `    <span class="keyword">return</span> <span class="string">${returnExpression}</span>;\n`;
      code += '};\n\n';

      // Python Lambda Style  
      code += '<span class="keyword"># Python Lambda Function</span>\n';
      code += `<span class="variable">${functionName}</span> <span class="operator">=</span> <span class="keyword">lambda</span> `;
      parameters.forEach((param, index) => {
        code += `<span class="variable">${param.name}</span>${index < parameters.length - 1 ? ', ' : ''}`;
      });
      code += ': (\n';

      if (letExpressions.length > 0) {
        code += '    # Let expressions (using walrus operator in Python 3.8+)\n';
        letExpressions.forEach((exp, index) => {
          if (index === letExpressions.length - 1) {
            code += `    <span class="string">${returnExpression.replace(new RegExp(exp.variable, 'g'), `(${exp.variable} := ${exp.expression})`)}</span>\n`;
          } else {
            code += `    # <span class="variable">${exp.variable}</span> = <span class="string">${exp.expression}</span>\n`;
          }
        });
      } else {
        code += `    <span class="string">${returnExpression}</span>\n`;
      }
      code += ')\n\n';

      // Usage Example
      code += '<span class="keyword">// Usage Example:</span>\n';
      if (parameters.length > 0) {
        const exampleArgs = parameters.map(p => {
          switch (p.type) {
            case 'int': return '1';
            case 'float':
            case 'double': return '1.0';
            case 'string': return '"test"';
            case 'bool': return 'true';
            default: return '1';
          }
        }).join(', ');
        code += `<span class="keyword">var</span> <span class="variable">result</span> <span class="operator">=</span> <span class="function">${functionName}</span>(<span class="number">${exampleArgs}</span>);`;
      }

      this.shadowRoot.getElementById('previewCode').innerHTML = code;
    }

    save() {
      request("/saveFunction", {
        method: "POST",
        body: { function: this.shadowRoot.getElementById('previewCode').innerHTML }
      }).then((res) => {
        console.log(res);
      }).catch((error) => console.error(error));
    }
}

// Define the element (guard against duplicate registration)
if (!customElements.get('lambda-function-builder')) customElements.define('lambda-function-builder', LambdaFunctionBuilder);