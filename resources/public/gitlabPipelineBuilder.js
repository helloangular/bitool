import { request } from "./library/utils.js";

const template = document.createElement('template');
template.innerHTML = `
    <style>
      :host {
        display: block;
        height: 100%;
        width: 100%;
        --color-primary: #4a90e2;
        --color-primary-dark: #2a5fa8;
        --color-success: #10b981;
        --color-warning: #f59e0b;
        --color-error: #ef4444;
        --color-info: #3b82f6;
        --color-job: #8b5cf6;
        --color-stage: #06b6d4;
        --color-artifacts: #10b981;
        --color-cache: #22c55e;
        --color-service: #f97316;
        --color-bg: #ffffff;
        --color-bg-alt: #f5f7fa;
        --color-border: #e5e7eb;
        --color-text: #1f2937;
        --color-text-muted: #6b7280;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', sans-serif;
        background: var(--color-bg-alt);
        color: var(--color-text);
        line-height: 1.6;
      }

      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }

      /* Header */
      .header {
        background: white;
        border-bottom: 1px solid var(--color-border);
        padding: 16px 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }

      .header h1 {
        font-size: 20px;
        font-weight: 600;
        color: var(--color-text);
        margin: 0;
      }

      .header-actions {
        display: flex;
        gap: 12px;
      }

      .btn {
        padding: 8px 16px;
        border: 1px solid var(--color-border);
        border-radius: 6px;
        background: white;
        color: var(--color-text);
        font-size: 14px;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.2s;
      }

      .btn:hover {
        background: var(--color-bg-alt);
        border-color: var(--color-primary);
      }

      .btn-primary {
        background: var(--color-primary);
        color: white;
        border-color: var(--color-primary);
      }

      .btn-primary:hover {
        background: var(--color-primary-dark);
      }

      /* Component Palette */
      .palette {
        background: var(--color-bg-alt);
        border-bottom: 1px solid var(--color-border);
        padding: 12px 24px;
        display: flex;
        gap: 8px;
        overflow-x: auto;
        flex-wrap: wrap;
      }

      .palette-group {
        display: flex;
        gap: 8px;
        padding-right: 16px;
        border-right: 1px solid var(--color-border);
      }

      .palette-group:last-child {
        border-right: none;
      }

      .palette-item {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 8px 14px;
        border: 1px solid var(--color-border);
        border-radius: 6px;
        background: white;
        cursor: grab;
        transition: all 0.2s;
        font-size: 13px;
        font-weight: 500;
        user-select: none;
      }

      .palette-item:hover {
        background: #e8f4fd;
        border-color: var(--color-primary);
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
      }

      .palette-item.dragging {
        opacity: 0.5;
        cursor: grabbing;
      }

      .palette-icon {
        font-size: 18px;
      }

      /* Main Layout */
      .main-layout {
        display: grid;
        grid-template-columns: 1fr 450px;
        height: calc(100% - 130px);
        background: var(--color-bg-alt);
      }

      /* Canvas */
      .canvas {
        background: white;
        padding: 24px;
        overflow-y: auto;
        border-right: 1px solid var(--color-border);
      }

      .canvas-empty {
        text-align: center;
        padding: 80px 20px;
        color: var(--color-text-muted);
      }

      .canvas-empty-icon {
        font-size: 64px;
        margin-bottom: 16px;
      }

      .canvas-empty h3 {
        font-size: 18px;
        margin-bottom: 8px;
        color: var(--color-text);
      }

      .canvas-empty p {
        font-size: 14px;
        margin-bottom: 24px;
      }

      /* Tree Node */
      .tree-node {
        margin-bottom: 8px;
      }

      .node-item {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 10px 12px;
        border: 2px solid transparent;
        border-radius: 6px;
        background: var(--color-bg-alt);
        cursor: pointer;
        transition: all 0.2s;
        position: relative;
      }

      .node-item:hover {
        background: #e8f4fd;
      }

      .node-item.selected {
        background: #dbeafe;
        border-color: var(--color-primary);
      }

      .node-item.drop-zone-valid {
        border: 2px dashed var(--color-success);
        background: rgba(16, 185, 129, 0.05);
      }

      .node-item.drop-zone-invalid {
        border: 2px dashed var(--color-error);
        background: rgba(239, 68, 68, 0.05);
      }

      .node-item.drop-target-active {
        border: 3px solid var(--color-primary);
        background: rgba(59, 130, 246, 0.1);
      }

      .node-expand {
        width: 20px;
        height: 20px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        border-radius: 3px;
        font-size: 12px;
      }

      .node-expand:hover {
        background: rgba(0,0,0,0.1);
      }

      .node-icon {
        font-size: 16px;
      }

      .node-label {
        flex: 1;
        font-size: 14px;
        font-weight: 500;
      }

      .node-meta {
        font-size: 12px;
        color: var(--color-text-muted);
      }

      .node-actions {
        display: flex;
        gap: 4px;
        opacity: 0;
        transition: opacity 0.2s;
      }

      .node-item:hover .node-actions {
        opacity: 1;
      }

      .node-action-btn {
        width: 24px;
        height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
        border: 1px solid var(--color-border);
        border-radius: 4px;
        background: white;
        cursor: pointer;
        font-size: 12px;
      }

      .node-action-btn:hover {
        background: var(--color-bg-alt);
        border-color: var(--color-primary);
      }

      .node-children {
        margin-left: 32px;
        margin-top: 8px;
      }

      .node-error::before {
        content: "⚠️";
        margin-right: 4px;
      }

      .node-valid::after {
        content: "✓";
        color: var(--color-success);
        margin-left: 4px;
        font-size: 12px;
      }

      /* Properties Panel */
      .properties-panel {
        background: white;
        padding: 24px;
        overflow-y: auto;
      }

      .properties-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
        padding-bottom: 16px;
        border-bottom: 2px solid var(--color-border);
      }

      .properties-title {
        font-size: 16px;
        font-weight: 600;
        color: var(--color-text);
      }

      .properties-empty {
        text-align: center;
        padding: 60px 20px;
        color: var(--color-text-muted);
      }

      .properties-empty-icon {
        font-size: 48px;
        margin-bottom: 16px;
      }

      .form-group {
        margin-bottom: 20px;
      }

      .form-label {
        display: block;
        font-size: 13px;
        font-weight: 500;
        color: var(--color-text);
        margin-bottom: 6px;
      }

      .form-label.required::after {
        content: "*";
        color: var(--color-error);
        margin-left: 4px;
      }

      .form-input,
      .form-select,
      .form-textarea {
        width: 100%;
        padding: 8px 12px;
        border: 1px solid var(--color-border);
        border-radius: 6px;
        font-size: 14px;
        font-family: inherit;
        transition: border-color 0.2s;
      }

      .form-input:focus,
      .form-select:focus,
      .form-textarea:focus {
        outline: none;
        border-color: var(--color-primary);
      }

      .form-textarea {
        resize: vertical;
        min-height: 80px;
        font-family: 'Monaco', 'Courier New', monospace;
      }

      .form-checkbox {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 12px;
      }

      .form-checkbox input {
        width: 18px;
        height: 18px;
        cursor: pointer;
      }

      .form-checkbox label {
        font-size: 14px;
        cursor: pointer;
      }

      .form-section {
        background: var(--color-bg-alt);
        padding: 16px;
        border-radius: 6px;
        margin-bottom: 20px;
      }

      .form-section-title {
        font-size: 14px;
        font-weight: 600;
        margin-bottom: 12px;
        color: var(--color-text);
      }

      .form-help {
        font-size: 12px;
        color: var(--color-text-muted);
        margin-top: 4px;
      }

      .form-error {
        font-size: 12px;
        color: var(--color-error);
        margin-top: 4px;
      }

      .tag-input-container {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        padding: 8px;
        border: 1px solid var(--color-border);
        border-radius: 6px;
        min-height: 40px;
      }

      .tag {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        background: var(--color-primary);
        color: white;
        border-radius: 12px;
        font-size: 12px;
      }

      .tag-remove {
        cursor: pointer;
        font-weight: bold;
      }

      .tag-input {
        flex: 1;
        border: none;
        outline: none;
        min-width: 100px;
        font-size: 14px;
      }

      .array-item {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 8px;
      }

      .array-item input {
        flex: 1;
      }

      .array-item-remove {
        width: 32px;
        height: 32px;
        display: flex;
        align-items: center;
        justify-content: center;
        border: 1px solid var(--color-border);
        border-radius: 4px;
        background: white;
        cursor: pointer;
        color: var(--color-error);
      }

      .array-item-remove:hover {
        background: #fef2f2;
      }

      .btn-add {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 12px;
        border: 1px dashed var(--color-border);
        border-radius: 6px;
        background: white;
        color: var(--color-text-muted);
        font-size: 13px;
        cursor: pointer;
        transition: all 0.2s;
      }

      .btn-add:hover {
        border-color: var(--color-primary);
        color: var(--color-primary);
      }

      /* Status Bar */
      .status-bar {
        background: white;
        border-top: 1px solid var(--color-border);
        padding: 12px 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 13px;
      }

      .status-info {
        display: flex;
        align-items: center;
        gap: 16px;
      }

      .status-item {
        display: flex;
        align-items: center;
        gap: 6px;
      }

      .status-icon-valid {
        color: var(--color-success);
      }

      .status-icon-error {
        color: var(--color-error);
      }

      /* Utility Classes */
      .hidden {
        display: none !important;
      }

      .mt-2 {
        margin-top: 16px;
      }

      /* Animations */
      @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.6; }
      }

      .pulse {
        animation: pulse 1s infinite;
      }

      /* Scrollbar */
      ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
      }

      ::-webkit-scrollbar-track {
        background: var(--color-bg-alt);
      }

      ::-webkit-scrollbar-thumb {
        background: var(--color-border);
        border-radius: 4px;
      }

      ::-webkit-scrollbar-thumb:hover {
        background: #9ca3af;
      }
    </style>

    <!-- Header -->
    <div class="header">
      <h1>🚀 GitLab Pipeline Builder</h1>
      <div class="header-actions">
        <button class="btn" onclick="this.getRootNode().host.validatePipeline()">🔍 Validate</button>
        <button class="btn btn-primary" onclick="this.getRootNode().host.showPipelineModel()">📊 View Model</button>
      </div>
    </div>

    <!-- Component Palette -->
    <div class="palette">
      <!-- Structure -->
      <div class="palette-group">
        <div class="palette-item" draggable="true" data-type="stages" data-group="structure">
          <span class="palette-icon">📊</span>
          <span>Stages</span>
        </div>
        <div class="palette-item" draggable="true" data-type="job" data-group="structure">
          <span class="palette-icon">💼</span>
          <span>Job</span>
        </div>
        <div class="palette-item" draggable="true" data-type="step" data-group="structure">
          <span class="palette-icon">▶️</span>
          <span>Step</span>
        </div>
      </div>

      <!-- Dependencies -->
      <div class="palette-group">
        <div class="palette-item" draggable="true" data-type="needs" data-group="dependencies">
          <span class="palette-icon">🔗</span>
          <span>Needs</span>
        </div>
      </div>

      <!-- Artifacts & Cache -->
      <div class="palette-group">
        <div class="palette-item" draggable="true" data-type="artifacts" data-group="data">
          <span class="palette-icon">📦</span>
          <span>Artifacts</span>
        </div>
        <div class="palette-item" draggable="true" data-type="cache" data-group="data">
          <span class="palette-icon">💾</span>
          <span>Cache</span>
        </div>
      </div>

      <!-- Environment -->
      <div class="palette-group">
        <div class="palette-item" draggable="true" data-type="environment" data-group="environment">
          <span class="palette-icon">🌍</span>
          <span>Environment</span>
        </div>
        <div class="palette-item" draggable="true" data-type="service" data-group="environment">
          <span class="palette-icon">🗄️</span>
          <span>Service</span>
        </div>
      </div>

      <!-- Control Flow -->
      <div class="palette-group">
        <div class="palette-item" draggable="true" data-type="rules" data-group="control">
          <span class="palette-icon">📋</span>
          <span>Rules</span>
        </div>
        <div class="palette-item" draggable="true" data-type="variables" data-group="control">
          <span class="palette-icon">🎯</span>
          <span>Variables</span>
        </div>
      </div>
    </div>

    <!-- Main Layout -->
    <div class="main-layout">
      <!-- Canvas -->
      <div class="canvas" id="canvas">
        <div class="canvas-empty" id="canvas-empty">
          <div class="canvas-empty-icon">📄</div>
          <h3>Empty Pipeline</h3>
          <p>Drag components from the top menu to build your pipeline</p>
          <button class="btn btn-primary" onclick="this.getRootNode().host.addFirstJob()">+ Add Job</button>
        </div>
        <div id="pipeline-tree" class="hidden"></div>
      </div>

      <!-- Properties Panel -->
      <div class="properties-panel" id="properties-panel">
        <div class="properties-empty" id="properties-empty">
          <div class="properties-empty-icon">👆</div>
          <p>Select a node to view and edit its properties</p>
        </div>
        <div id="properties-content" class="hidden"></div>
      </div>
    </div>

    <!-- Status Bar -->
    <div class="status-bar">
      <div class="status-info">
        <div class="status-item">
          <span class="status-icon-valid">✓</span>
          <span id="status-text">Pipeline valid</span>
        </div>
        <div class="status-item">
          <span>•</span>
          <span id="status-jobs">0 jobs</span>
        </div>
        <div class="status-item">
          <span>•</span>
          <span id="status-stages">0 stages</span>
        </div>
      </div>
    </div>
    `;


// TODO: How can I implement a toast notification system to show feedback when backend requests succeed or fail?

export class GitlabPipelineBuilder extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });

    // Global State
    this.pipeline = {
      id: 'pipeline-root',
      type: 'pipeline',
      name: 'my-pipeline',
      children: {}
    };

    this.selectedNode = null;
    this.draggedType = null;

    // Strategy map for rendering property forms
    this.formRenderers = {
      'job': this.renderJobForm,
      'step': this.renderStepForm,
      'stages': this.renderStagesForm,
      'artifacts': this.renderArtifactsForm,
      'cache': this.renderCacheForm,
      'environment': this.renderEnvironmentForm,
      'variable': this.renderVariableForm,
      'need': this.renderNeedForm,
      'service': this.renderServiceForm,
      'rule': this.renderRuleForm,
    };
  }

  connectedCallback() {
    if (this._initialized) return;
    this.render();
    this.initializeDragDrop();
    this.updateStatus();
    this._initialized = true;
  }

  render() {
    this.shadowRoot.appendChild(template.content.cloneNode(true));
  }

  updateStatus() {
  }

  initializeDragDrop() {
    const paletteItems = this.shadowRoot.querySelectorAll('.palette-item');

    paletteItems.forEach(item => {
      item.addEventListener('dragstart', (e) => {
        this.draggedType = e.target.dataset.type;
        e.target.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'copy';
      });

      item.addEventListener('dragend', (e) => {
        e.target.classList.remove('dragging');
        this.draggedType = null;
      });
    });

    const canvas = this.shadowRoot.getElementById('canvas');
    this.setupDropZone(canvas);
  }

  setupDropZone(element) {
    element.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
    });

    element.addEventListener('drop', (e) => {
      e.preventDefault();
      if (this.draggedType) {
        this.handleDrop(this.draggedType, this.pipeline);
      }
    });
  }

  handleDrop(type, targetNode) {
    const nodeId = 'node-' + Date.now();
    let newNode = null;

    switch (type) {
      case 'stages':
        if (this.pipeline.children.stages) {
          alert('Pipeline already has a stages block');
          return;
        }
        newNode = {
          id: nodeId,
          type: 'stages',
          list: ['build', 'test', 'deploy']
        };
        this.pipeline.children.stages = newNode;
        this.showNotification('✓ Created stages');
        this.notifyBackend('stages added', newNode);
        break;

      case 'job':
        if (!this.pipeline.children.jobs) {
          this.pipeline.children.jobs = {
            id: 'jobs-container',
            type: 'jobs',
            children: []
          };
        }
        newNode = this.createJob(nodeId);
        this.pipeline.children.jobs.children.push(newNode);
        this.showNotification('✓ Created job');
        this.notifyBackend('job added', newNode);
        break;

      case 'variables':
        if (!this.pipeline.children.variables) {
          this.pipeline.children.variables = {
            id: 'variables-' + Date.now(),
            type: 'variables',
            children: []
          };
        }
        newNode = {
          id: nodeId,
          type: 'variable',
          name: 'NEW_VAR',
          value: 'value'
        };
        this.pipeline.children.variables.children.push(newNode);
        this.showNotification('✓ Added variable');
        this.notifyBackend('variable added', newNode);
        break;

      default:
        alert(`Drop ${type} onto a job instead`);
        return;
    }

    this.renderPipeline();
    this.updateStatus();
    if (newNode) {
      setTimeout(() => this.selectNode(newNode), 100);
    }
  }

  createJob(id) {
    const jobNum = (this.pipeline.children.jobs?.children?.length || 0) + 1;
    return {
      id: id,
      type: 'job',
      name: `job-${jobNum}`,
      properties: {
        stage: 'test',
        image: '',
        tags: [],
        when: 'on_success',
        'allow-failure': false,
        interruptible: true
      },
      children: {
        steps: {
          id: 'steps-' + Date.now(),
          type: 'steps',
          children: [
            {
              id: 'step-' + Date.now(),
              type: 'step',
              run: 'echo "Hello World"'
            }
          ]
        }
      }
    };
  }

  addFirstJob() {
    this.handleDrop('job', this.pipeline);
  }

  renderPipeline() {
    const canvasEmpty = this.shadowRoot.getElementById('canvas-empty');
    const pipelineTree = this.shadowRoot.getElementById('pipeline-tree');

    const hasContent = this.pipeline.children.jobs?.children?.length > 0 ||
      this.pipeline.children.stages ||
      this.pipeline.children.variables?.children?.length > 0;

    if (!hasContent) {
      canvasEmpty.classList.remove('hidden');
      pipelineTree.classList.add('hidden');
      return;
    }

    canvasEmpty.classList.add('hidden');
    pipelineTree.classList.remove('hidden');

    pipelineTree.innerHTML = this.renderNode(this.pipeline);
    this.attachNodeEventListeners();
  }

  renderNode(node, depth = 0) {
    let html = '';
    const hasChildren = node.children && Object.keys(node.children).length > 0;
    const icon = this.getNodeIcon(node.type);
    const label = this.getNodeLabel(node);
    const meta = this.getNodeMeta(node);
    const isExpanded = !node._collapsed;

    html += `<div class="tree-node">`;
    html += `<div class="node-item" data-id="${node.id}" data-type="${node.type}">`;

    if (hasChildren) {
      html += `<div class="node-expand">${isExpanded ? '▼' : '▶'}</div>`;
    } else {
      html += `<div class="node-expand"></div>`;
    }

    html += `<span class="node-icon">${icon}</span>`;
    html += `<span class="node-label">${label}</span>`;
    if (meta) {
      html += `<span class="node-meta">${meta}</span>`;
    }

    html += `<div class="node-actions">`;
    html += `<div class="node-action-btn" title="Edit">🔧</div>`;
    if (node.type !== 'pipeline') {
      html += `<div class="node-action-btn" title="Delete">🗑️</div>`;
    }
    html += `</div></div>`;

    if (hasChildren && isExpanded) {
      html += `<div class="node-children">`;

      for (const key in node.children) {
        const child = node.children[key];
        if (Array.isArray(child)) {
          child.forEach(c => {
            html += this.renderNode(c, depth + 1);
          });
        } else if (child.children && Array.isArray(child.children)) {
          child.children.forEach(c => {
            html += this.renderNode(c, depth + 1);
          });
        } else if (child && typeof child === 'object') {
          html += this.renderNode(child, depth + 1);
        }
      }

      html += `</div>`;
    }

    html += `</div>`;
    return html;
  }

  getNodeIcon(type) {
    const icons = {
      pipeline: '📄', stages: '📊', job: '💼', steps: '▶️', step: '▶️',
      artifacts: '📦', cache: '💾', environment: '🌍', service: '🗄️',
      needs: '🔗', rules: '📋', variables: '🎯', variable: '🔖',
      paths: '📁', need: '🔗', rule: '📋'
    };
    return icons[type] || '📌';
  }

  getNodeLabel(node) {
    if (node.name) return node.name;
    if (node.type === 'step' && node.run) {
      const cmd = node.run.substring(0, 40);
      return cmd + (node.run.length > 40 ? '...' : '');
    }
    if (node.type === 'need' && node.job) return node.job;
    return node.type;
  }

  getNodeMeta(node) {
    if (node.type === 'job' && node.properties?.stage) {
      return `(${node.properties.stage})`;
    }
    if (node.type === 'stages' && node.list) {
      return node.list.slice(0, 3).join(', ') + (node.list.length > 3 ? '...' : '');
    }
    if (node.type === 'artifacts' && node.children?.paths?.list) {
      return `${node.children.paths.list.length} paths`;
    }
    return '';
  }

  attachNodeEventListeners() {
    this.shadowRoot.querySelectorAll('.node-item').forEach(item => {
      // Click to select
      item.addEventListener('click', (e) => {
        if (e.target.closest('.node-action-btn')) return;
        if (e.target.closest('.node-expand')) {
          const node = this.findNodeById(item.dataset.id);
          if (node) {
            node._collapsed = !node._collapsed;
            this.renderPipeline();
          }
          return;
        }

        const node = this.findNodeById(item.dataset.id);
        if (node) this.selectNode(node);
      });

      // Delete button
      const deleteBtn = item.querySelector('.node-action-btn[title="Delete"]');
      if (deleteBtn) {
        deleteBtn.addEventListener('click', (e) => {
          e.stopPropagation();
          this.deleteNode(item.dataset.id);
        });
      }

      // Drag-drop for jobs
      if (item.dataset.type === 'job') {
        item.addEventListener('dragover', (e) => {
          e.preventDefault();
          e.stopPropagation();
          item.classList.add('drop-target-active');
        });

        item.addEventListener('dragleave', () => {
          item.classList.remove('drop-target-active');
        });

        item.addEventListener('drop', (e) => {
          e.preventDefault();
          e.stopPropagation();
          item.classList.remove('drop-target-active');

          const targetNode = this.findNodeById(item.dataset.id);
          if (targetNode && this.draggedType) {
            this.handleJobDrop(this.draggedType, targetNode);
          }
        });
      }
    });
  }

  handleJobDrop(type, job) {
    const nodeId = 'node-' + Date.now();

    switch (type) {
      case 'step':
        if (!job.children.steps) {
          job.children.steps = { id: 'steps-' + Date.now(), type: 'steps', children: [] };
        }
        const stepNode = { id: nodeId, type: 'step', run: 'echo "New step"' };
        job.children.steps.children.push(stepNode);
        this.showNotification('✓ Added step');
        this.notifyBackend('step added', { jobId: job.id, ...stepNode });
        break;

      case 'artifacts':
        if (job.children.artifacts) {
          alert('Job already has artifacts');
          return;
        }
        const artifactsNode = {
          id: nodeId,
          type: 'artifacts',
          properties: { 'expire-in': '1 day' },
          children: { paths: { id: 'paths-' + Date.now(), type: 'paths', list: ['dist/'] } }
        };
        job.children.artifacts = artifactsNode;
        this.showNotification('✓ Added artifacts');
        this.notifyBackend('artifacts added', { jobId: job.id, ...artifactsNode });
        break;

      case 'cache':
        if (job.children.cache) {
          alert('Job already has cache');
          return;
        }
        const cacheNode = {
          id: nodeId,
          type: 'cache',
          properties: { key: 'cache-key', policy: 'pull-push' },
          children: { paths: { id: 'paths-' + Date.now(), type: 'paths', list: ['node_modules/'] } }
        };
        job.children.cache = cacheNode;
        this.showNotification('✓ Added cache');
        this.notifyBackend('cache added', { jobId: job.id, ...cacheNode });
        break;

      case 'needs':
        if (!job.children.needs) {
          job.children.needs = { id: 'needs-' + Date.now(), type: 'needs', children: [] };
        }
        const jobs = this.getAllJobs().filter(j => j.id !== job.id);
        if (jobs.length > 0) {
          const needNode = { id: nodeId, type: 'need', job: jobs[0].name };
          job.children.needs.children.push(needNode);
          this.showNotification('✓ Added dependency');
          this.notifyBackend('dependency added', { jobId: job.id, ...needNode });
        } else {
          alert('No other jobs to depend on');
          return;
        }
        break;

      case 'environment':
        if (job.children.environment) {
          alert('Job already has environment');
          return;
        }
        const envNode = { id: nodeId, type: 'environment', name: 'production', url: '' };
        job.children.environment = envNode;
        this.showNotification('✓ Added environment');
        this.notifyBackend('environment added', { jobId: job.id, ...envNode });
        break;

      case 'service':
        if (!job.children.services) {
          job.children.services = { id: 'services-' + Date.now(), type: 'services', children: [] };
        }
        const serviceNode = { id: nodeId, type: 'service', name: 'postgres:13' };
        job.children.services.children.push(serviceNode);
        this.showNotification('✓ Added service');
        this.notifyBackend('service added', { jobId: job.id, ...serviceNode });
        break;

      case 'rules':
        if (!job.children.rules) {
          job.children.rules = { id: 'rules-' + Date.now(), type: 'rules', children: [] };
        }
        const ruleNode = { id: nodeId, type: 'rule', if: '$CI_COMMIT_BRANCH == "main"' };
        job.children.rules.children.push(ruleNode);
        this.showNotification('✓ Added rule');
        this.notifyBackend('rule added', { jobId: job.id, ...ruleNode });
        break;

      case 'variables':
        if (!job.children.variables) {
          job.children.variables = { id: 'variables-' + Date.now(), type: 'variables', children: [] };
        }
        const varNode = { id: nodeId, type: 'variable', name: 'VAR', value: 'value' };
        job.children.variables.children.push(varNode);
        this.showNotification('✓ Added variable');
        this.notifyBackend('variable added', { jobId: job.id, ...varNode });
        break;

      default:
        return;
    }

    this.renderPipeline();
    this.updateStatus();
  }

  selectNode(node) {
    this.selectedNode = node;

    this.shadowRoot.querySelectorAll('.node-item').forEach(el => el.classList.remove('selected'));
    const nodeEl = this.shadowRoot.querySelector(`[data-id="${node.id}"]`);
    if (nodeEl) nodeEl.classList.add('selected');

    this.renderProperties(node);
  }

  renderProperties(node) {
    this.shadowRoot.getElementById('properties-empty').classList.add('hidden');
    const content = this.shadowRoot.getElementById('properties-content');
    content.classList.remove('hidden');

    let html = `
      <div class="properties-header">
        <div class="properties-title">${this.getNodeIcon(node.type)} ${node.type}</div>
      </div>
    `;

    const renderer = this.formRenderers[node.type];
    if (renderer) {
      // Use .call(this, node) to ensure the correct `this` context inside the renderer function
      html += renderer.call(this, node);
    } else {
      html += `<p>Properties for ${node.type}</p>`;
    }

    content.innerHTML = html;
    this.attachFormListeners(node);
  }

  renderJobForm(j) {
    const stages = this.pipeline.children.stages?.list || ['build', 'test', 'deploy'];
    return `
      <div class="form-group">
        <label class="form-label required">Name</label>
        <input type="text" class="form-input" id="f-name" value="${j.name || ''}" />
      </div>
      <div class="form-group">
        <label class="form-label">Stage</label>
        <select class="form-select" id="f-stage">
          ${stages.map(s => `<option ${j.properties.stage === s ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Image</label>
        <input type="text" class="form-input" id="f-image" value="${j.properties.image || ''}" placeholder="node:18" />
      </div>
      <div class="form-group">
        <label class="form-label">When</label>
        <select class="form-select" id="f-when">
          ${['on_success', 'on_failure', 'always', 'manual'].map(w =>
      `<option ${j.properties.when === w ? 'selected' : ''}>${w}</option>`).join('')}
        </select>
      </div>
      <div class="form-checkbox">
        <input type="checkbox" id="f-allow-failure" ${j.properties['allow-failure'] ? 'checked' : ''} />
        <label>Allow Failure</label>
      </div>
      <div class="form-checkbox">
        <input type="checkbox" id="f-interruptible" ${j.properties.interruptible ? 'checked' : ''} />
        <label>Interruptible</label>
      </div>
    `;
  }

  renderStepForm(s) {
    return `
      <div class="form-group">
        <label class="form-label required">Script</label>
        <textarea class="form-textarea" id="f-run">${s.run || ''}</textarea>
      </div>
    `;
  }

  renderStagesForm(s) {
    return `
      <div class="form-group">
        <label class="form-label required">Stages</label>
        <div id="stages-list">
          ${(s.list || []).map((st, i) => `
            <div class="array-item">
              <input type="text" class="form-input" value="${st}" data-idx="${i}" />
              <div class="array-item-remove" onclick="this.getRootNode().host.removeStage(${i})">×</div>
            </div>
          `).join('')}
        </div>
        <button class="btn-add mt-2" onclick="this.getRootNode().host.addStage()">+ Add Stage</button>
      </div>
    `;
  }

  renderArtifactsForm(a) {
    const paths = a.children.paths?.list || [];
    return `
      <div class="form-group">
        <label class="form-label required">Paths</label>
        <div id="paths-list">
          ${paths.map((p, i) => `
            <div class="array-item">
              <input type="text" class="form-input" value="${p}" data-idx="${i}" />
              <div class="array-item-remove" onclick="this.getRootNode().host.removePath(${i})">×</div>
            </div>
          `).join('')}
        </div>
        <button class="btn-add mt-2" onclick="this.getRootNode().host.addPath()">+ Add Path</button>
      </div>
      <div class="form-group">
        <label class="form-label">Expire In</label>
        <input type="text" class="form-input" id="f-expire" value="${a.properties['expire-in'] || '1 day'}" />
      </div>
    `;
  }

  renderCacheForm(c) {
    const paths = c.children.paths?.list || [];
    return `
      <div class="form-group">
        <label class="form-label required">Key</label>
        <input type="text" class="form-input" id="f-key" value="${c.properties.key || ''}" />
      </div>
      <div class="form-group">
        <label class="form-label required">Paths</label>
        <div id="cache-paths">
          ${paths.map((p, i) => `
            <div class="array-item">
              <input type="text" class="form-input" value="${p}" data-idx="${i}" />
              <div class="array-item-remove" onclick="this.getRootNode().host.removeCachePath(${i})">×</div>
            </div>
          `).join('')}
        </div>
        <button class="btn-add mt-2" onclick="this.getRootNode().host.addCachePath()">+ Add Path</button>
      </div>
      <div class="form-group">
        <label class="form-label">Policy</label>
        <select class="form-select" id="f-policy">
          ${['pull-push', 'pull', 'push'].map(p =>
      `<option ${c.properties.policy === p ? 'selected' : ''}>${p}</option>`).join('')}
        </select>
      </div>
    `;
  }

  renderEnvironmentForm(e) {
    return `
      <div class="form-group">
        <label class="form-label required">Name</label>
        <input type="text" class="form-input" id="f-env-name" value="${e.name || ''}" />
      </div>
      <div class="form-group">
        <label class="form-label">URL</label>
        <input type="text" class="form-input" id="f-env-url" value="${e.url || ''}" placeholder="https://example.com" />
      </div>
    `;
  }

  renderVariableForm(v) {
    return `
      <div class="form-group">
        <label class="form-label required">Key</label>
        <input type="text" class="form-input" id="f-var-key" value="${v.name || ''}" />
      </div>
      <div class="form-group">
        <label class="form-label required">Value</label>
        <input type="text" class="form-input" id="f-var-val" value="${v.value || ''}" />
      </div>
    `;
  }

  renderNeedForm(n) {
    const jobs = this.getAllJobs().map(j => j.name);
    return `
      <div class="form-group">
        <label class="form-label required">Job</label>
        <select class="form-select" id="f-need-job">
          ${jobs.map(j => `<option ${n.job === j ? 'selected' : ''}>${j}</option>`).join('')}
        </select>
      </div>
    `;
  }

  renderServiceForm(s) {
    return `
      <div class="form-group">
        <label class="form-label required">Name</label>
        <input type="text" class="form-input" id="f-svc-name" value="${s.name || ''}" placeholder="postgres:13" />
      </div>
      <div class="form-group">
        <label class="form-label">Alias</label>
        <input type="text" class="form-input" id="f-svc-alias" value="${s.alias || ''}" placeholder="db" />
      </div>
    `;
  }

  renderRuleForm(r) {
    return `
      <div class="form-group">
        <label class="form-label">If Condition</label>
        <textarea class="form-textarea" id="f-rule-if">${r.if || ''}</textarea>
      </div>
    `;
  }

  attachFormListeners(node) {
    this.shadowRoot.querySelectorAll('#properties-content input, #properties-content select, #properties-content textarea').forEach(el => {
      el.addEventListener('change', () => this.updateProperty(node, el));
    });

    this.shadowRoot.querySelectorAll('[data-idx]').forEach(el => {
      el.addEventListener('change', () => this.updateArrayItem(node, el));
    });
  }

  updateProperty(node, el) {
    const val = el.type === 'checkbox' ? el.checked : el.value;
    const id = el.id;

    if (id === 'f-name') node.name = val;
    else if (id === 'f-stage') node.properties.stage = val;
    else if (id === 'f-image') node.properties.image = val;
    else if (id === 'f-when') node.properties.when = val;
    else if (id === 'f-allow-failure') node.properties['allow-failure'] = val;
    else if (id === 'f-interruptible') node.properties.interruptible = val;
    else if (id === 'f-run') node.run = val;
    else if (id === 'f-expire') node.properties['expire-in'] = val;
    else if (id === 'f-key') node.properties.key = val;
    else if (id === 'f-policy') node.properties.policy = val;
    else if (id === 'f-env-name') node.name = val;
    else if (id === 'f-env-url') node.url = val;
    else if (id === 'f-var-key') node.name = val;
    else if (id === 'f-var-val') node.value = val;
    else if (id === 'f-need-job') node.job = val;
    else if (id === 'f-svc-name') node.name = val;
    else if (id === 'f-svc-alias') node.alias = val;
    else if (id === 'f-rule-if') node.if = val;

    this.renderPipeline();
    this.updateStatus();
    this.notifyBackend('property updated', { id: node.id, property: id, value: val });
  }

  updateArrayItem(node, el) {
    const idx = parseInt(el.dataset.idx);
    if (node.type === 'stages' && node.list) {
      node.list[idx] = el.value;
    } else if (node.children?.paths?.list) {
      node.children.paths.list[idx] = el.value;
    }
    this.renderPipeline();
    this.updateStatus();
    this.notifyBackend('property updated', { id: node.id, listIndex: idx, value: el.value });
  }

  addStage() {
    if (this.selectedNode?.type === 'stages') {
      if (!this.selectedNode.list) this.selectedNode.list = [];
      this.selectedNode.list.push('new-stage');
      this.renderProperties(this.selectedNode);
      this.renderPipeline();
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'stage added' });
    }
  }

  removeStage(idx) {
    if (this.selectedNode?.type === 'stages' && this.selectedNode.list) {
      this.selectedNode.list.splice(idx, 1);
      this.renderProperties(this.selectedNode);
      this.renderPipeline();
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'stage removed' });
    }
  }

  addPath() {
    if (this.selectedNode?.type === 'artifacts') {
      if (!this.selectedNode.children.paths) {
        this.selectedNode.children.paths = { id: 'paths-' + Date.now(), type: 'paths', list: [] };
      }
      this.selectedNode.children.paths.list.push('path/');
      this.renderProperties(this.selectedNode);
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'path added' });
    }
  }

  removePath(idx) {
    if (this.selectedNode?.type === 'artifacts' && this.selectedNode.children.paths) {
      this.selectedNode.children.paths.list.splice(idx, 1);
      this.renderProperties(this.selectedNode);
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'path removed' });
    }
  }

  addCachePath() {
    if (this.selectedNode?.type === 'cache') {
      if (!this.selectedNode.children.paths) {
        this.selectedNode.children.paths = { id: 'paths-' + Date.now(), type: 'paths', list: [] };
      }
      this.selectedNode.children.paths.list.push('path/');
      this.renderProperties(this.selectedNode);
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'cache path added' });
    }
  }

  removeCachePath(idx) {
    if (this.selectedNode?.type === 'cache' && this.selectedNode.children.paths) {
      this.selectedNode.children.paths.list.splice(idx, 1);
      this.renderProperties(this.selectedNode);
      this.updateStatus();
      this.notifyBackend('property updated', { id: this.selectedNode.id, action: 'cache path removed' });
    }
  }

  findNodeById(id, node = this.pipeline) {
    if (node.id === id) return node;
    if (!node.children) return null;

    for (const key in node.children) {
      const child = node.children[key];
      if (Array.isArray(child)) {
        for (const c of child) {
          const found = this.findNodeById(id, c);
          if (found) return found;
        }
      } else if (child?.children) {
        if (child.id === id) return child;
        if (Array.isArray(child.children)) {
          for (const c of child.children) {
            const found = this.findNodeById(id, c);
            if (found) return found;
          }
        } else {
          const found = this.findNodeById(id, child);
          if (found) return found;
        }
      } else if (child && typeof child === 'object' && child.id === id) {
        return child;
      }
    }
    return null;
  }

  deleteNode(id) {
    if (!confirm('Delete this node?')) return;

    const remove = (node) => {
      if (!node.children) return false;
      for (const key in node.children) {
        const child = node.children[key];
        if (Array.isArray(child)) {
          const idx = child.findIndex(c => c.id === id);
          if (idx !== -1) {
            const deletedNode = child[idx];
            child.splice(idx, 1);
            this.notifyBackend('node deleted', { id: deletedNode.id, type: deletedNode.type });
            return true;
          }
          for (const c of child) {
            if (remove(c)) return true;
          }
        } else if (child?.id === id) {
          const deletedNode = child;
          delete node.children[key];
          this.notifyBackend('node deleted', { id: deletedNode.id, type: deletedNode.type });
          return true;
        } else if (child?.children) {
          if (Array.isArray(child.children)) {
            const idx = child.children.findIndex(c => c.id === id);
            if (idx !== -1) {
              child.children.splice(idx, 1);
              return true;
            }
            for (const c of child.children) {
              if (remove(c)) return true;
            }
          } else if (remove(child)) {
            return true;
          }
        }
      }
      return false;
    }

    remove(this.pipeline);
    this.selectedNode = null;
    this.shadowRoot.getElementById('properties-empty').classList.remove('hidden');
    this.shadowRoot.getElementById('properties-content').classList.add('hidden');
    this.renderPipeline();
    this.updateStatus();
  }

  getAllJobs() {
    return this.pipeline.children.jobs?.children || [];
  }

  updateStatus() {
    const jobs = this.getAllJobs();
    const stages = this.pipeline.children.stages?.list?.length || 0;
    this.shadowRoot.getElementById('status-jobs').textContent = `${jobs.length} job${jobs.length !== 1 ? 's' : ''}`;
    this.shadowRoot.getElementById('status-stages').textContent = `${stages} stage${stages !== 1 ? 's' : ''}`;
  }

  showNotification(msg) {
    const st = this.shadowRoot.getElementById('status-text');
    const orig = st.textContent;
    st.textContent = msg;
    setTimeout(() => st.textContent = orig, 2500);
  }

  validatePipeline() {
    const jobs = this.getAllJobs();
    if (jobs.length === 0) {
      alert('❌ Validation Failed\n\nPipeline must have at least one job.');
      return;
    }

    const errors = [];
    jobs.forEach(j => {
      if (!j.children.steps || j.children.steps.children.length === 0) {
        errors.push(`Job "${j.name}" has no steps`);
      }
    });

    if (errors.length > 0) {
      alert('❌ Validation Failed\n\n' + errors.join('\n'));
    } else {
      alert(`✓ Pipeline is valid!\n\n• ${jobs.length} jobs\n• ${this.pipeline.children.stages?.list?.length || 0} stages`);
    }
  }

  showPipelineModel() {
    console.log('Pipeline Model:', this.pipeline);
    alert('Pipeline model logged to console (F12).\n\nThis is the internal data structure ready for YAML generation.');
  }

  notifyBackend(action, payload) {
    request('/pipeline-event', {
      method: 'POST',
      body: {
        action: action,
        ...payload
      }
    }).catch(error => {
      console.error('Error notifying backend:', error)
      alert('Error notifying backend')
    });
  }
}

customElements.define('gitlab-pipeline-builder', GitlabPipelineBuilder);