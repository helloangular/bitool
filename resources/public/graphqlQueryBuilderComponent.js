import { request } from "./library/utils.js";

const template = document.createElement("template");
template.innerHTML = `
    <link rel="stylesheet" type="text/css" href="source/styles/smart.default.css" />
    <style>
        :host {
            display: flex;
            flex-direction: column;
            height: 100%;
            width: 100%;
        }

        #schemaBar {
            padding: 0.5rem 1rem;
            border-bottom: 1px solid #eee;
            display: flex;
            align-items: center;
            gap: 1rem;
            flex-wrap: wrap;
            background: #fafafa;
            font-size: 0.85rem;
        }

        #schemaBar .label {
            font-weight: 600;
            font-size: 0.85rem;
        }

        #schemaUrlInput {
            min-width: 260px;
        }

        #schemaFileInput {
            font-size: 0.8rem;
        }

        #app {
            flex: 1;
            display: grid;
            grid-template-columns: 320px 340px 1.2fr;
            gap: 0.75rem;
            padding: 0.75rem;
            box-sizing: border-box;
            min-height: 0;
        }

        .panel {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 0.75rem;
            display: flex;
            flex-direction: column;
            min-height: 0;
            background: #fff;
        }

        .panel-title {
            font-weight: 600;
            margin-bottom: 0.5rem;
            font-size: 0.95rem;
        }

        .section-label {
            font-size: 0.8rem;
            font-weight: 600;
            margin: 0.4rem 0 0.2rem;
        }

        .small {
            font-size: 0.8rem;
            color: #666;
        }

        .row {
            display: flex;
            gap: 0.5rem;
            align-items: center;
            margin: 0.25rem 0;
            flex-wrap: wrap;
        }

        textarea {
            width: 100%;
            resize: vertical;
            min-height: 80px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            font-size: 0.8rem;
            padding: 0.4rem;
            box-sizing: border-box;
        }

        #argForm {
            flex: 1;
            overflow: auto;
            padding-right: 0.25rem;
        }

        #fieldTree {
            flex: 1;
            width: inherit;
        }

        #selectedFields {
            max-height: 120px;
            overflow: auto;
            padding: 0.25rem 0;
            border-top: 1px dashed #ddd;
            margin-top: 0.25rem;
        }

        #selectedFields ul {
            list-style: none;
            padding-left: 1rem;
            margin: 0;
        }

        #selectedFields li {
            font-size: 0.8rem;
        }

        smart-button {
            margin-top: 0.5rem;
            align-self: flex-start;
        }

        .button-row-inline {
            justify-content: flex-start;
            margin-top: 0.5rem;
        }

        #outputTabs {
            flex: 1;
            min-height: 0;
            display: flex;
            flex-direction: column;
        }

        #outputTabs smart-tabs {
            flex: 1;
            min-height: 0;
        }

        smart-spinner {
            margin-left: 0.5rem;
        }

        smart-tab-item {
            padding: 0.5rem;
            box-sizing: border-box;
        }

        .hidden {
            display: none !important;
        }
    </style>

    <!-- Schema source bar -->
    <div id="schemaBar">
        <span class="label">Schema source:</span>
        <smart-drop-down-list id="schemaSourceType" selection-mode="one" placeholder="Select schema source"></smart-drop-down-list>
        <div class="row" id="schemaUrlRow">
            <span class="small">Schema URL:</span>
            <smart-text-box id="schemaUrlInput" placeholder="https://example.com/schema.graphql"></smart-text-box>
            <smart-button id="loadSchemaButton">Load Schema</smart-button>
            <smart-spinner id="schemaLoadingSpinner" class="hidden"></smart-spinner>
        </div>
        <div class="row hidden" id="schemaFileRow">
            <span class="small">Schema file:</span>
            <input type="file" id="schemaFileInput" accept=".graphql,.gql,.json,.schema,.txt" />
            <span class="small">(Upload introspection JSON or SDL)</span>
        </div>
    </div>

    <div id="app">
        <!-- LEFT PANEL: Query selection + arguments -->
        <div class="panel">
            <div class="panel-title">1. Choose root query</div>
            <label class="section-label" for="querySelect">Query</label>
            <smart-drop-down-list id="querySelect" selection-mode="one" placeholder="Select query..."></smart-drop-down-list>
            <div class="row small">
                <span>Return type:</span>
                <span id="returnTypeLabel"><em>—</em></span>
            </div>
            <hr />
            <div class="panel-title" style="margin-top: 0.25rem;">2. Configure arguments</div>
            <div id="argForm" class="small"><p>No query selected.</p></div>
            <div class="row button-row-inline">
                <smart-button id="generateButton" class="primary">Generate query</smart-button>
                <smart-button id="saveQueryButton">Save query</smart-button>
            </div>
        </div>

        <!-- MIDDLE PANEL: Response type tree + selected fields -->
        <div class="panel">
            <div class="panel-title">3. Select response fields</div>
            <span class="small">Expand and tick the fields you want in the result.</span>
            <smart-tree id="fieldTree" selection-mode="checkBox"></smart-tree>
            <div id="selectedFields">
                <div class="section-label">Selected field paths</div>
                <ul id="selectedList"></ul>
            </div>
        </div>

        <!-- RIGHT PANEL: Generated output -->
        <div class="panel">
            <div class="panel-title">4. Generated output</div>
            <div id="outputTabs">
                <smart-tabs id="tabs" selected-index="0">
                    <smart-tab-item label="Query"><textarea id="queryText" readonly></textarea></smart-tab-item>
                    <smart-tab-item label="Variables"><textarea id="variablesText" readonly></textarea></smart-tab-item>
                    <smart-tab-item label="Response shape"><textarea id="responseShapeText" readonly></textarea></smart-tab-item>
                </smart-tabs>
            </div>
        </div>
    </div>
`;

class GraphQLQueryBuilder extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this.shadowRoot.appendChild(template.content.cloneNode(true));

    // Initialize with an empty schema. It will be loaded dynamically.
    this.schemaMeta = { queries: [] };
  }

  connectedCallback() {
    // Element references
    const S = this.shadowRoot;
    this.querySelect = S.getElementById("querySelect");
    this.returnTypeLabel = S.getElementById("returnTypeLabel");
    this.argForm = S.getElementById("argForm");
    this.fieldTree = S.getElementById("fieldTree");
    this.selectedList = S.getElementById("selectedList");
    this.generateButton = S.getElementById("generateButton");
    this.saveQueryButton = S.getElementById("saveQueryButton");
    this.queryText = S.getElementById("queryText");
    this.variablesText = S.getElementById("variablesText");
    this.responseShapeText = S.getElementById("responseShapeText");
    this.schemaSourceType = S.getElementById("schemaSourceType");
    this.schemaUrlRow = S.getElementById("schemaUrlRow");
    this.schemaFileRow = S.getElementById("schemaFileRow");
    this.schemaUrlInput = S.getElementById("schemaUrlInput");
    this.schemaFileInput = S.getElementById("schemaFileInput");
    this.loadSchemaButton = S.getElementById("loadSchemaButton");
    this.schemaLoadingSpinner = S.getElementById("schemaLoadingSpinner");

    this.init();
  }

  init() {
    // Schema source toggle
    this.schemaSourceType.dataSource = [
      { label: "Schema URL", value: "url" },
      { label: "Upload file", value: "file" },
    ];
    this.schemaSourceType.selectedIndex = 0;
    this.updateSchemaSourceRows();
    this.schemaSourceType.addEventListener("change", () => {
      this.updateSchemaSourceRows();
      this.clearSchemaAndUI();
    });

    // Event Listeners
    this.querySelect.addEventListener("change", () => this.handleQuerySelect());
    this.fieldTree.addEventListener("change", () =>
      this.updateSelectedFieldList(),
    );
    this.generateButton.addEventListener("click", () => this.handleGenerate());
    this.saveQueryButton.addEventListener("click", () => this.handleSave());
    this.loadSchemaButton.addEventListener("click", () => {
      const url = this.schemaUrlInput.value;
      if (url) {
        this.fetchAndProcessSchema(url);
      } else {
        alert("Please enter a GraphQL schema URL.");
      }
    });
    this.schemaFileInput.addEventListener("change", (event) =>
      this.handleFileUpload(event),
    );
  }

  clearSchemaAndUI() {
    this.schemaMeta = { queries: [] };
    this.querySelect.dataSource = [];
    this.clearOutputs();
    this.argForm.innerHTML = "<p>No query selected.</p>";
    this.fieldTree.dataSource = [];
    this.returnTypeLabel.textContent = "—";
  }

  handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const introspectionResult = JSON.parse(e.target.result);
        // The file could be a full introspection result { "data": { "__schema": ... } } or just the __schema object
        const schema = introspectionResult.data
          ? introspectionResult.data.__schema
          : introspectionResult.__schema || introspectionResult;
        this.schemaMeta = this.processIntrospectionResult(schema);
        this.populateUIFromSchema();
      } catch (error) {
        console.error("Failed to parse schema file:", error);
        alert(
          "Failed to parse schema file. Please ensure it is a valid introspection JSON file.",
        );
      }
    };
    reader.readAsText(file);
  }

  // All helper functions from the original script go here as methods
  getQueryMeta(name) {
    return this.schemaMeta.queries.find((q) => q.name === name) || null;
  }
  safeParseJSON(text) {
    try {
      return JSON.parse(text);
    } catch (e) {
      return null;
    }
  }
  updateSchemaSourceRows() {
    const value = this.schemaSourceType.selectedValues[0];
    this.schemaUrlRow.classList.toggle("hidden", value !== "url");
    this.schemaFileRow.classList.toggle("hidden", value === "url");
  }
  handleQuerySelect() {
    const selected = this.querySelect.selectedValues?.[0];
    if (!selected) return;
    const meta = this.getQueryMeta(selected);
    if (!meta) return;
    this.returnTypeLabel.textContent = meta.returnType;
    this.renderArgForm(meta);
    this.fieldTree.dataSource = meta.fieldTree;
    this.fieldTree.expandAll();
    this.updateSelectedFieldList();
    this.clearOutputs();
  }
  updateSelectedFieldList() {
    // Ensure fieldTree.getSelectedValues is a function before calling it
    if (typeof this.fieldTree.getSelectedValues !== "function") {
      console.warn(
        "fieldTree.getSelectedValues is not a function. Smart-Tree component might not be initialized or configured correctly.",
      );
      this.selectedList.innerHTML = "";
      return;
    }

    const paths = this.getCheckedFieldPaths();
    this.selectedList.innerHTML = "";
    paths.forEach((p) => {
      const li = document.createElement("li");
      li.textContent = p;
      this.selectedList.appendChild(li);
    });
  }
  handleGenerate() {
    const selected = this.querySelect.selectedValues?.[0];
    if (!selected) {
      alert("Please select a root query first.");
      return;
    }
    const meta = this.getQueryMeta(selected);
    if (!meta) return;

    const args = this.readArgForm(meta);
    const paths = this.getCheckedFieldPaths();

    this.queryText.value = this.buildGraphQLQuery(meta, args, paths);
    this.variablesText.value = JSON.stringify(
      this.buildVariablesObject(meta, args),
      null,
      2,
    );
    this.responseShapeText.value = JSON.stringify(
      this.buildResponseShape(meta, paths),
      null,
      2,
    );
  }
  handleSave() {
    const selected = this.querySelect.selectedValues?.[0];
    if (!selected) {
      alert("Please select a root query first.");
      return;
    }
    const meta = this.getQueryMeta(selected);
    if (!meta) return;

    const query = this.queryText.value;
    if (
      !query ||
      query.trim() === "" ||
      query.startsWith("# No fields selected")
    ) {
      alert("Generate the query before saving.");
      return;
    }

    const payload = {
      rootQuery: meta.name,
      query,
      variables: this.safeParseJSON(this.variablesText.value),
      createdAt: new Date().toISOString(),
    };

    // Make request to the /save-query.
    request("/save-query", { method: "POST", body: JSON.stringify(payload) })
      .then((response) => {
        if (response.ok) {
          alert("Query saved successfully!");
        } else {
          alert("Failed to save query. Please try again.");
        }
      })
      .catch((error) => {
        console.error("Error saving query:", error);
        alert("An error occurred while saving the query. Please try again.");
      });
  }
  renderArgForm(meta) {
    this.argForm.innerHTML = "";
    if (!meta.args || meta.args.length === 0) {
      this.argForm.innerHTML = "<p>No arguments for this query.</p>";
      return;
    }
    meta.args.forEach((arg) => {
      const row = document.createElement("div");
      row.className = "row";
      const label = document.createElement("label");
      label.className = "section-label";
      label.textContent = arg.name + (arg.type.endsWith("!") ? " *" : "");
      row.appendChild(label);
      let inputEl;
      if (arg.kind === "ENUM") {
        inputEl = document.createElement("smart-drop-down-list");
        inputEl.selectionMode = "one";
        inputEl.placeholder = arg.type;
        inputEl.dataSource = arg.enumValues.map((v) => ({
          label: v,
          value: v,
        }));
      } else if (arg.type.startsWith("Int")) {
        inputEl = document.createElement("smart-numeric-text-box");
        inputEl.placeholder = arg.type;
        if (arg.defaultValue != null) inputEl.value = arg.defaultValue;
      } else {
        inputEl = document.createElement("smart-text-box");
        inputEl.placeholder = arg.type;
      }
      inputEl.id = `arg-${meta.name}-${arg.name}`;
      row.appendChild(inputEl);
      this.argForm.appendChild(row);
    });
  }
  readArgForm(meta) {
    const args = {};
    meta.args.forEach((arg) => {
      const el = this.shadowRoot.querySelector(`#arg-${meta.name}-${arg.name}`);
      if (!el) return;
      let value = arg.kind === "ENUM" ? el.selectedValues?.[0] : el.value;
      if (value !== null && value !== "") args[arg.name] = value;
    });
    return args;
  }

  // Helper to get the base type name from a nested type reference
  _getBaseTypeName(typeRef) {
    let currentType = typeRef;
    while (currentType.ofType) {
      currentType = currentType.ofType;
    }
    return currentType.name;
  }

  // Helper to reconstruct the full GraphQL type string (e.g., "String!", "[Int!]!")
  _getFullTypeString(typeRef) {
    let typeString = "";
    let current = typeRef;
    let parts = [];

    if (!typeRef) {
      return "UnknownType"; // Handle cases where typeRef might be null/undefined
    }

    // Traverse up the `ofType` chain to get the base type and modifiers
    while (current) {
      parts.push({ kind: current.kind, name: current.name });
      current = current.ofType;
    }

    // Reconstruct the string from base type outwards
    parts.reverse().forEach((part, index) => {
      if (index === 0) {
        // Base type
        typeString = part.name;
      } else {
        if (part.kind === "NON_NULL") {
          typeString = `${typeString}!`;
        } else if (part.kind === "LIST") {
          typeString = `[${typeString}]`;
        }
      }
    });
    return typeString;
  }

  // Transforms raw GraphQL introspection result into the component's schemaMeta format
  processIntrospectionResult(schema) {
    const queries = [];
    const typesMap = new Map(schema.types.map((type) => [type.name, type]));

    const queryTypeName = schema.queryType.name;
    const queryType = typesMap.get(queryTypeName);

    if (!queryType || !queryType.fields) {
      console.warn("No query type or fields found in schema introspection.");
      return { queries: [] };
    }

    // Helper to build the fieldTree recursively
    const buildFieldTree = (type, currentPath = [], depth = 0) => {
      if (!type || !type.fields || depth > 5) {
        // Limit depth to prevent infinite recursion for circular refs
        return [];
      }

      const items = [];
      for (const field of type.fields) {
        const fieldPath = [...currentPath, field.name].join(".");
        const fieldNode = {
          label: field.name,
          value: fieldPath,
        };

        const fieldTypeBaseName = this._getBaseTypeName(field.type);
        const nestedType = typesMap.get(fieldTypeBaseName);

        // Only recurse for OBJECT, INTERFACE, or UNION types that have fields or possibleTypes
        if (nestedType) {
          if (
            (nestedType.kind === "OBJECT" || nestedType.kind === "INTERFACE") &&
            nestedType.fields &&
            nestedType.fields.length > 0
          ) {
            fieldNode.items = buildFieldTree(
              nestedType,
              [...currentPath, field.name],
              depth + 1,
            );
          } else if (nestedType.kind === "UNION" && nestedType.possibleTypes) {
            // Handle union types by listing possible types
            fieldNode.items = nestedType.possibleTypes.map((pt) => {
              const ptName = pt.name;
              const ptType = typesMap.get(ptName);
              return {
                label: `... on ${ptName}`,
                value: `${fieldPath}.${ptName}`,
                items: buildFieldTree(
                  ptType,
                  [...currentPath, ptName],
                  depth + 1,
                ),
              };
            });
          }
        }
        items.push(fieldNode);
      }
      return items;
    };

    for (const field of queryType.fields) {
      const args = field.args.map((arg) => ({
        name: arg.name,
        type: this._getFullTypeString(arg.type),
        kind: this._getBaseTypeName(arg.type), // Use base kind for ENUM check
        defaultValue:
          arg.defaultValue != null ? JSON.parse(arg.defaultValue) : undefined, // Parse default value if it's a string
        enumValues:
          this._getBaseTypeName(arg.type) === "ENUM"
            ? typesMap
                .get(this._getBaseTypeName(arg.type))
                .enumValues.map((ev) => ev.name)
            : undefined,
      }));

      const returnTypeBaseName = this._getBaseTypeName(field.type);
      const returnTypeMeta = typesMap.get(returnTypeBaseName);

      const queryMeta = {
        name: field.name,
        description: field.description,
        args: args,
        returnType: this._getFullTypeString(field.type),
        fieldTree: [
          {
            label: field.name,
            value: field.name,
            items: buildFieldTree(returnTypeMeta, [field.name]),
          },
        ],
      };
      queries.push(queryMeta);
    }

    return { queries };
  }

  async fetchAndProcessSchema(url) {
    this.schemaLoadingSpinner.classList.remove("hidden");
    try {
      // The full GraphQL introspection query is quite long. For brevity in the diff, it's concatenated.
      // In a real application, you'd typically store this in a separate file or a multiline string.
      const introspectionQuery =
        `query IntrospectionQuery{__schema{queryType{name}mutationType{name}subscriptionType{name}types{...FullType}}}` +
        `fragment FullType on __Type{kind name description fields(includeDeprecated:true){name description args{...InputValue}type{...TypeRef}isDeprecated deprecationReason}inputFields{...InputValue}interfaces{...TypeRef}enumValues(includeDeprecated:true){name description isDeprecated deprecationReason}possibleTypes{...TypeRef}}` +
        `fragment InputValue on __InputValue{name description type{...TypeRef}defaultValue}` +
        `fragment TypeRef on __Type{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name}}}}}}}}`;

      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ query: introspectionQuery }),
      });
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const result = await response.json();
      if (result.errors) {
        console.error("GraphQL Introspection Errors:", result.errors);
        alert("Error fetching schema: " + result.errors[0].message);
        return;
      }
      this.schemaMeta = this.processIntrospectionResult(result.data.__schema);
      this.populateUIFromSchema();
    } catch (error) {
      console.error("Failed to fetch or process schema:", error);
      alert("Failed to load schema: " + error.message);
    } finally {
      this.schemaLoadingSpinner.classList.add("hidden");
    }
  }

  populateUIFromSchema() {
    console.log("Schema loaded:", this.schemaMeta);
    this.querySelect.dataSource = this.schemaMeta.queries.map((q) => ({
      label: q.name,
      value: q.name,
    }));
    if (this.schemaMeta.queries.length > 0) {
      this.querySelect.selectedIndex = 0;
      this.handleQuerySelect();
    } else {
      this.clearSchemaAndUI();
      this.argForm.innerHTML =
        "<p>No queries found in the provided schema.</p>";
    }
  }

  getCheckedFieldPaths() {
    return this.fieldTree.getSelectedValues?.() || [];
  }
  clearOutputs() {
    this.queryText.value = "";
    this.variablesText.value = "";
    this.responseShapeText.value = "";
  }
  buildGraphQLQuery(meta, args, paths) {
    if (!paths.length) return `# No fields selected.`;
    const requiredArgs = meta.args.filter((a) => args[a.name] != null);
    const varDefs = requiredArgs.map((a) => `$${a.name}: ${a.type}`).join(", ");
    const argExpressions = meta.args
      .map((arg) =>
        args[arg.name] != null ? `${arg.name}: $${arg.name}` : null,
      )
      .filter(Boolean)
      .join(", ");
    const selection = this.buildSelectionSetFromPaths(paths, meta.name);
    return `query ${meta.name.charAt(0).toUpperCase() + meta.name.slice(1)}${varDefs ? `(${varDefs})` : ""} {\n  ${meta.name}${argExpressions ? `(${argExpressions})` : ""} {\n${selection}\n  }\n}`;
  }

  /**
   * Builds a nested object from a flat array of dot-notation paths.
   * e.g., ['a.b', 'a.c'] becomes { a: { b: {}, c: {} } }
   * @param {string[]} paths - The array of dot-notation paths.
   * @returns {object} A nested object representing the paths.
   */
  _buildTreeFromPaths(paths) {
    const tree = {};
    for (const path of paths) {
      if (!path) continue; // Skip empty paths that might result from cleaning
      let currentNode = tree;
      const parts = path.split(".");
      for (const part of parts) {
        currentNode[part] = currentNode[part] || {};
        currentNode = currentNode[part];
      }
    }
    return tree;
  }

  /**
   * Recursively renders a nested object into an indented GraphQL selection set string.
   * @param {object} treeNode - The current node of the tree to render.
   * @param {number} level - The current indentation level.
   * @returns {string} The formatted GraphQL selection set.
   */
  _renderTreeToString(treeNode, level) {
    const indent = "  ".repeat(level);
    return Object.keys(treeNode)
      .map((key) => {
        const childNode = treeNode[key];
        const hasChildren = Object.keys(childNode).length > 0;

        if (hasChildren) {
          return `${indent}${key} {\n${this._renderTreeToString(childNode, level + 1)}\n${indent}}`;
        } else {
          return `${indent}${key}`;
        }
      })
      .join("\n");
  }

  buildSelectionSetFromPaths(paths, rootName) {
    // 1. Clean paths by removing the root query name.
    const cleanedPaths = paths.map((p) =>
      p.replace(new RegExp(`^${rootName}\\.?`), ""),
    );
    // 2. Build a nested object from the cleaned paths.
    const tree = this._buildTreeFromPaths(cleanedPaths);
    // 3. Render the nested object into a formatted string, starting at indentation level 2.
    return this._renderTreeToString(tree, 2);
  }
  buildVariablesObject(meta, args) {
    const vars = {};
    meta.args.forEach((arg) => {
      if (arg.type.endsWith("!") && args[arg.name] != null)
        vars[arg.name] = args[arg.name];
    });
    return vars;
  }
  buildResponseShape(meta, paths) {
    const root = {};
    paths.forEach((p) => {
      let node = root;
      p.split(".").forEach((part, idx, arr) => {
        const isLeaf = idx === arr.length - 1;
        if (!node[part]) node[part] = isLeaf ? "<value>" : {};
        else if (!isLeaf && typeof node[part] !== "object") node[part] = {};
        node = node[part];
      });
    });
    return root;
  }
}

customElements.define("graphql-query-builder", GraphQLQueryBuilder);

export default GraphQLQueryBuilder;
