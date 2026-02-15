const fs = require("fs");

class DBManager {
  constructor(filePath = "database.json") {
    this.filePath = filePath;
    this.data = {};

    this.load();
  }

  // Load JSON from file
  load() {
    if (fs.existsSync(this.filePath)) {
      const raw = fs.readFileSync(this.filePath, "utf-8");
      try {
        this.data = JSON.parse(raw);
      } catch (e) {
        console.error("Invalid JSON, starting fresh DB.");
        this.data = {};
      }
    } else {
      this.data = {};
      this.save();
    }
  }

  // Save JSON to file
  save() {
    fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
  }

  // Create a table if not exists
  createTable(tableName) {
    if (!this.data[tableName]) {
      this.data[tableName] = [];
      this.save();
    }
  }

  // Insert row
  insert(tableName, row) {
    if (!this.data[tableName]) this.createTable(tableName);

    // const id = Date.now(); // auto id
    // const newRow = { id, ...row };
    this.data[tableName].push(row);
    this.save();
    return row;
  }

  // Get all rows
  select(tableName, filterFn = null) {
    if (!this.data[tableName]) return [];
    return filterFn ? this.data[tableName].filter(filterFn) : this.data[tableName];
  }

  // Update row by id
  update(tableName, id, updates) {
    if (!this.data[tableName]) return null;
    let row = this.data[tableName].find(r => r.id === id);
    if (row) {
      Object.assign(row, updates);
      this.save();
      return row;
    }
    return null;
  }

  // Delete row by id
  delete(tableName, id) {
    if (!this.data[tableName]) return false;
    const initialLength = this.data[tableName].length;
    this.data[tableName] = this.data[tableName].filter(r => r.id !== id);
    this.save();
    return this.data[tableName].length < initialLength;
  }

  // Drop a table
  dropTable(tableName) {
    delete this.data[tableName];
    this.save();
  }

  // Show all tables
  listTables() {
    return Object.keys(this.data);
  }

  // Row count
  count(tableName) {
    return this.data[tableName].length;
  }
}

module.exports = {DBManager, };
