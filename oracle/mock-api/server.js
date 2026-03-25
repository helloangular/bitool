const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3002;

// Load CSV — check local copy first (Docker), then parent dir (dev)
const csvPath = fs.existsSync(path.join(__dirname, "data.csv"))
  ? path.join(__dirname, "data.csv")
  : path.join(__dirname, "..", "OracleEbs-AP_INVOICES_ALL.csv");
const rawCsv = fs.readFileSync(csvPath, "utf-8");
const lines = rawCsv.trim().split("\n");
const headers = lines[0].split(",");

const records = lines.slice(1).map((line) => {
  // Simple CSV parse (handles commas inside values for this dataset)
  const values = line.split(",");
  const obj = {};
  headers.forEach((h, i) => {
    const v = (values[i] || "").trim();
    obj[h.trim()] = v === "" ? null : v;
  });
  return obj;
});

console.log(`Loaded ${records.length} AP invoices, ${headers.length} columns`);

// OpenAPI spec
app.get("/openapi.json", (_req, res) => {
  res.sendFile(path.join(__dirname, "openapi.json"));
});

// Health check
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "oracle-ebs-mock-api", records: records.length });
});

// GET /ap/invoices — list with pagination, filtering, watermark
app.get("/ap/invoices", (req, res) => {
  let filtered = [...records];

  // Watermark filter: updatedAfter
  const updatedAfter = req.query.updatedAfter || req.query.LAST_UPDATE_DATE;
  if (updatedAfter) {
    const threshold = new Date(updatedAfter);
    filtered = filtered.filter((r) => {
      const d = r.LAST_UPDATE_DATE ? new Date(r.LAST_UPDATE_DATE) : null;
      return d && d > threshold;
    });
  }

  // Filter by vendor
  if (req.query.vendor_id) {
    filtered = filtered.filter((r) => r.VENDOR_ID === req.query.vendor_id);
  }

  // Filter by invoice type
  if (req.query.invoice_type) {
    filtered = filtered.filter((r) => r.INVOICE_TYPE_LOOKUP_CODE === req.query.invoice_type);
  }

  // Filter by org
  if (req.query.org_id) {
    filtered = filtered.filter((r) => r.ORG_ID === req.query.org_id);
  }

  // Pagination
  const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
  const offset = parseInt(req.query.offset || "0", 10);
  const page = filtered.slice(offset, offset + limit);
  const hasNextPage = offset + limit < filtered.length;

  res.json({
    data: page,
    pagination: {
      total: filtered.length,
      limit,
      offset,
      hasNextPage,
      nextOffset: hasNextPage ? offset + limit : null,
    },
  });
});

// GET /ap/invoices/:id — single invoice
app.get("/ap/invoices/:id", (req, res) => {
  const invoice = records.find((r) => r.INVOICE_ID === req.params.id);
  if (!invoice) return res.status(404).json({ error: "Invoice not found" });
  res.json({ data: invoice });
});

// GET /ap/invoices/summary — aggregates
app.get("/ap/invoices/summary", (_req, res) => {
  const total = records.length;
  const totalAmount = records.reduce((sum, r) => sum + parseFloat(r.INVOICE_AMOUNT || 0), 0);
  const currencies = [...new Set(records.map((r) => r.INVOICE_CURRENCY_CODE).filter(Boolean))];
  const types = [...new Set(records.map((r) => r.INVOICE_TYPE_LOOKUP_CODE).filter(Boolean))];
  const vendors = [...new Set(records.map((r) => r.VENDOR_ID).filter(Boolean))].length;

  res.json({
    data: {
      total_invoices: total,
      total_amount: Math.round(totalAmount * 100) / 100,
      unique_vendors: vendors,
      currencies,
      invoice_types: types,
    },
  });
});

// GET /ap/schema — return column metadata
app.get("/ap/schema", (_req, res) => {
  const schema = headers.map((h) => {
    const sample = records.find((r) => r[h] != null)?.[h] || "";
    let type = "STRING";
    if (/^\d+$/.test(sample)) type = "INT";
    else if (/^\d+\.\d+$/.test(sample)) type = "DOUBLE";
    else if (/\d{4}-\d{2}-\d{2}/.test(sample)) type = "TIMESTAMP";
    else if (sample === "true" || sample === "false") type = "BOOLEAN";
    return { column_name: h.trim(), data_type: type, sample_value: sample.substring(0, 50) };
  });
  res.json({ data: schema, total_columns: schema.length });
});

app.listen(PORT, () => {
  console.log(`Oracle EBS Mock API running at http://localhost:${PORT}`);
  console.log(`  GET /health`);
  console.log(`  GET /ap/invoices?limit=50&offset=0`);
  console.log(`  GET /ap/invoices/:id`);
  console.log(`  GET /ap/invoices/summary`);
  console.log(`  GET /ap/schema`);
});
