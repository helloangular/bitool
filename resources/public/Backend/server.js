const express = require("express");
const cors = require("cors");
const path = require("path");
const app = express();
const port = 3000;
const url = require("url");
const { DBManager } = require('./db_manager.js');
const db = new DBManager();

app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, "..")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "index.html"));
});

const BTYPES = {
  "function": "Fu",
  "aggregation": "A",
  "sorter": "S",
  "union": "U",
  "mapping": "Mp",
  "filter": "Fi",
  "table": "T",
  "join": "J",
  "union": "U",
  "projection": "P",
  "target": "Tg",
  "api-connection": "Ap",
  "conditionals": "C",
  "grid": "G"
}

app.post("/save/save-conn", (req, res) => {
  const {
    connection_name,
    username,
    password,
    dbname,
    dbtype,
    host,
    port,
    sid,
    service,
  } = req.body;

  const response = {
    "conn-id": 88,
    "tree-data": {
      label: "postgres",
      items: [
        {
          label: "public",
          items: [
            {
              label: "accounts",
              items: [
                { label: "user_id-integer-NO-32-0", items: null },
                { label: "username-character varying-NO-50", items: null },
                { label: "password-character varying-NO-50", items: null },
                { label: "email-character varying-NO-255", items: null },
                {
                  label: "created_at-timestamp without time zone-NO",
                  items: null,
                },
                {
                  label: "last_login-timestamp without time zone-YES",
                  items: null,
                },
              ],
            },
            {
              label: "users",
              items: [
                { label: "id-integer-NO-32-0", items: null },
                { label: "username-character varying-NO-50", items: null },
                { label: "password-character varying-NO-100", items: null },
                { label: "email-character varying-NO-100", items: null },
                {
                  label: "created_at-timestamp without time zone-YES",
                  items: null,
                },
              ],
            },
          ],
        },
      ],
    },
  };

  res.json(response);
});

app.post("/newgraph", (req, res) => {
  // const { graphname } = req.body;
  // db.insert("graph", { "alias": graphname, "x": 250, "y": 250, "id": db.count("graph")+1, "parent": 0 })
  // const response = db.select("graph", (g) => g.alias === graphname);
  res.json({ "alias": "Output", "x": 250, "y": 250, "id": 1, "parent": 0, btype: "O" });
});

app.post("/addFilter", (req, res) => {
  const { label } = req.body;

  var response = {};
  if (label == 'filter') {
    response = db.select("cp-filter");
  } else if (label == "function" || label == "aggregation" || label == "sorter" ||
    label == "union" || label == "mapping") {
    response = {
      cp: db.select("cp-account"),
      rp: db.select("panel-items", (item) => item.label === BTYPES[label])
    };
  } else {
    response = db.select("cp-other-filters");
  }

  res.json(response);
});

app.post("/saveRectJoin", (req, res) => {
  const { src, dest, panelItems } = req.body;
  for (const item of panelItems) {
    if (item.id === +dest) {
      item.parent = +src;
    }
  }

  res.status(200).json(panelItems);
});

app.post("/addSingle", (req, res) => {
  const { alias, id, parent, x, y, conn_id, panelItems } = req.body;
  try {
    const newRectId = db.count("panel_items") + 1;
    db.insert("panel_items", {
      id: newRectId,
      alias,
      parent_object: alias,
      business_name: "",
      technical_name: "",
      btype: BTYPES[alias],
      items: []
    });
    return res.status(200).json([...panelItems,
    {
      alias,
      y,
      x,
      btype: BTYPES[alias],
      parent,
      id: newRectId
    }]);
  } catch (error) {
    console.error(error);
  }

})

app.post("/moveSingle", (req, res) => {
  const { rect, panelItems } = req.body;
  for (let i = 0; i < panelItems.length - 1; i++) {
    if (rect.id === panelItems[i].id) {
      panelItems[i] = rect;
      break;
    }
  }

  return res.status(200).json(panelItems);
})

app.post("/addtable", (req, res) => {
  const { table1, table2, conn_id, schema, panelItems } = req.body;

  const table2Item = panelItems.find((item) => item.id == table2);

  if (!table2Item) {
    return res.sendStatus(400);
  }

  if (table2 == 1) {
    const op = db.select("cp-output");
    res.json(op);
  }
  else {
    // res.json([
    //   {"alias":"Output","y":350,"x":550,"btype":"output","parent":0,"id":1},
    //   {"alias":"sqldi","y":400,"x":250,"btype":"table","parent":4,"id":2},
    //   {"alias":"graph","y":300,"x":250,"btype":"table","parent":4,"id":3},
    //   {"alias":"join-graph","y":350,"x":350,"btype":"join","parent":5,"id":4},
    //   {"alias":"projection-graph","y":350,"x":450,"btype":"projection","parent":1,"id":5}
    // ]);
    const response = db.select("cp-account");
    res.json(response);
  };

});

app.post("/saveColumn", (req, res) => {
  const { id } = req.body;
  const response = db.select("panel_items", (item) => item.id === id);
  res.json(response[0]);
});

app.post("/saveAggregation", (req, res) => {
  const { id } = req.body;
  const response = db.select("panel_items", (item) => item.id === id);
  res.json(response[0]);
});

app.post("/saveFunction", (req, res) => {
  const { id } = req.body;
  const response = db.select("panel_items", (item) => item.id === id);
  res.json(response[0]);
});

app.post("/saveSchedule", (req, res) => {
  res.status(200).json(req.body);
})

app.get("/getTree", (req, res) => {
  res.status(200).json(db.select('tree-demo'));
})

app.get("/getItem", (req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const id = parsedUrl.query.id;
  const response = db.select("panel_items", (item) => item.id === Number(id));
  if (response.length > 0) res.status(200).json(response[0]);
  else res.status(404).json({ message: "Item not available." });
});


app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
