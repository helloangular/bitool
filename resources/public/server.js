const express = require("express");
const cors = require("cors");
const path = require("path");
const app = express();
const port = 3000;
const http = require("http");
const url = require("url");
const { json } = require("stream/consumers");

app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, "..")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "index.html"));
});

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
  const { graphname } = req.body;

  const response = { "alias": "Output", "x": "250", "y": "250", "id": 1, "parent": 0 };

  res.json(response);
});

const jsonData1 = {
  "cp": [
    {
      "alias": "Output",
      "y": 350,
      "x": 650,
      "btype": "output",
      "parent": 0,
      "id": 1
    },
    {
      "alias": "sqldi",
      "y": 400,
      "x": 350,
      "btype": "table",
      "parent": 4,
      "id": 2
    },
    {
      "alias": "graph",
      "y": 300,
      "x": 250,
      "btype": "table",
      "parent": 6,
      "id": 3
    },
    {
      "alias": "join-graph",
      "y": 350,
      "x": 450,
      "btype": "join",
      "parent": 5,
      "id": 4
    },
    {
      "alias": "projection-graph",
      "y": 350,
      "x": 550,
      "btype": "projection",
      "parent": 1,
      "id": 5
    },
    {
      "alias": "aggregation-graph",
      "y": 300,
      "x": 350,
      "btype": "aggregation",
      "parent": 4,
      "id": 6
    }
  ],
  "rp": {
    "id": "6",
    "alias": "aggregation-graph",
    "parent_object": "Table_view",
    "business_name": "aggregation-graph",
    "technical_name": "aggregation-graph",
    "btype": "aggregation",
    "having": "SUM(test) > 0"
    "items": [
      {
        "tid": 3,
        "business_name": "graph.id",
        "technical_name": "graph.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column",
        "af": "SUM"
      },
      {
        "tid": 3,
        "business_name": "graph.version",
        "technical_name": "graph.version",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
        "af": "None"
      },
      {
        "tid": 3,
        "business_name": "graph.name",
        "technical_name": "graph.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
        "af": "None"
      },
      {
        "tid": 3,
        "business_name": "graph.definition",
        "technical_name": "graph.definition",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
        "af": "None"
      }
    ]
  }
};

const jsonData = {
  "cp": [
    {
      "alias": "Output",
      "y": 350,
      "x": 650,
      "btype": "output",
      "parent": 0,
      "id": 1
    },
    {
      "alias": "sqldi",
      "y": 400,
      "x": 250,
      "btype": "table",
      "parent": 6,
      "id": 2
    },
    {
      "alias": "graph",
      "y": 300,
      "x": 250,
      "btype": "table",
      "parent": 7,
      "id": 3
    },
    {
      "alias": "join-graph",
      "y": 350,
      "x": 450,
      "btype": "join",
      "parent": 5,
      "id": 4
    },
    {
      "alias": "projection-graph",
      "y": 350,
      "x": 550,
      "btype": "projection",
      "parent": 1,
      "id": 5
    },
    {
      "alias": "aggregation-sqldi",
      "y": 400,
      "x": 350,
      "btype": "aggregation",
      "parent": 4,
      "id": 6
    },
    {
      "alias": "function-graph",
      "y": 300,
      "x": 350,
      "btype": "function",
      "parent": 4,
      "id": 7
    }
  ],
  "rp": {
    "id": 7,
    "alias": "function-graph",
    "parent_object": "Table_view",
    "business_name": "function-graph",
    "technical_name": "function-graph",
    "btype": "function",
    "items": [
      {
        "tid": 3,
        "business_name": "graph.id",
        "technical_name": "graph.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column",
	"excluded": "YES"
      },
      {
        "tid": 3,
        "business_name": "graph.version",
        "technical_name": "graph.version",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
	"excluded": "YES"
      },
      {
        "tid": 3,
        "business_name": "graph.name",
        "technical_name": "graph.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
	"excluded": "NO"
      },
      {
        "tid": 3,
        "business_name": "graph.definition",
        "technical_name": "graph.definition",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
	"excluded": "NO"
      },
      {
        "tid": 3,
        "business_name": "graph.tax",
        "technical_name": "graph.tax",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "calculated",
	"excluded": "NO",
	"length": 10,
        "expression" : "(Itemid * 2 ) + ( productid * 3))"
 
      }
    ]
  }
};

const unionData = {
  "id": "8",
  "alias": "union112",
  "parent_object": "Table_view",
  "business_name": "union-graph",
  "technical_name": "union-graph",
  "btype": "union",
  "jtype": "all",
  "tables": [
    "SalesOrder11",
    "SalesOrder22",
    "SalesOrder33"
  ],
  "items": {
    "SalesOrder11": [
      {
        "tid": 3,
        "business_name": "SalesOrder.id",
        "technical_name": "SalesOrder.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 3,
        "business_name": "SalesOrder.name",
        "technical_name": "SalesOrder.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ],
    "SalesOrder22": [
      {
        "tid": 3,
        "business_name": "SalesOrderItems.id",
        "technical_name": "SalesOrderItems.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 3,
        "business_name": "SalesOrderItems.name",
        "technical_name": "SalesOrderItems.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ],
    "SalesOrder33": [
      {
        "tid": 3,
        "business_name": "SalesOrderItems.id",
        "technical_name": "SalesOrderItems.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 3,
        "business_name": "SalesOrderItems.name",
        "technical_name": "SalesOrderItems.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ],
    "Final": [
      {
        "tid": 3,
        "business_name": "SalesOrderItems.id",
        "technical_name": "SalesOrderItems.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 3,
        "business_name": "SalesOrderItems.name",
        "technical_name": "SalesOrderItems.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ]
  },
  "join_items": {
    "SalesOrder11": ["SalesOrder.id"],
    "SalesOrder22": ["SalesOrderItems.id"],
    "SalesOrder33": ["SalesOrderItems.id"]
  }
};

app.post("/addFilter", (req, res) => {
  const { ftype, item } = req.body;

  var response = {};
  if (ftype == 'filter') {
    response = [{ "alias": "Output", "y": 350, "x": 750, "btype": "output", "parent": 0, "id": 1 }, { "alias": "accounts", "y": 300, "x": 250, "btype": "table", "parent": 6, "id": 2 }, { "alias": "users", "y": 400, "x": 350, "btype": "table", "parent": 4, "id": 3 }, { "alias": "join-users", "y": 350, "x": 450, "btype": "join", "parent": 5, "id": 4 }, { "alias": "projection-users", "y": 350, "x": 550, "btype": "projection", "parent": 7, "id": 5 }, { "alias": "filter-accounts", "y": 300, "x": 350, "btype": "filter", "parent": 4, "id": 6 }, { "alias": "filter-projection-users", "y": 350, "x": 650, "btype": "filter", "parent": 1, "id": 7 }];
  } else if (ftype == "function" || ftype == "aggregation") {
    response = jsonData;
  } else if (ftype == "union") {
    response = {cp: jsonData.cp, rp: unionData};
  } else {
    response = [{ "alias": "Output", "y": 350, "x": 850, "btype": "output", "parent": 0, "id": 1 }, { "alias": "accounts", "y": 300, "x": 250, "btype": "table", "parent": 6, "id": 2 }, { "alias": "users", "y": 400, "x": 350, "btype": "table", "parent": 4, "id": 3 }, { "alias": "join-users", "y": 350, "x": 450, "btype": "join", "parent": 5, "id": 4 }, { "alias": "projection-users", "y": 350, "x": 550, "btype": "projection", "parent": 8, "id": 5 }, { "alias": "filter-accounts", "y": 300, "x": 350, "btype": "filter", "parent": 4, "id": 6 }, { "alias": "filter-projection-users", "y": 350, "x": 750, "btype": "filter", "parent": 1, "id": 7 }, { "alias": "function-projection-users", "y": 350, "x": 650, "btype": "function", "parent": 7, "id": 8 }];
  }

  res.json(response);
});


app.post("/addtable", (req, res) => {
  const { table1, table2, conn_id, schema, panelItems } = req.body;

  const table2Item = panelItems.find((item) => item.id == table2);

  if (!table2Item) {
    return res.sendStatus(400);
  }

  if (table2 == 1) {
    const op = [{ "alias": "Output", "y": 250, "x": 250, "btype": "output", "parent": 0, "id": 1 }, { "alias": "accounts", "y": 250, "x": 150, "btype": "table", "parent": 1, "id": 2 }];
    res.json(op);
  }
  else {
    const newPanelItems = [
      ...panelItems.filter((item) => item.id !== table2Item.id),
      {
        alias: "projection",
        parent: table2Item.parent,
        id: panelItems.length + 1,
      },
      {
        alias: "join",
        parent: panelItems.length + 1,
        id: panelItems.length + 2,
      },
      {
        alias: table1,
        parent: panelItems.length + 2,
        id: panelItems.length + 3,
      },
      {
        alias: table2Item.alias,
        parent: panelItems.length + 2,
        id: table2Item.id,
      },
    ]
    // res.json([
    //   {"alias":"Output","y":350,"x":550,"btype":"output","parent":0,"id":1},
    //   {"alias":"sqldi","y":400,"x":250,"btype":"table","parent":4,"id":2},
    //   {"alias":"graph","y":300,"x":250,"btype":"table","parent":4,"id":3},
    //   {"alias":"join-graph","y":350,"x":350,"btype":"join","parent":5,"id":4},
    //   {"alias":"projection-graph","y":350,"x":450,"btype":"projection","parent":1,"id":5}
    // ]);
    res.json(jsonData.cp);
  };

});

app.post("/saveColumn", (req, res) => {
  console.log(req.body);

  res.json({
    "id": "5",
    "alias": "projection-graph",
    "parent_object": "Table_view",
    "business_name": "projection-graph",
    "technical_name": "projection-graph",
    "btype": "projection",
    "items": [
      {
        "tid": 2,
        "business_name": "sqldi.id",
        "technical_name": "sqldi.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "sqldi.name",
        "technical_name": "sqldi.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.id",
        "technical_name": "graph.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.version",
        "technical_name": "graph.version",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.name",
        "technical_name": "graph.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.definition",
        "technical_name": "graph.definition",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ]
  });
});

app.post("/saveAggregation", (req, res) => {
  console.log(req.body);

  res.json({
    "id": "5",
    "alias": "projection-graph",
    "parent_object": "Table_view",
    "business_name": "projection-graph",
    "technical_name": "projection-graph",
    "btype": "projection",
    "items": [
      {
        "tid": 2,
        "business_name": "sqldi.id",
        "technical_name": "sqldi.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "sqldi.name",
        "technical_name": "sqldi.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.id",
        "technical_name": "graph.id",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.version",
        "technical_name": "graph.version",
        "semantic_type": "integer",
        "data_type": "integer",
        "key": "YES",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.name",
        "technical_name": "graph.name",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      },
      {
        "tid": 2,
        "business_name": "graph.definition",
        "technical_name": "graph.definition",
        "semantic_type": "text",
        "data_type": "text",
        "key": "NO",
        "column_type": "column"
      }
    ]
  });
});

app.get("/getItem", (req, res) => {
  const responses = {
    1: {
      "id": "1",
      "alias": "Output",
      "parent_object": "Output",
      "business_name": "View 2 business name",
      "technical_name": "View 2 technical name",
      "btype": "output",
      "items": [
        {
        "tid": 2,
          "business_name": "accounts.user_id",
          "technical_name": "accounts.user_id",
          "semantic_type": "integer",
          "data_type": "integer",
          "key": "YES",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "accounts.username",
          "technical_name": "accounts.username",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "accounts.password",
          "technical_name": "accounts.password",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "accounts.email",
          "technical_name": "accounts.email",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "accounts.created_at",
          "technical_name": "accounts.created_at",
          "semantic_type": "datetime",
          "data_type": "datetime",
          "key": "NO",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "accounts.last_login",
          "technical_name": "accounts.last_login",
          "semantic_type": "datetime",
          "data_type": "datetime",
          "key": "NO",
          "column_type": "attribute"
        },
        {
        "tid": 2,
          "business_name": "users.id",
          "technical_name": "users.id",
          "semantic_type": "integer",
          "data_type": "integer",
          "key": "YES",
          "column_type": "measure"
        },
        {
        "tid": 2,
          "business_name": "users.username",
          "technical_name": "users.username",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "measure"
        },
        {
        "tid": 2,
          "business_name": "users.password",
          "technical_name": "users.password",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "measure"
        },
        {
        "tid": 2,
          "business_name": "users.email",
          "technical_name": "users.email",
          "semantic_type": "varchar",
          "data_type": "varchar",
          "key": "NO",
          "column_type": "measure"
        },
        {
        "tid": 2,
          "business_name": "users.created_at",
          "technical_name": "users.created_at",
          "semantic_type": "datetime",
          "data_type": "datetime",
          "key": "NO",
          "column_type": "measure"
        }
      ]
    }
    ,
    2: { "id": "2", "alias": "accounts", "parent_object": "Table_view", "business_name": "accounts", "technical_name": "accounts", "btype": "table", "items": [{ "business_name": "accounts.user_id", "technical_name": "accounts.user_id", "semantic_type": "integer", "data_type": "integer", "key": "YES", "column_type": "column" }, { "business_name": "accounts.username", "technical_name": "accounts.username", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.password", "technical_name": "accounts.password", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.email", "technical_name": "accounts.email", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.created_at", "technical_name": "accounts.created_at", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }, { "business_name": "accounts.last_login", "technical_name": "accounts.last_login", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }] },
    5: { "id": "5", "alias": "users", "parent_object": "Table_view", "business_name": "users", "technical_name": "users", "btype": "table", "items": [{ "business_name": "users.id", "technical_name": "users.id", "semantic_type": "integer", "data_type": "integer", "key": "YES", "column_type": "column" }, { "business_name": "users.username", "technical_name": "users.username", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.password", "technical_name": "users.password", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.email", "technical_name": "users.email", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.created_at", "technical_name": "users.created_at", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }] },
    4: {
      "id": "4",
      "alias": "join-graph",
      "parent_object": "Table_view",
      "business_name": "join-graph",
      "technical_name": "join-graph",
      "btype": "join",
      "jtype": "Inner",
      "distinct": "true",
      "t1": "SalesOrders",
      "t2": "SalesOrderItems",
      t1items: [
        {
        "tid": 2,
          business_name: "SalesOrder.id",
          technical_name: "SalesOrder.id",
          semantic_type: "integer",
          data_type: "integer",
          key: "YES",
          column_type: "column"
        },
        {
        "tid": 2,
          business_name: "SalesOrder.name",
          technical_name: "SalesOrder.name",
          semantic_type: "text",
          data_type: "text",
          key: "NO",
          column_type: "column"
        }
      ],
      t2items: [
        {
        "tid": 2,
          business_name: "SalesOrderItems.id",
          technical_name: "SalesOrderItems.id",
          semantic_type: "integer",
          data_type: "integer",
          key: "YES",
          column_type: "column"
        },
        {
        "tid": 2,
          business_name: "SalesOrderItems.name",
          technical_name: "SalesOrderItems.name",
          semantic_type: "text",
          data_type: "text",
          key: "NO",
          column_type: "column"
        }
      ],
      join_items: {
        table1: ["SalesOrder.name", "SalesOrder.id"],
        table2: ["SalesOrderItems.name", "SalesOrderItems.id"]
      }
    },
    3: { "id": "3", "alias": "projection-users", "parent_object": "Table_view", "business_name": "projection-users", "technical_name": "projection-users", "btype": "projection", "items": [{ "business_name": "accounts.user_id", "technical_name": "accounts.user_id", "semantic_type": "integer", "data_type": "integer", "key": "YES", "column_type": "column" }, { "business_name": "accounts.username", "technical_name": "accounts.username", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.password", "technical_name": "accounts.password", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.email", "technical_name": "accounts.email", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "accounts.created_at", "technical_name": "accounts.created_at", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }, { "business_name": "accounts.last_login", "technical_name": "accounts.last_login", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }, { "business_name": "users.id", "technical_name": "users.id", "semantic_type": "integer", "data_type": "integer", "key": "YES", "column_type": "column" }, { "business_name": "users.username", "technical_name": "users.username", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.password", "technical_name": "users.password", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.email", "technical_name": "users.email", "semantic_type": "varchar", "data_type": "varchar", "key": "NO", "column_type": "column" }, { "business_name": "users.created_at", "technical_name": "users.created_at", "semantic_type": "datetime", "data_type": "datetime", "key": "NO", "column_type": "column" }] },
    6: jsonData1.rp,
    7: jsonData.rp,
    8: unionData
  };

  const parsedUrl = url.parse(req.url, true);

  const id = parsedUrl.query.id;

  const response = responses[id];

  res.json(response);
});


app.listen(port, () => {
  // console.log(`Server is running on port ${port}`);
});
