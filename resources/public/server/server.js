const express = require("express");
const cors = require("cors");
const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());

app.post("/api/connection", (req, res) => {
  const { connectionName } = req.body;

  const response = {
    connectionName,
    databaseName: "Postgres",
    schemaName: "public",
    tables: [
      {
        name: "accounts",
        columns: [
          {
            name: "user_id",
            type: "integer",
          },
          {
            name: "username_character",
            type: "varying",
          },
          {
            name: "password_character",
            type: "varying",
          },
          {
            name: "email_character",
            type: "varying",
          },
          {
            name: "created_at",
            type: "timestamp",
          },
          {
            name: "last_login",
            type: "timestamp",
          },
        ],
      },
      {
        name: "users",
        columns: [
          {
            name: "user_id",
            type: "integer",
          },
          {
            name: "username_character",
            type: "varying",
          },
          {
            name: "password_character",
            type: "varying",
          },
          {
            name: "email_character",
            type: "varying",
          },
          {
            name: "created_at",
            type: "timestamp",
          },
          {
            name: "last_login",
            type: "timestamp",
          },
        ],
      },
    ],
  };

  res.json(response);
});

app.post("/api/newgraph", (req, res) => {
  const { graphname } = req.body;

  const response = {
    alias: graphname,
    id: 1,
    parent: 0,
  };

  res.json(response);
});

app.post("/api/join", (req, res) => {
  const { table1, table2, conn_id, schema, panelItems } = req.body;

  const table2Item = panelItems.find((item) => item.id == table2);

  if (!table2Item) {
    return res.sendStatus(400);
  }

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
  ];

  res.json(newPanelItems);
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
