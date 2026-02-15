curl -X POST http://localhost:8000/save/save-conn \
     -H "Content-Type: application/json" \
     -d '{
           "connection_name": "harish1",
           "dbtype": "postgresql",
           "dbname": "bitool",
           "schema": "bitool”,
           "host": "localhost",
           "port": 5432,
           "username": "postgres",
           "password": "postgres",
           "sid": null,
           "service": null }'
