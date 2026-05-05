#### Add `Minimal Compute Policy` To Lakeflow Connect Ingestion Gateway Pipeline
```bash
databricks pipelines list-pipelines --output json

databricks pipelines update pipeline_id --json '{
  "name": "gw_ingestion_silver",
  "catalog": "ws_dbxproject",
  "schema": "00_landing",
  "gateway_definition": {
    "connection_name": "conn_restaurantops",
    "gateway_storage_catalog": "ws_dbxproject",
    "gateway_storage_schema": "00_landing",
    "gateway_storage_name": "gw_ingestion_silver"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "000ED73F24A01A46",
      "apply_policy_default_values": true
    }
  ],
  "continuous": true
}'
```

### databricks configure --token

### databricks secrets

### databricks secrets create-scope sql-credentials

### databricks auth login --host https://azuredatabricks.net #workspace URL

### databricks secrets create-scope sql-credentials --initial-manage-principal users -p "Zuhair Bhati"

### databricks secrets put-secret sql-credentials host --string-value "server host" -p "Zuhair Bhati"

### databricks secrets put-secret sql-credentials database --string-value "database name" -p "Zuhair Bhati"

### databricks secrets put-secret sql-credentials username --string-value "user_name" -p "Zuhair Bhati"

### databricks secrets put-secret sql-credentials password -p "Zuhair Bhati"


#### CDC Changes
```sql
UPDATE customers
SET city='Abu Dhabi'
WHERE customer_id='CUST-10000';


INSERT INTO dbo.menu_items (restaurant_id, item_id, name, category, price, ingredients, is_vegetarian, spice_level)
VALUES ('REST-AUH-001','ITEM-999','Samosa (2 pcs)','Starter',18.49,'Potato, Peas, Spices, Pastry',1,'Medium');
```

### SDP Event Log
```sql
SELECT
  timestamp,
  details:flow_definition.output_dataset,
  details:flow_progress.status,
  details:planning_information
FROM event_log(TABLE(`03_gold`.d_customer_360))
WHERE details:planning_information IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10;
```