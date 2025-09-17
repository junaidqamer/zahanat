## Azure Blob to Snowflake (Mule 4)

This Mule 4 application ingests files from Azure Blob Storage and inserts records into a Snowflake table.

### Features
- Scheduler-driven ingestion
- Supports CSV and JSON files
- Inserts rows into a Snowflake table using the Snowflake connector

### Prerequisites
- Mule 4.6.x runtime or Anypoint Studio 7.16+
- Access to Azure Storage account and container
- Snowflake account, warehouse, database, schema, role, and a target table

### Configuration
Copy `src/main/resources/mule-dev.properties` to your environment-specific properties file (or use it directly for local dev) and fill values:

```
ingest.frequency.seconds=300

# Azure Storage
azure.accountName=your_account
azure.accountKey=your_key
azure.containerName=container
azure.prefix=optional/path/prefix

# Snowflake
snowflake.account=your_account_region
snowflake.user=your_user
snowflake.password=your_password
snowflake.warehouse=COMPUTE_WH
snowflake.database=MY_DB
snowflake.schema=PUBLIC
snowflake.role=SYSADMIN
snowflake.table=TARGET_TABLE
```

### Build
```
mvn -q -DskipTests package
```

The resulting deployable will be in `target/`.

### Run (Studio)
- Import into Anypoint Studio
- Set `mule-dev.properties` as the configuration properties file
- Run the app

### Notes
- CSV parsing assumes header row and maps to key-value pairs per column. Ensure column names match Snowflake table columns.
- For large files or higher throughput, consider staging to Snowflake (PUT to stage + COPY INTO) rather than row inserts. This app uses simple inserts for clarity.
# zahanat
a custom algorithmic trading portfolio project
