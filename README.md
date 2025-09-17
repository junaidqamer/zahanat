## Azure Blob to Snowflake Mule App

This Mule 4 application periodically lists blobs in an Azure Storage container/prefix and ingests each file into Snowflake using a `COPY INTO` command from an External Stage. After a successful load, the blob is copied to an archive location and the original is deleted.

### Prerequisites

- Mule 4.6.x runtime (Studio 7.16+ recommended)
- Access to the Azure Storage account and container
- Snowflake account with a Warehouse, Database, Schema, Role
- A Snowflake External Stage pointing to the Azure container (via Storage Integration or SAS)
- A Snowflake File Format compatible with your file(s)

### Configure Snowflake External Stage (example)

Create or use a Storage Integration and Stage. Example using Storage Integration:

```sql
-- One-time setup by a Snowflake admin
CREATE OR REPLACE STORAGE INTEGRATION AZURE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('azure://<ACCOUNT>.blob.core.windows.net/<CONTAINER>/<PREFIX>');

-- Create an external stage referencing the Azure container path
CREATE OR REPLACE STAGE EXT_AZURE_STAGE
  STORAGE_INTEGRATION = AZURE_INT
  URL = 'azure://<ACCOUNT>.blob.core.windows.net/<CONTAINER>/<PREFIX>';

-- Create a file format compatible with your files
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER=1;
```

Update the app properties to reference `EXT_AZURE_STAGE` and `MY_CSV_FORMAT`.

### Configuration

Set values in `src/main/resources/mule-app.properties`:

```
# Scheduler
scheduler.frequency.ms=60000

# Azure Blob Storage
azure.account.name=your_account_name
azure.account.key=your_account_key
azure.container.name=source-container
azure.prefix=path/in/container/
azure.archive.container=archive-container
azure.archive.prefix=archive/

# Snowflake
snowflake.account=xy12345.us-east-1
snowflake.username=svc_mule
snowflake.password=****
snowflake.warehouse=LOAD_WH
snowflake.database=RAW
snowflake.schema=PUBLIC
snowflake.role=SYSADMIN
snowflake.stage=EXT_AZURE_STAGE
snowflake.table=INGEST_TABLE
snowflake.file_format=MY_CSV_FORMAT
snowflake.copy_on_error=CONTINUE
```

If you prefer keypair auth, switch the connector configuration in `azure-blob-to-snowflake.xml` accordingly.

### How it works

1. A scheduler triggers every `scheduler.frequency.ms`.
2. Lists blobs under `azure.container.name` and `azure.prefix`.
3. For each blob, issues `COPY INTO <db>.<schema>.<table> FROM @<stage> FILES=('<blobName>') FILE_FORMAT=(FORMAT_NAME=<file_format>)`.
4. Copies the blob to `azure.archive.container`/`azure.archive.prefix` and deletes the original.

### Run locally (Studio)

1. Import the project into Anypoint Studio.
2. From Exchange, ensure the connectors are resolved: Azure Storage and Snowflake.
3. Set properties in `mule-app.properties`.
4. Run the app. Watch the console for execution logs.

### Deploy to CloudHub (optional)

Use the Mule Maven Plugin or Runtime Manager to deploy. Provide secure property values as secure app properties.

### Notes

- The app assumes files are already in the external stage location; no content is downloaded to Mule.
- Adjust `FILE_FORMAT` and table definition to match your data.
- Consider idempotency: Snowflake `COPY INTO` can load the same file once if `ON_ERROR` and file metadata are configured appropriately (review `COPY_OPTIONS` like `FORCE=FALSE`).
# zahanat
a custom algorithmic trading portfolio project
