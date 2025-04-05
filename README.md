# Azure END-TO-END Data Engineeing Project: Incremental data pipeline and Medallion Architecture with Databricks

## Project Architecture

<img width="468" alt="image" src="https://github.com/user-attachments/assets/45a559fc-8f35-4197-9e2a-c6c016366314" />

## Azure Environment Setup

The project provisions the following core Azure resources:

- Resource Group to group all Azure resources logically.
- ADLS Gen2 Storage Account with hierarchical namespace enabled:
  - raw — raw landing zone
  - conformed — cleaned/structured data
  - curated — business-ready dimensional data
- Azure SQL Database — to store metadata and watermarking information.
- Azure Data Factory — for orchestrating data ingestion and transformation.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/df532a4e-4f97-425e-a033-b33105e11505" />

## Implementation Overview

### Step 1: Source Preparation

To initiate the pipeline, we start by creating a schema in the Azure SQL Database, which defines the table structure and column names but does not yet contain data. The next phase involves ingesting raw data using Azure Data Factory (ADF). We use the Copy Activity, where the source is an HTTP endpoint (a GitHub-hosted CSV file), and the sink is the Azure SQL Database. For dynamic ingestion, a parameterized dataset is created—this includes a filename parameter allowing us to ingest new, incremental data simply by passing the new file name to the dataset.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/b3b4798b-b413-4bcd-8f2b-b7ba7718907d" />
<br><br>
<img width="468" alt="image" src="https://github.com/user-attachments/assets/0f75dc15-9d2f-40c7-8ef1-236423de6ebe" />


### Step 2: Incremental Load Pipeline

For automated incremental data loading, we utilize two Lookup activities in ADF. The first retrieves the last_load date, and the second fetches the current_load date from the source data. Since our dataset uses Date_ID values (e.g., DT00001) instead of standard timestamps, we manually initialize the watermark table with a starting value like DT00000 to ensure the very first record is included in the initial load.

The last_load Lookup activity reads from the watermark table:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/7a2b0317-0698-4011-95e6-04d9d81c3389" />

```sql
SELECT * FROM watermark_table
```

The current_load Lookup activity fetches the latest Date_ID from the source data table:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/4107ab82-326a-4d03-b9b9-8efcb8e68394" />

```sql
SELECT MAX(Date_ID) AS max_date FROM cars_source_data
```

These values are used in the Copy Activity's query to filter data:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/69393f5d-308e-4713-997e-c07dcbf5a42c" />

```sql
SELECT * FROM cars_source_data
WHERE Date_ID > '@{activity('last_load').output.value[0].last_load}'
AND Date_ID <= '@{activity('current_load').output.value[0].max_date}'
```

The ingested data is stored in the raw_data folder of the ADLS Gen2 storage under the "raw" zone. Post-copy, a stored procedure is called to update the watermark table with the latest Date_ID:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/0bc9376c-7650-4945-894d-268542f01a33" />
<br><br>
<img width="468" alt="image" src="https://github.com/user-attachments/assets/e2196fbb-5ff1-4b0a-b7ad-2ab9d74cc9f2" />


```sql
CREATE PROCEDURE UpdateWatermarkTable
    @lastload VARCHAR(200)
AS
BEGIN
    BEGIN TRANSACTION;
    UPDATE [dbo].[watermark_table]
    SET last_load = @lastload;
    COMMIT TRANSACTION;
END;
```

This ensures that each new run starts where the previous one left off, supporting continuous and incremental data ingestion.

![image](https://github.com/user-attachments/assets/0ae256de-9cba-4b0a-a8de-3b0301911d09)


## Databricks and Medallion Architecture

Once raw data lands in the Data Lake, we transition to Databricks to implement the Medallion Architecture (Bronze → Silver → Gold layers), refining data incrementally and ensuring high data quality.

### Step 1: Unity Catalog & Metastore Configuration

<img width="468" alt="image" src="https://github.com/user-attachments/assets/50d4b5c0-deb3-438c-b240-e5857b8fc1f4" />

We begin by creating a Unity Metastore from the Databricks Admin Console, assigning the admin role to the Azure account. A dedicated container in ADLS Gen2 is provisioned to store metadata. An Access Connector is then created with the Storage Blob Data Contributor role, allowing Databricks to interact with the data lake. This connector's ID is added to Databricks to enable secure access.

The Unity Catalog is then activated by assigning the metastore to the workspace.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/44a88556-bd28-46e8-bfd8-04e0b635cd31" />


### Step 2: Cluster Creation

A new cluster is launched in Databricks to serve as the compute engine for data transformation and processing across all Medallion layers.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/76c0b480-5cd3-4652-b9d2-9ad8ade35bee" />


### Step 3: External Locations Setup

To access the raw, conformed, and curated data containers, external locations are created in Databricks. Each location uses the Access Connector ID as a storage credential, enabling read/write operations on the data lake folders.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/1fbee508-efa4-486d-8ab6-83b2c6044be0" />


### Step 4: Workspace and Schema Setup

Inside the Databricks workspace, a new project directory is created. Within this directory, notebooks are developed to define and manage:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/62323239-a576-4f0b-b22b-31e46145e605" />

- Catalogs: Top-level namespace objects in the Unity Catalog.
- Schemas: Logical groupings within catalogs (equivalent to databases).
- Tables/Views: The actual datasets, structured and versioned.

## Silver Layer (Conformed Zone)

Using PySpark, data is read from the raw zone in Parquet format, and inferSchema is enabled to detect column types. Transformations such as column splitting, computed fields, and aggregations are applied. This cleansed, enriched dataset is then written to the "conformed" container in ADLS Gen2.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/8aa665c5-6f53-46b8-94c3-8d4b08896dd6" />
<br><br>
<img width="468" alt="image" src="https://github.com/user-attachments/assets/94d37ed3-4dd4-41d8-85f4-e70007b83545" />

You can refer to the Conformed_notebook.py file for the full transformation logic and analytical steps performed.

## Gold Layer (Curated Zone)

In the final Medallion layer, we implement a dimensional model to support business reporting and analytics. This involves:

- Creating Dimension Tables with surrogate keys
- Implementing SCD Type 1 (Upsert) logic to manage changes over time without historical tracking

Each dimension—like Dim_Model, Dim_Dealer, Dim_Branch, and Dim_Date—is handled in separate notebooks following the same modeling pattern. For an example, you can refer to the Curated_dim_model.py file.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/0e89285f-1513-4863-a83c-3028c7371c02" />


### Fact Table Construction

Data Model- Star Schema

<img width="468" alt="image" src="https://github.com/user-attachments/assets/a522a942-29b2-4bf5-a026-4e29fee0e24f" />


Once dimensions are in place, a fact table is constructed by joining cleaned transactional data with the dimension tables:

```python
df_fact = df_conformed \
    .join(df_branch, df_conformed['Branch_ID'] == df_branch['Branch_ID'], 'left') \
    .join(df_dealer, df_conformed['Dealer_ID'] == df_dealer['Dealer_ID'], 'left') \
    .join(df_model, df_conformed['Model_ID'] == df_model['Model_ID'], 'left') \
    .join(df_date, df_conformed['Date_ID'] == df_date['Date_ID'], 'left') \
    .select(
        df_conformed['Revenue'],
        df_conformed['Units_Sold'],
        df_conformed['revperunit'],
        df_branch['dim_branch_key'],
        df_dealer['dim_dealer_key'],
        df_model['dim_mod_key'],
        df_date['dim_date_key']
    )
```

This final fact dataset is saved in the "gold" zone of the data lake. You can refer to the Curated_fact_sales.py file for the implementation.

## End-to-End Pipeline Orchestration

To automate the complete process, a job is configured in Databricks Workflows. Each notebook (dimensions and fact logic) is added as a sequential task. The dependencies are defined such that:

- All dimension tasks depend on the conformed table.
- The fact table task depends on the successful execution of all dimension tasks.

This structure ensures efficient execution and data integrity.

<img width="468" alt="image" src="https://github.com/user-attachments/assets/5ffd4e1a-0362-4c34-9cd3-f304da2daca4" />

Last Successful run:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/27072df2-8499-4ce3-a967-6eb30ad95bef" />
<br><br>
<img width="468" alt="image" src="https://github.com/user-attachments/assets/b61d197a-82df-47d3-9d0d-473f7e5fbb12" />



## Completion

With the successful creation of the Fact and Dimension tables in the Gold layer, data analysts can now query the final datasets directly using the SQL Editor in Databricks. This marks the completion of the end-to-end Azure Data Engineering pipeline, leveraging Azure Data Factory, ADLS Gen2, Azure SQL Database, and Databricks following the Medallion Architecture principles.

## Validation Checks

Post pipeline execution, the following validations were performed:

✅ **Incremental Load Check**: New records successfully appended to the cars_source_data table without duplication.

✅ **Watermark Table Update**: The last_load field in the watermark table was updated correctly after the Incremental run.

✅ **Record Growth Validation**: Record counts increased across layers (raw → silver → gold), confirming data progression.

✅ **Surrogate Keys in Dimensions**: Surrogate keys were generated and incremented properly across dimension tables.

✅ **SCD Type 1 Upserts**: Changes in source data (e.g., updated Model_name) were applied via upserts without historical retention.

