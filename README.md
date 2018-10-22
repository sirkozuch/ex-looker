# Looker Extractor for KBC

Looker extractor for KBC is an extension of extractors, which allows user to download data from looks in Looker via Looker API. Currently, the extractor is in development phase. 

### API Limitations
The extractor is limited by **limitations** of Looker API, which currently are:
* If a look has more than 5000 rows and contains pivot columns, these columns along with dimensions will be omitted. Only non-pivoted columns will be downloaded. 
* If a look has less than 5000 rows, everything will be downloaded, without any limitations to pivoting and/or dimensions.
* If a look does not contain any pivot columns, everything will be downloaded with no limitations to row limit. 

### Input

Configuration schema accepts following parameters:
* **Client ID** - Client ID obtained in the API section of Looker dashboard.
* **Client Secret** - Client Secret obtained in the API section of Looker dashboard.
* **API Endpoint** - API Endpoint via which requests are sent.
* **Look ID** - ID of a look, from which the data should be downloaded. The look is ran automatically.
* **Destination Table** - A table in KBC Storage, where data will be loaded. If left blank, data will be downloaded to `in.c-looker.looker_data_xx`, where `xx` is equal to ID of a look.
* **Incremental Load** - Marks, whether load should be incremental.
* **Primary Key** - Comma-separated columns, which are to be used as PK.
* **Limit** - Row limit for look. See API Limitations above.

For more info about Looker API, see [Looker API Documentation](https://docs.looker.com/reference/api-and-integration/api-getting-started).

### Output

Output of the extractor is a table in storage data for the given look. By default, table is stored in `c-looker` bucket, with ID denoting the table. This behavior can be overwritten by specifying correct and full KBC destination in configuration schema.
