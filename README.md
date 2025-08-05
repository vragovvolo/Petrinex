# Petrinex

This repo contains code for the Petrinex project. It illustrates how to use declarative pipelines to build a data ingestion pipeline from public data followed by decline curve analysis.

## Getting started

0. Install UV: https://docs.astral.sh/uv/getting-started/installation/

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle validate
    $ databricks bundle deploy --target dev
    ```

4. To deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ``` 

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```
6. Optionally, install the Databricks extension for Visual Studio code for local development from
   https://docs.databricks.com/dev-tools/vscode-ext.html. It can configure your
   virtual environment and setup Databricks Connect for running unit tests locally.
   When not using these tools, consult your development environment's documentation
   and/or the documentation for Databricks Connect for manually setting up your environment
   (https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html).

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.


## Notes

- Make a 00_setup task, define UC functions and register
- Join ingest & process, refactor to SQL? Trigger on each run (weekly / monthly)
- Run forecast post ingest & process job?
- Add expectations to declarative pipeline / tables as data quality solution


## Testing

- As this is mainly designed to run in Databricks, we use an actual spark connection to run any spark unit tests.
- The tests are run in Serverless to replicate a full serverless workflow.