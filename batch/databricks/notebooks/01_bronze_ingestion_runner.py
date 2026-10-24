"""Databricks Notebook 01_bronze_ingestion_runner entrypoint."""

from batch.databricks.notebooks.bronze_ingestion_runner import (
    main,
    parse_args,
    run_bronze_ingestion,
)

if __name__ == "__main__":
    main()
