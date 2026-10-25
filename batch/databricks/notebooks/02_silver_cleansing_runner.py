"""Databricks Notebook 02_silver_cleansing_runner entrypoint."""

from batch.databricks.notebooks.silver_cleansing_runner import (
    main,
    parse_args,
    run_silver_cleansing,
)

if __name__ == "__main__":
    main()
