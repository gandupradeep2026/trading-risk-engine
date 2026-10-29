"""Databricks Notebook 03_gold_var_feature_store_runner entrypoint."""

from batch.databricks.notebooks.gold_var_runner import (
    main,
    parse_args,
    run_gold_var_pipeline,
)

if __name__ == "__main__":
    main()
