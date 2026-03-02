# Milestone 1 - Inventory Data Engineering (PySpark)

## Artifacts
- `inventory_pipeline.py`: end-to-end PySpark ingestion/cleaning/transformation/loading pipeline.
- `docs/inventory_schema_validation.md`: schema and validation rules.
- `docs/inventory_mongodb_validator.json`: MongoDB collection validator.
- `logs/inventory_pipeline.log`: pipeline execution log.

## Run

```bash
python data_pipeline/inventory_pipeline.py \
  --source-uri az://<container>/inventory.csv \
  --source-format csv \
  --mongo-uri "mongodb://localhost:27017" \
  --mongo-db enterprise_ai_commerce \
  --mongo-collection inventory
```

If `--source-uri` is omitted, synthetic inventory data is generated.

```bash
python data_pipeline/inventory_pipeline.py --synth-records 1000
```

## Remote Source Support
- `az://container/blob-path` (requires `AZURE_STORAGE_CONNECTION_STRING`)
- `http://` or `https://`
- local file path / `file://`
