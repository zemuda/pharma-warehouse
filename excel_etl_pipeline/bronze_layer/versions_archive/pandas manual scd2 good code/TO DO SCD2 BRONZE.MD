## Run the loader:

```bash
# Using the default config
python bronze_loader_pandas.py

# Or specify config
python bronze_loader_pandas.py --config bronze_loader_config_pandas.yaml

```

## For large datasets (memory management):
```bash
# Create a batch processing script
python bronze_batch_processor.py
```

Key Advantages of Pandas Version:
CPU Compatibility: Works on any CPU without AVX2/SIMD issues

Familiar API: Pandas is widely known and used

Built-in Memory Management: Chunk processing for large datasets

Flexibility: Easy to extend and customize

Performance Tips:
For large files: Enable chunked processing in config

Memory issues: Reduce chunk_size in config

Speed: Install modin for parallel processing

Very large datasets: Use dask for out-of-core computation

Output Structure:

bronze_data/
├── bronze/
│   └── tables/
│       ├── bronze_sales_cash_invoices_detailed/  # Delta table
│       ├── bronze_sales_cash_invoices_summarized/
│       └── bronze_master_data_customer_accounts_all_branches/
├── logs/
│   └── bronze_loader_pandas.log
└── checkpoints/

https://chat.deepseek.com/a/chat/s/cb355974-3e46-4dad-a3fe-67ce01a895c9

Q: what happens if primary keys is not unique or it is unavailable

# In downstream processing, check:
df = pl.read_delta(table_path)
scd2_status = df.select("_broze_is_current").unique()

if len(scd2_status) == 1 and scd2_status[0, 0] == True:
    print("⚠️ Warning: Table has no SCD2 history (all records current)")