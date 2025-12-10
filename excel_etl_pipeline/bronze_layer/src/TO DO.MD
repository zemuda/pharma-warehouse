<!-- TO DO: WHAT IS retention_days: 90  # Keep 90 days IN TIME TRAVEL -->
time_travel:
    enabled: true
    retention_days: 90  # Keep 90 days of history
    max_versions: 100   # Maximum versions to keep

ncremental_loading:
    enabled: true
    checkpoint_enabled: true
    <!--  -->
    lookback_days: 7  # Process files from last 7 days if checkpoint missing

optimize:
    enabled: true
    frequency: "weekly"

data_skipping:
    enabled: true  # Auto-enabled by Delta
    indexed_columns: 15

bloom_filters