# Bulletin Data Load Behavior Matrix

## Complete Behavior Matrix

| Trigger | `full_refresh` | `last_modified_date` | `upload_to_s3` | Expected Behavior | Use Case |
|---------|----------------|----------------------|----------------|-------------------|----------|
| **Schedule Run** | `False` | `None` | `False` | • Auto-discovers baseline from S3 via `list_objects_v2()`<br>• Finds most recent file by `LastModified` timestamp<br>• Extracts date from S3 key: `load_date=YYYY-MM-DD`<br>• If S3 empty: defaults to `2000-01-01T00:00:00`<br>• API call: `modified_after={discovered_date}`<br>• Exports to local Parquet only (no S3 upload) | **Standard incremental load** for testing/development in containerized environment |
| **Schedule Run** | `False` | `None` | `True` | • Same S3 auto-discovery as above<br>• Incremental load with `modified_after={discovered_date}`<br>• Exports to local Parquet<br>• **Uploads to S3**: `s3://bucket/bulletin_raw/{endpoint}/load_date={date}/load_time={time}/`<br>• Future runs detect this new partition as baseline | **Production incremental load** with S3 persistence |
| **Manual: Full Refresh** | `True` | `None` or any | `False` | • **Ignores all baseline logic** (full_refresh takes precedence)<br>• Returns `filter_date=None`<br>• API call: No `modified_after` parameter<br>• Fetches **ALL data** from WordPress API<br>• Local Parquet export only | Complete data refresh, initial migration, data quality validation |
| **Manual: Full Refresh** | `True` | `None` or any | `True` | • Same as above: fetches ALL data<br>• Exports to local Parquet<br>• **Uploads full dataset to S3**<br>• Creates new partition with current timestamp | Historical data load to S3, disaster recovery, complete rebuild |
| **Manual: Custom Baseline** | `False` | `2024-01-15` | `False` | • **Skips S3 auto-discovery**<br>• User-provided date takes precedence<br>• Transforms input: `2024-01-15` → `2024-01-15T00:00:00`<br>• API call: `modified_after=2024-01-15T00:00:00`<br>• Local Parquet export only | Backfill from specific date, testing incremental logic, recovery from known date |
| **Manual: Custom Baseline** | `False` | `2024-01-15` | `True` | • Same custom baseline logic<br>• API call: `modified_after=2024-01-15T00:00:00`<br>• Exports to local Parquet<br>• **Uploads to S3** with current partition timestamp<br>• Future auto-discovery may use this or newer partitions | Backfill to S3 from specific date, targeted data recovery |
| **First Run (Cold Start)** | `False` | `None` | `False` | • S3 query returns empty: `Contents` not in response<br>• Default baseline applied: `2000-01-01T00:00:00`<br>• API call: `modified_after=2000-01-01T00:00:00`<br>• Effectively fetches all historical data<br>• Local Parquet export only | Initial testing run, local development setup |
| **First Run (Cold Start)** | `False` | `None` | `True` | • Same as above: defaults to `2000-01-01T00:00:00`<br>• Fetches all historical data<br>• Exports to local Parquet<br>• **Uploads to S3**: creates first partition<br>• **Next run will use this partition as baseline** | Production bootstrap, initial S3 data lake population |

---

## Decision Logic Flow

```
determine_filter_date(endpoint, config, aws_s3_config, get_config, context):
│
├─ IF full_refresh == True
│  └─ RETURN None
│     └─ API: {base_url}{endpoint}?per_page=100&page={N}
│
├─ ELSE IF last_modified_date provided (user override)
│  └─ Transform: "YYYY-MM-DD" → "YYYY-MM-DDTHH:MM:SS"
│     └─ RETURN transformed_date
│        └─ API: {base_url}{endpoint}?modified_after={transformed_date}&per_page=100&page={N}
│
└─ ELSE (auto-discovery mode)
   └─ get_last_modified_from_s3(endpoint, aws_s3_config, get_config, context)
      │
      ├─ S3 Query: list_objects_v2(Bucket, Prefix="bulletin_raw/{endpoint}/")
      │
      ├─ IF 'Contents' in response AND response['Contents'] not empty
      │  └─ most_recent = max(objects, key=lambda x: x['LastModified'])
      │     └─ Extract date with regex: load_date=(\d{4}-\d{2}-\d{2})
      │        └─ RETURN extracted_date
      │           └─ API: {base_url}{endpoint}?modified_after={extracted_date}&per_page=100&page={N}
      │
      └─ ELSE (no S3 objects found)
         └─ RETURN "2000-01-01T00:00:00" (default baseline)
            └─ API: {base_url}{endpoint}?modified_after=2000-01-01T00:00:00&per_page=100&page={N}
```

---

## Configuration Precedence

**Order of precedence** (highest to lowest):

1. **`full_refresh=True`** → Overrides everything, fetches all data
2. **`last_modified_date` provided** → Skips S3 auto-discovery, uses user-provided date
3. **S3 auto-discovery** → Queries S3 for most recent partition
4. **Default baseline** → `2000-01-01T00:00:00` (when S3 is empty)

---

## Additional Runtime Config Options

| Config Field | Type | Description | Default | Notes |
|--------------|------|-------------|---------|-------|
| `upload_to_s3` | `bool` | Enable S3 upload after local Parquet export | `False` | Schedule hardcodes to `False` for testing |
| `full_refresh` | `bool` | Bypass incremental logic and fetch all data | `False` | Takes precedence over all baseline logic |
| `last_modified_date` | `str` | Custom baseline date in YYYY-MM-DD format | `None` | Auto-converted to ISO 8601 datetime |
| `load_date` | `str` | Override partition date (YYYY-MM-DD) | Current date | Used for S3 path and local folder structure |
| `load_time` | `str` | Override partition time (HH:MM:SS) | Current time | Used for S3 path and local folder structure |

---

## Key Behavioral Notes

1. **Schedule Configuration**: Currently hardcoded with `upload_to_s3=False` and `full_refresh=False` (testing mode)
2. **Empty API Response**: If no records modified since baseline, asset returns `"0 records processed (no updates since {date})"`
3. **Date Transformation**: User inputs accepted in friendly `YYYY-MM-DD` format, automatically converted to `YYYY-MM-DDTHH:MM:SS`
4. **S3 as Source of Truth**: In containerized Kubernetes environment with emptyDir volumes, S3 is the persistent state storage
5. **Testing Frequency**: Schedule runs every 5 minutes (change to `"0 10 * * *"` for daily production runs)
6. **First Run Behavior**: When no S3 objects exist, system gracefully defaults to historical baseline without errors

---

## Recommendations

### For Testing/Development
- Use schedule as-is: `upload_to_s3=False`, `full_refresh=False`
- S3 auto-discovery will fall back to `2000-01-01T00:00:00` on first run
- Subsequent runs use local S3 bucket partitions for baseline

### For Production
1. Change schedule to daily: `cron_schedule="0 10 * * *"`
2. Enable S3 upload: `upload_to_s3=True` in schedule config
3. First run will populate S3 with historical data
4. Subsequent runs incrementally append only modified records

### For Manual Operations
- **Data Quality Check**: Set `full_refresh=True` to validate complete dataset
- **Targeted Backfill**: Provide `last_modified_date="2024-01-15"` to reprocess from specific date
- **Recovery**: Use `full_refresh=True` + `upload_to_s3=True` to rebuild S3 data lake
