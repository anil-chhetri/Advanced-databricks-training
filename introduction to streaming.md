%md
# Streaming DataFrames - Conversation Summary

## Topic 1: How to Stop Streaming Aggregation DataFrames

### Key Concept
**DataFrames don't have a `.stop()` method**. You must stop the **StreamingQuery** object returned by `.start()`.

### Methods to Stop Streaming Queries

**Method 1: Stop by Query Handle (Recommended)**
```python
# When you start a stream, save the handle
query = df.writeStream.start(...)

# Later, stop it
query.stop()
```

**Method 2: Stop by Query Name**
```python
# If you named your query
for stream in spark.streams.active:
    if stream.name == "your_query_name":
        stream.stop()
```

**Method 3: Stop All Active Streams**
```python
# Stop everything
for stream in spark.streams.active:
    stream.stop()
```

---

## Topic 2: Stopping the Streaming Display in Cell 6

### The Problem
Cell 6 uses `display(df, streamName='coupon_sales')` which creates a streaming query without returning a handle.

### Solution: Stop by Stream Name
```python
# Stop the streaming display by name
for stream in spark.streams.active:
    if stream.name == 'coupon_sales':
        stream.stop()
        print(f"Stopped stream: {stream.name}")
```

---

## Topic 3: Difference Between `display()` and `writeStream`

### Cell 6 - `display()` Approach
```python
display(df, streamName='coupon_sales')
```

**Characteristics:**
* **Sink type**: In-memory table (temporary)
* **Purpose**: Visualization only
* **Query handle**: No direct handle returned
* **Output**: Notebook UI table widget
* **Persistence**: Data NOT written to disk
* **Trigger**: Default (as fast as possible)
* **How to stop**: Find by name in `spark.streams.active`

**Equivalent to:**
```python
df.writeStream
    .format("memory")
    .queryName("coupon_sales")
    .outputMode("append")
    .start()
```

### Cell 20 - `writeStream` Approach
```python
coupon_sales_query = (
    coupon_sales_df
    .writeStream
    .outputMode('append')
    .format("delta")
    .trigger(processingTime="1 second")
    .queryName("coupon_sales_write_stream")
    .option("checkpointLocation", coupons_checkpoint_path)
    .start(coupons_output_path)
)
```

**Characteristics:**
* **Sink type**: Delta files on disk (persistent)
* **Purpose**: Permanent data storage
* **Query handle**: Returns `StreamingQuery` object
* **Output**: Parquet files in Delta format
* **Persistence**: Data permanently written to storage
* **Trigger**: Explicitly set to 1 second intervals
* **How to stop**: `coupon_sales_query.stop()`

### Comparison Table

| Aspect | `display()` | `writeStream` |
|--------|-------------|---------------|
| **Sink** | In-memory table | Delta files on disk |
| **Purpose** | Visualization | Persistent storage |
| **Query handle** | No | Yes (StreamingQuery) |
| **Trigger** | Default (fast) | 1 second (explicit) |
| **Persistence** | Temporary | Permanent |
| **Managed by** | Databricks UI + Spark | Spark streaming engine |

---

## Topic 4: Checking Batch Details

### Method 1: Last Batch Progress
```python
# Get the most recent batch information
last_progress = coupon_sales_query.lastProgress
print(last_progress)
```

### Method 2: Recent Batches (Last ~20)
```python
# Get information about recent batches (typically last 10-20 batches)
recent_progress = coupon_sales_query.recentProgress

print(f"Number of batches tracked: {len(recent_progress)}\n")

# Display details for each batch
for i, batch in enumerate(recent_progress):
    print(f"Batch {i + 1}:")
    print(f"  Batch ID: {batch.get('batchId', 'N/A')}")
    print(f"  Timestamp: {batch.get('timestamp', 'N/A')}")
    print(f"  Input Rows: {batch.get('numInputRows', 0)}")
    print(f"  Processing Time: {batch.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")
    print(f"  Sources: {batch.get('sources', [])}")
    print("-" * 50)
```

### Method 3: Detailed Batch Metrics (JSON)
```python
import json

# Pretty print the last batch with all details
if coupon_sales_query.lastProgress:
    print(json.dumps(coupon_sales_query.lastProgress, indent=2))
else:
    print("No progress information available yet")
```

### Batch Progress Information Includes:
* **`batchId`**: Sequential batch number (0, 1, 2, ...)
* **`timestamp`**: When the batch was processed
* **`numInputRows`**: Number of rows in this batch
* **`inputRowsPerSecond`**: Input rate
* **`processedRowsPerSecond`**: Processing rate
* **`durationMs`**: Time breakdown for different phases
* **`sources`**: Input source details (files, offsets)
* **`sink`**: Output destination details
* **`stateOperators`**: Metrics for stateful operations

---

## Topic 5: Accessing ALL Batch Details (Beyond 20-Batch Limit)

### Limitation
`recentProgress` only keeps the last ~20 batches in memory.

### Solution 1: Query Checkpoint Directory (Best for Historical Data)

**List All Batch Commits**
```python
# The checkpoint directory contains a 'commits' folder with one file per batch
import os

commits_path = f"{coupons_checkpoint_path}/commits"

# List all commit files (each represents a batch)
commit_files = dbutils.fs.ls(commits_path)

print(f"Total batches processed: {len(commit_files)}\n")

# Show first 10 and last 10 batch files
for i, file in enumerate(commit_files[:10]):
    print(f"Batch {i}: {file.name}")

if len(commit_files) > 20:
    print("...")
    for i, file in enumerate(commit_files[-10:], start=len(commit_files)-10):
        print(f"Batch {i}: {file.name}")
```

**Read Specific Batch Details**
```python
# Read a specific batch commit file to see its details
# Batch files are named as numbers: 0, 1, 2, etc.

batch_number = 0  # Change this to read different batches
batch_file_path = f"{coupons_checkpoint_path}/commits/{batch_number}"

try:
    # Read the commit file content
    batch_content = dbutils.fs.head(batch_file_path)
    print(f"Batch {batch_number} details:")
    print(batch_content)
except Exception as e:
    print(f"Could not read batch {batch_number}: {e}")
```

**Get Batch Count from Offsets**
```python
# Alternative: Check the offsets directory to see all processed batches
offsets_path = f"{coupons_checkpoint_path}/offsets"

try:
    offset_files = dbutils.fs.ls(offsets_path)
    print(f"Total batches tracked in offsets: {len(offset_files)}")
    
    # The highest numbered file tells you the latest batch ID
    batch_ids = [int(f.name) for f in offset_files if f.name.isdigit()]
    if batch_ids:
        print(f"Batch IDs range: {min(batch_ids)} to {max(batch_ids)}")
        print(f"Total unique batches: {len(batch_ids)}")
except Exception as e:
    print(f"Could not read offsets: {e}")
```

### Solution 2: StreamingQueryListener (For Real-Time Tracking)

**Set Up Custom Listener**
```python
from pyspark.sql.streaming import StreamingQueryListener

# Storage for all batch progress
all_batches = []

class BatchProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")
    
    def onQueryProgress(self, event):
        # Store every batch progress
        progress = event.progress
        all_batches.append({
            'batchId': progress.batchId,
            'timestamp': progress.timestamp,
            'numInputRows': progress.numInputRows,
            'inputRowsPerSecond': progress.inputRowsPerSecond,
            'processedRowsPerSecond': progress.processedRowsPerSecond,
            'durationMs': progress.durationMs
        })
        print(f"Batch {progress.batchId} completed - {progress.numInputRows} rows")
    
    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

# Add the listener
listener = BatchProgressListener()
spark.streams.addListener(listener)

print("Listener added. All future batch progress will be tracked in 'all_batches' list.")
```

**Note**: Listener must be set up BEFORE starting the streaming query.

### Comparison of Methods

| Method | Access to Batches | When to Use |
|--------|-------------------|-------------|
| **`recentProgress`** | Last ~20 batches only | Quick check of recent activity |
| **Checkpoint directory** | ALL batches (persisted) | Historical analysis after query stops |
| **StreamingQueryListener** | ALL batches (in-memory) | Real-time tracking during execution |

---

## Topic 6: Batch IDs and Empty Batches

### Key Behavior
**Batch IDs are only created when there is data to process.**

If a trigger fires but finds no new data:
* ❌ No batch is created
* ❌ No batch ID is assigned
* ✅ Trigger waits for next interval

### Example Timeline

| Time | Trigger Fires | New Files? | Batch Created? | Batch ID |
|------|---------------|------------|----------------|----------|
| 0s | Yes | Yes (1 file) | ✅ Yes | 0 |
| 1s | Yes | Yes (1 file) | ✅ Yes | 1 |
| 2s | Yes | **No** | ❌ No | - |
| 3s | Yes | **No** | ❌ No | - |
| 4s | Yes | Yes (1 file) | ✅ Yes | 2 |

**Result**: Batch IDs go 0 → 1 → 2 (skipping times when no data was available)

### In Your Notebook Context

Your streaming query configuration:
```python
.trigger(processingTime="1 second")  # Checks every 1 second
.option('maxFilesPerTrigger', 1)     # Processes 1 file per batch
```

**What happens:**
1. Every 1 second, Spark checks for new files
2. If file exists → creates batch, processes it, assigns batch ID
3. If no file exists → **no batch created**, waits for next trigger
4. Batch IDs are **sequential but may have time gaps** between them

### Implications

* **Checkpoint directory**: Only contains batch IDs for batches that processed data
* **`recentProgress`**: Only shows batches where data was processed
* **`all_batches` list**: Only captures batches with actual processing
* **Time gaps**: If you see batch IDs 0, 1, 2, 5, 6, 10... it means triggers 3, 4, 7, 8, 9 found no new data

### Why This Design?

Spark doesn't waste resources creating empty batches when there's nothing to process. This is **by design** and optimizes resource usage.

---

## Summary of Key Takeaways

1. **Stop streaming queries** using the StreamingQuery handle, not the DataFrame
2. **`display()` vs `writeStream`**: display() is for visualization (memory sink), writeStream is for persistent storage
3. **Check batch details** using `lastProgress` and `recentProgress` properties
4. **Access all batches** by reading the checkpoint directory (commits and offsets folders)
5. **Empty batches don't create batch IDs** - only batches with data are numbered
6. **Use StreamingQueryListener** for real-time tracking of all batches during execution