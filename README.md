# High-Performance Log Processor

## Overview
This tool is a specialized ETL (Extract, Transform, Load) script designed to process massive server logs (10GB+) on a single machine without consuming excessive RAM. It utilizes Python's `multiprocessing` to parallelize parsing and converts raw text logs into optimized **Parquet** files with Snappy compression.

## Architecture
- **Input:** Raw Server Logs (Text)
- **Processing:** - Chunk-based file reading (Memory Safe)
    - Parallel execution using CPU cores
    - Regex pattern matching for extraction
- **Output:** Apache Parquet (Columnar Storage)

## Tech Stack
- **Language:** Python 3.x
- **Libraries:** Pandas, PyArrow, Multiprocessing
- **Concepts:** Distributed Processing, Data Serialization

## How to Run
1. Install dependencies:
   ```bash
   pip install pandas pyarrow fastparquet