import multiprocessing
import os
import re
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---
INPUT_FILE = 'server_logs.log'
OUTPUT_DIR = './processed_data/' 
# NOTE: Adjusted for testing. In Prod, we can increase this to 256MB to optimize I/O.
CHUNK_SIZE_MB = 10  

# --- ETL LOGIC ---
# Regex pattern to extract structured fields (IP, Time, Method, Status) from raw text
LOG_PATTERN = re.compile(
    r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' 
    r'\s-\s-\s\['                                 
    r'(?P<timestamp>.*?)\]'                       
    r'\s"(?P<method>\w+)\s(?P<endpoint>.*?)\s.*?"'
    r'\s(?P<status>\d{3})'                        
)

def get_file_chunks(file_path, size_mb):
    """
    LOGIC: Calculates logical split points (byte offsets) for the file.
    BENEFIT: Allows us to process 100GB+ files without loading them into RAM.
    """
    if not os.path.exists(file_path):
        return []
        
    file_size = os.path.getsize(file_path)
    chunk_size = size_mb * 1024 * 1024
    chunks = []
    
    with open(file_path, 'rb') as f:
        current_pos = 0
        while current_pos < file_size:
            start = current_pos
            # Jump ahead by chunk size
            f.seek(min(current_pos + chunk_size, file_size))
            
            # CRITICAL: Read to the next newline so we don't cut a log line in half
            f.readline() 
            end = f.tell()
            
            chunks.append((start, end))
            current_pos = end
    return chunks

def process_log_chunk(args):
    """
    WORKER: This runs in parallel on a separate CPU core.
    TASK: Reads a specific slice of the file, cleans data, and saves to Parquet.
    """
    file_path, start_byte, end_byte, chunk_id, output_dir = args
    data = []
    
    try:
        # Efficient File Reading: Open file and jump straight to assigned chunk
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(start_byte)
            while f.tell() < end_byte:
                line = f.readline()
                if not line: break
                
                # TRANSFORMATION: Regex parsing
                match = LOG_PATTERN.match(line)
                if match:
                    row = match.groupdict()
                    data.append(row)

        # LOAD: Write processed data to storage
        if data:
            df = pd.DataFrame(data)
            
            # DATA OPTIMIZATION: Downcast integer to save storage space
            df['status'] = df['status'].astype('int16') 
            
            output_file = os.path.join(output_dir, f"part-{chunk_id:04d}.parquet")
            
            # STORAGE FORMAT: Parquet + Snappy (Industry standard for Big Data/Spark)
            df.to_parquet(output_file, engine='pyarrow', compression='snappy')
            
            return f"✅ Worker {chunk_id}: Successfully processed {len(df)} rows."
        else:
            return f"⚠️ Worker {chunk_id}: No valid data found in this chunk."

    except Exception as e:
        return f"❌ Worker {chunk_id}: Failed - {str(e)}"

def main():
    # Setup output location
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Validation
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please generate data first.")
        return

    print(f"--- Starting ETL Job on {INPUT_FILE} ---")
    
    # Step 1: Divide and Conquer Strategy
    print("Calculating file chunks...")
    chunks = get_file_chunks(INPUT_FILE, CHUNK_SIZE_MB)
    print(f"Job Split: {len(chunks)} independent tasks created.")

    worker_args = [(INPUT_FILE, start, end, i, OUTPUT_DIR) for i, (start, end) in enumerate(chunks)]

    # Step 2: Parallel Processing
    # Automatically detects how many cores your server/laptop has
    cpu_count = multiprocessing.cpu_count()
    print(f"Resource Allocation: Using {cpu_count} CPU cores.")
    
    start_time = datetime.now()
    
    # Execute the Pool (Map Phase)
    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = pool.map(process_log_chunk, worker_args)

    # Step 3: Reporting
    duration = datetime.now() - start_time
    print(f"\n--- Job Complete in {duration} ---")
    
    # Optional: Print first few results to verify
    for res in results[:5]: 
        print(res)

if __name__ == '__main__':
    main()