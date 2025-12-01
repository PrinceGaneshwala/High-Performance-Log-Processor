import multiprocessing
import os
import re
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---
INPUT_FILE = 'server_logs.log'
# Changed to relative path so it runs on Mac without permission errors
OUTPUT_DIR = './processed_data/' 
CHUNK_SIZE_MB = 10  # Reduced to 10MB for testing with smaller files
LOG_PATTERN = re.compile(
    r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' 
    r'\s-\s-\s\['                                 
    r'(?P<timestamp>.*?)\]'                       
    r'\s"(?P<method>\w+)\s(?P<endpoint>.*?)\s.*?"'
    r'\s(?P<status>\d{3})'                        
)

def get_file_chunks(file_path, size_mb):
    """Generates start/end byte positions to split file logically."""
    if not os.path.exists(file_path):
        return []
        
    file_size = os.path.getsize(file_path)
    chunk_size = size_mb * 1024 * 1024
    chunks = []
    
    with open(file_path, 'rb') as f:
        current_pos = 0
        while current_pos < file_size:
            start = current_pos
            f.seek(min(current_pos + chunk_size, file_size))
            f.readline() # Read until newline to avoid cutting lines
            end = f.tell()
            chunks.append((start, end))
            current_pos = end
    return chunks

def process_log_chunk(args):
    """Worker function: Reads bytes, parses, and writes Parquet."""
    file_path, start_byte, end_byte, chunk_id, output_dir = args
    data = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(start_byte)
            while f.tell() < end_byte:
                line = f.readline()
                if not line: break
                
                match = LOG_PATTERN.match(line)
                if match:
                    row = match.groupdict()
                    data.append(row)

        if data:
            df = pd.DataFrame(data)
            df['status'] = df['status'].astype('int16') # Optimization
            
            output_file = os.path.join(output_dir, f"part-{chunk_id:04d}.parquet")
            df.to_parquet(output_file, engine='pyarrow', compression='snappy')
            return f"✅ Worker {chunk_id}: Written {len(df)} rows."
        else:
            return f"⚠️ Worker {chunk_id}: No valid data found."

    except Exception as e:
        return f"❌ Worker {chunk_id}: Error - {str(e)}"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Check if input exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please generate dummy data first.")
        return

    print(f"Analyzing {INPUT_FILE}...")
    chunks = get_file_chunks(INPUT_FILE, CHUNK_SIZE_MB)
    print(f"Split input into {len(chunks)} chunks.")

    worker_args = [(INPUT_FILE, start, end, i, OUTPUT_DIR) for i, (start, end) in enumerate(chunks)]

    cpu_count = multiprocessing.cpu_count()
    print(f"Starting processing with {cpu_count} cores...")
    
    start_time = datetime.now()
    
    # Run the parallel workers
    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = pool.map(process_log_chunk, worker_args)

    print(f"\n--- Processing Complete in {datetime.now() - start_time} ---")
    for res in results:
        print(res)

if __name__ == '__main__':
    main()