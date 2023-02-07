import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq

def write_chunk_to_parquet(chunk, out_path):
    writer = pq.ParquetWriter(out_path, chunk.schema)
    next_table = pa.Table.from_batches([chunk])
    writer.write_table(next_table)
    writer.close()

in_path = 'people2M.csv'
out_paths = [f"people2M-Py_{i}.parquet" for i in range(4)]

with pyarrow.csv.open_csv(in_path) as reader:
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        chunk_counter = 0
        futures = []
        for next_chunk in reader:
            if next_chunk is None:
                break
            chunk_counter += 1
            out_path = out_paths[chunk_counter % 4]
            future = executor.submit(write_chunk_to_parquet, next_chunk, out_path)
            futures.append(future)
        concurrent.futures.wait(futures)
