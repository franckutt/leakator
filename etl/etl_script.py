import csv
import json
import os
import logging
from io import TextIOWrapper, BytesIO
from datetime import datetime
from time import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError as ESConnectionError
from minio import Minio
from minio.error import S3Error

# Configuration via environment variables
ES_HOST = os.getenv("ELASTIC_HOST", "http://elasticsearch:9200")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin1")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin1")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"
REQUEST_TIMEOUT = int(os.getenv("ES_REQUEST_TIMEOUT", 1000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", os.cpu_count() or 4))

# Set up logging
logging.basicConfig(
    filename="./etl_errors.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)
logger = logging.getLogger(__name__)

print('ES_HOST', ES_HOST)
print('MINIO_ENDPOINT', MINIO_ENDPOINT)

# Initialize Elasticsearch client
es = Elasticsearch(
    ES_HOST,
    request_timeout=REQUEST_TIMEOUT,
    retry_on_timeout=True
)

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def ensure_minio_bucket(bucket_name: str) -> bool:
    """Ensure the MinIO bucket exists, create it if it doesn't."""
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created MinIO bucket: {bucket_name}")
        else:
            logger.info(f"MinIO bucket {bucket_name} already exists")
        return True
    except S3Error as e:
        logger.error(f"Failed to create or validate MinIO bucket {bucket_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error with MinIO bucket {bucket_name}: {e}")
        return False

def count_lines_from_stream(data_stream: TextIOWrapper) -> int:
    """Count the number of lines in a streamed file."""
    try:
        return sum(1 for _ in data_stream)
    except Exception as e:
        logger.error(f"Error counting lines in stream: {e}")
        return 0
    finally:
        data_stream.seek(0)  # Reset stream position

def validate_minio_object(bucket_name: str, object_name: str) -> bool:
    """Validate if the object exists and is accessible in MinIO."""
    try:
        stat = minio_client.stat_object(bucket_name, object_name)
        if stat.size == 0:
            logger.error(f"Object {object_name} in bucket {bucket_name} is empty")
            return False
        return True
    except S3Error as e:
        logger.error(f"MinIO error accessing {object_name} in {bucket_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error validating {object_name} in {bucket_name}: {e}")
        return False

def process_batch(batch: List[Dict[str, Any]], index_name: str) -> None:
    """Process a batch of documents and index them into Elasticsearch."""
    actions = [{"_index": index_name, "_source": doc} for doc in batch]
    try:
        success, failed = helpers.bulk(es, actions, raise_on_error=False)
        if failed:
            for item in failed:
                op_type = next(iter(item))
                err_details = item[op_type]
                logger.error(
                    f"Bulk item failed: Action='{op_type}', Index='{err_details.get('_index')}', "
                    f"ID='{err_details.get('_id', 'N/A')}', Status={err_details.get('status')}, "
                    f"Error={err_details.get('error')}"
                )
        logger.info(f"Processed batch: {success} documents indexed, {len(failed)} failed")
    except ESConnectionError as e:
        logger.error(f"Elasticsearch connection error during bulk processing: {e}")
    except RequestError as e:
        logger.error(f"Elasticsearch request error during bulk processing: {e}")
    except Exception as e:
        logger.error(f"General bulk processing error: {e}")

def create_index_with_mapping(index_name: str) -> bool:
    """Create an Elasticsearch index with a default mapping if it doesn't exist."""
    if es.indices.exists(index=index_name):
        logger.info(f"Index '{index_name}' already exists.")
        return True
    try:
        mapping = {
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "email": {"type": "keyword"},
                    "timestamp": {"type": "date"}
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"Created index '{index_name}' with default mapping.")
        return True
    except RequestError as e:
        logger.error(f"Failed to create index '{index_name}': {e}")
        return False

def load_csv_from_minio(bucket_name: str, object_name: str, index_name: str) -> int:
    """Load and index CSV data from MinIO into Elasticsearch."""
    if not validate_minio_object(bucket_name, object_name):
        return 0

    try:
        response = minio_client.get_object(bucket_name, object_name)
        data_stream = TextIOWrapper(BytesIO(response.read()), encoding='utf-8', errors='ignore')
    except S3Error as e:
        logger.error(f"MinIO error fetching {object_name} from {bucket_name}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error fetching {object_name} from {bucket_name}: {e}")
        return 0
    finally:
        response.close()
        response.release_conn()

    total_lines = count_lines_from_stream(data_stream)
    if total_lines == 0:
        logger.warning(f"Skipping empty object: {object_name}")
        return 0

    processed_count = 0
    data_stream.seek(0)  # Reset stream for reading
    reader = csv.DictReader(data_stream)
    batch = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for row in tqdm(reader, total=total_lines, desc=f"Loading {object_name}"):
            try:
                cleaned_row = {k: v for k, v in row.items() if v is not None and v != ''}
                batch.append(cleaned_row)
                processed_count += 1
            except Exception as e:
                logger.error(f"CSV row processing error: {e} — data: {row}")
                continue

            if len(batch) >= BATCH_SIZE:
                futures.append(executor.submit(process_batch, batch.copy(), index_name))
                batch = []

        if batch:
            futures.append(executor.submit(process_batch, batch.copy(), index_name))

        for future in tqdm(futures, desc="Processing batches"):
            future.result()

    return processed_count

def load_jsonl_from_minio(bucket_name: str, object_name: str, index_name: str) -> int:
    """Load and index JSONL data from MinIO into Elasticsearch."""
    if not validate_minio_object(bucket_name, object_name):
        return 0

    try:
        response = minio_client.get_object(bucket_name, object_name)
        data_stream = TextIOWrapper(BytesIO(response.read()), encoding='utf-8', errors='ignore')
    except S3Error as e:
        logger.error(f"MinIO error fetching {object_name} from {bucket_name}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error fetching {object_name} from {bucket_name}: {e}")
        return 0
    finally:
        response.close()
        response.release_conn()

    total_lines = count_lines_from_stream(data_stream)
    if total_lines == 0:
        logger.warning(f"Skipping empty object: {object_name}")
        return 0

    processed_count = 0
    data_stream.seek(0)  # Reset stream for reading
    batch = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        line_num = 0
        for line in tqdm(data_stream, total=total_lines, desc=f"Loading {object_name}"):
            line_num += 1
            try:
                stripped_line = line.strip()
                if not stripped_line:
                    continue
                doc = json.loads(stripped_line)
                cleaned_doc = {k: v for k, v in doc.items() if v is not None and v != ''}
                batch.append(cleaned_doc)
                processed_count += 1
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error at line {line_num}: {e} — data: {line.strip()}")
                continue
            except Exception as e:
                logger.error(f"JSON line processing error at line {line_num}: {e} — data: {line.strip()}")
                continue

            if len(batch) >= BATCH_SIZE:
                futures.append(executor.submit(process_batch, batch.copy(), index_name))
                batch = []

        if batch:
            futures.append(executor.submit(process_batch, batch.copy(), index_name))

        for future in tqdm(futures, desc="Processing batches"):
            future.result()

    return processed_count

def detect_and_load_from_minio(bucket_name: str, object_name: str, index_name: str) -> int:
    """Detect file type and load data from MinIO into Elasticsearch."""
    start_time = time()
    logger.info(f"Starting data load from {object_name} in bucket {bucket_name} into index '{index_name}'")

    if not ensure_minio_bucket(bucket_name):
        print(f"Error: Failed to validate or create bucket {bucket_name}")
        return 0

    if not validate_minio_object(bucket_name, object_name):
        print(f"Error: Invalid object {object_name} in bucket {bucket_name}")
        return 0

    if not create_index_with_mapping(index_name):
        print(f"Error: Failed to create index '{index_name}'")
        return 0

    processed_count = 0
    if object_name.endswith(".csv"):
        processed_count = load_csv_from_minio(bucket_name, object_name, index_name)
    elif object_name.endswith(".json") or object_name.endswith(".jsonl"):
        processed_count = load_jsonl_from_minio(bucket_name, object_name, index_name)
    else:
        err_msg = f"Unsupported file type: {object_name}. Only .csv, .json, .jsonl are supported."
        logger.error(err_msg)
        print(f"Error: {err_msg}")
        return 0

    elapsed_time = time() - start_time
    logger.info(f"Finished loading {processed_count} documents from {object_name} in {elapsed_time:.2f} seconds")
    print(f"Finished loading {processed_count} documents from {object_name} in {elapsed_time:.2f} seconds")
    return processed_count

def main():
    """Main function to execute the ETL process."""
    bucket_name = os.getenv("MINIO_BUCKET", "data")
    object_name = os.getenv("DATA_OBJECT_NAME", "clients_data.csv")
    target_index_name = os.getenv("INDEX_NAME", "import_clients")

    try:
        logger.info("Testing Elasticsearch connection...")
        if not es.ping():
            raise ESConnectionError("Elasticsearch ping failed")
        logger.info(f"Elasticsearch connection successful: {es.info()}")
        print("Elasticsearch connection successful.")

        processed_count = detect_and_load_from_minio(bucket_name, object_name, target_index_name)
        if processed_count == 0:
            logger.warning("No documents were processed.")
            print("No documents were processed.")
    except ESConnectionError as ce:
        logger.error(f"ConnectionError: {ce}")
        print(f"Error: Could not connect to Elasticsearch at {ES_HOST}. Is it running?")
    except Exception as e:
        logger.error(f"Unexpected error during ETL process: {e}")
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()