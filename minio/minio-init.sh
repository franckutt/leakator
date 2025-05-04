#!/bin/sh

# Exit on error
set -e

# Start MinIO temporarily in the background
echo "Starting temporary MinIO server for initialization..."
/usr/bin/minio server /data --console-address ":9001" &
MINIO_PID=$!

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
for i in $(seq 30); do
  if curl -sf http://localhost:9000/minio/health/live >/dev/null; then
    echo "MinIO is ready!"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "Error: MinIO failed to start within 30 seconds"
    kill $MINIO_PID
    exit 1
  fi
  sleep 1
done

# Configure mc alias
echo "Configuring MinIO alias..."
mc alias set minio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Create data bucket
echo "Creating data bucket..."
mc mb minio/data 2>/dev/null || echo "Bucket data already exists"

# Check and upload files from /data
echo "Checking for files in /data..."
if [ -z "$(ls -A /data)" ]; then
  echo "Warning: /data directory is empty, no files to upload"
else
  for file in /data/*; do
    if [ -f "$file" ]; then
      echo "Uploading $file to minio/data/"
      mc cp "$file" minio/data/
    fi
  done
  echo "File upload complete. Listing objects in minio/data:"
  mc ls minio/data
fi

# Stop temporary MinIO server
echo "Stopping temporary MinIO server..."
kill $MINIO_PID
wait $MINIO_PID 2>/dev/null || true

# Start MinIO server in foreground
echo "Starting MinIO server in foreground..."
exec /usr/bin/minio server /data --console-address ":9001"