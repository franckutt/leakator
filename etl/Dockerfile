FROM python:3.11-slim

WORKDIR /app

COPY etl_script.py .

RUN pip install elasticsearch==8.13.2 tqdm boto3 minio requests

CMD ["sh", "-c", "python etl_script.py && tail -f /dev/null"]
