import argparse
import asyncio
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading
import boto3
from botocore.config import Config

# Глобальные счетчики для статистики
stats = defaultdict(lambda: [0, 0])


def upload_worker(thread_id, bucket, obj_size_mb, stop_event, uploaded_keys, s3_args):
    """Воркер для непрерывной заливки объектов в S3."""
    session = boto3.session.Session()
    s3_client = session.client(
        's3',
        endpoint_url=s3_args['endpoint_url'],
        aws_access_key_id=s3_args['aws_access_key_id'],
        aws_secret_access_key=s3_args['aws_secret_access_key'],
        region_name=s3_args['region_name'],
        config=Config(
            max_pool_connections=1,
            retries={'max_attempts': 30}
        )
    )
    
    # Генерируем случайные данные нужного размера в памяти
    data = os.urandom(obj_size_mb * 1024 * 1024)
    
    while not stop_event.is_set():
        key = f"load-test/{thread_id}_{int(time.time() * 1000)}.bin"
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=data)
            
            uploaded_keys.append(key)
            stats[thread_id][0] += 1  # Всего
            stats[thread_id][1] += 1  # За секунду
        except Exception as e:
            # Ошибки пишем в stderr, чтобы не ломать stdout парсерам
            print(f"ERROR: Thread {thread_id} failed to upload: {e}", file=sys.stderr)
            time.sleep(0.1)


async def stats_printer(duration, stop_event):
    """Ежесекундный вывод статистики в формате CSV для машинного чтения."""
    start_time = time.time()
    
    # Заголовок для парсеров
    print("timestamp_epoch,thread_id,total_uploaded,last_report_uploaded", flush=True)
    
    while time.time() - start_time < duration:
        await asyncio.sleep(5)
        
        current_timestamp = int(time.time())
        
        for thread_id in sorted(stats.keys()):
            total, current_sec = stats[thread_id]
            print(f"{current_timestamp},{thread_id},{total},{current_sec}", flush=True)
            stats[thread_id][1] = 0
            
    stop_event.set()


def cleanup_objects(bucket, keys, s3_args):
    """Удаление объектов пачками по 1000 штук с явными credentials."""
    if not keys:
        return
        
    print(f"CLEANUP: Deleting {len(keys)} objects...", file=sys.stderr)
    
    s3_client = boto3.client(
        's3',
        endpoint_url=s3_args['endpoint_url'],
        aws_access_key_id=s3_args['aws_access_key_id'],
        aws_secret_access_key=s3_args['aws_secret_access_key'],
        region_name=s3_args['region_name']
    )
    
    chunk_size = 1000
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i:i + chunk_size]
        delete_objects = {'Objects': [{'Key': k} for k in chunk]}
        try:
            s3_client.delete_objects(Bucket=bucket, Delete=delete_objects)
        except Exception as e:
            print(f"CLEANUP_ERROR: Failed to delete chunk: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="S3 Load Test (Args-configured)")
    # Обязательные параметры S3
    parser.add_argument('--bucket', required=True, help="S3 bucket name")
    parser.add_argument('--endpoint', required=True, help="S3 endpoint URL (e.g. https://storage.yandexcloud.net)")
    parser.add_argument('--access-key', required=True, help="AWS Access Key ID")
    parser.add_argument('--secret-key', required=True, help="AWS Secret Access Key")
    parser.add_argument('--region', default='us-east-1', help="S3 region name (default: us-east-1)")
    
    # Параметры нагрузки
    parser.add_argument('--threads', type=int, default=10, help="Number of threads")
    parser.add_argument('--size', type=int, default=4, help="Object size in MB")
    parser.add_argument('--duration', type=int, default=30, help="Duration in seconds")
    
    args = parser.parse_args()

    # Собираем аргументы S3 в словарь для передачи в потоки
    s3_args = {
        'endpoint_url': args.endpoint,
        'aws_access_key_id': args.access_key,
        'aws_secret_access_key': args.secret_key,
        'region_name': args.region
    }

    uploaded_keys = []
    stop_event = threading.Event()

    # Пред-инициализация структуры под треды
    for t_id in range(args.threads):
        stats[t_id] = [0, 0]

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [
            executor.submit(upload_worker, i, args.bucket, args.size, stop_event, uploaded_keys, s3_args)
            for i in range(args.threads)
        ]

        try:
            asyncio.run(stats_printer(args.duration, stop_event))
        except KeyboardInterrupt:
            stop_event.set()

        for fut in futures:
            fut.result()

    cleanup_objects(args.bucket, uploaded_keys, s3_args)


if __name__ == '__main__':
    main()
