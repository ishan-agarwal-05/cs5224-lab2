"""Read-only API and frontend: stdlib locally; AWS Lambda + private S3 in cloud."""
from __future__ import annotations
import argparse
import csv
import io
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
ASSETS = {'/': ('index.html', 'text/html; charset=utf-8'),
          '/index.html': ('index.html', 'text/html; charset=utf-8'),
          '/style.css': ('style.css', 'text/css; charset=utf-8'),
          '/app.js': ('app.js', 'application/javascript; charset=utf-8')}
ID = re.compile(r'^[0-9]{1,40}$')


class ResultStore:
    def __init__(self, directory=None):
        cloud = ROOT / 'results/cloud-20261011'
        default = cloud if (cloud / 'metadata').exists() else ROOT / 'results/spark-local'
        self.directory = Path(directory or os.getenv('RESULTS_DIR', default))
        self.bucket = os.getenv('RESULTS_BUCKET')
        self.prefix = os.getenv('RESULTS_PREFIX', 'results/run-1').strip('/')
        self.lock = threading.Lock()
        self.rows, self.by_id, self.metadata = [], {}, {}
        self.loaded_at = 0

    def documents(self, name, extension):
        if self.bucket:
            import boto3
            s3 = boto3.client('s3')
            prefix = self.prefix + '/' + name + '/'
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=prefix):
                for item in page.get('Contents', []):
                    key = item['Key']
                    if key.rsplit('/', 1)[-1].startswith('part-') and key.endswith(extension):
                        yield s3.get_object(Bucket=self.bucket, Key=key)['Body'].read().decode('utf-8')
        else:
            for path in sorted((self.directory / name).glob('part-*' + extension)):
                yield path.read_text()

    def load(self):
        if self.loaded_at and (not self.bucket or time.monotonic() - self.loaded_at < 60):
            return
        with self.lock:
            if self.loaded_at and (not self.bucket or time.monotonic() - self.loaded_at < 60):
                return
            metadata = [json.loads(line) for text in self.documents('metadata', '.json')
                        for line in text.splitlines() if line.strip()]
            if len(metadata) != 1:
                raise RuntimeError('Spark output is missing or incomplete: expected one metadata record.')
            by_id = {}
            for text in self.documents('users', '.csv'):
                for row in csv.DictReader(io.StringIO(text)):
                    user = row['user_id']
                    if not ID.fullmatch(user) or user in by_id:
                        raise RuntimeError('Invalid or duplicate user in results.')
                    item = dict(user_id=user, followers=int(row['followers']), followees=int(row['followees']))
                    if min(item['followers'], item['followees']) < 0:
                        raise RuntimeError('Negative count in results.')
                    by_id[user] = item
            meta = metadata[0]
            if (len(by_id) != meta['unique_users'] or
                sum(x['followers'] for x in by_id.values()) != meta['unique_edges'] or
                sum(x['followees'] for x in by_id.values()) != meta['unique_edges']):
                raise RuntimeError('Result integrity checks failed.')
            self.rows = sorted(by_id.values(), key=lambda x: int(x['user_id']))
            self.by_id = by_id
            self.metadata = {k: v for k, v in meta.items() if k not in {'input_uri', 'master'}}
            self.metadata['source'] = ('S3 cloud results' if self.bucket else
                'AWS Glue cloud results, imported from private S3; app hosted locally'
                if meta.get('execution_mode') == 'cloud/distributed' else
                'Local Spark results (not cloud evidence)')
            self.loaded_at = time.monotonic()


STORE = ResultStore()


def dispatch(path, query='', method='GET', store=None):
    store = store or STORE
    headers = {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store',
               'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'same-origin'}
    def response(status, body):
        return status, headers, json.dumps(body).encode()
    if method != 'GET':
        return response(405, {'error': 'Only GET is supported.'})
    if path in ASSETS:
        filename, content_type = ASSETS[path]
        headers.update({'Content-Type': content_type,
                        'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; frame-ancestors 'none'"})
        return 200, headers, (ROOT / 'frontend' / filename).read_bytes()
    if path not in {'/api/summary', '/api/users', '/health'} and not path.startswith('/api/users/'):
        return response(404, {'error': 'Route not found.'})
    try:
        store.load()
    except Exception:
        return response(503, {'error': 'Spark results are unavailable or incomplete. Run/import a successful job first.'})
    if path == '/health':
        return response(200, {'status': 'ok', 'users': len(store.rows)})
    if path == '/api/summary':
        return response(200, store.metadata)
    try:
        params = parse_qs(query)
        if path.startswith('/api/users/'):
            user = path.rsplit('/', 1)[-1]
            if not ID.fullmatch(user):
                return response(400, {'error': 'User ID must be numeric.'})
            row = store.by_id.get(str(int(user)))
            return response(200, row) if row else response(404, {'error': 'User is not present in this dataset.'})
        if 'ids' in params:
            ids = params['ids'][0].split(',')
            if len(ids) > 20 or not all(ID.fullmatch(x) for x in ids):
                raise ValueError('Provide at most 20 numeric IDs.')
            ids = list(dict.fromkeys(str(int(x)) for x in ids))
            return response(200, {'users': [store.by_id[x] for x in ids if x in store.by_id],
                                  'missing': [x for x in ids if x not in store.by_id]})
        page, limit = int(params.get('page', ['1'])[0]), int(params.get('limit', ['25'])[0])
        if page < 1 or not 1 <= limit <= 100:
            raise ValueError('Page must be positive; limit must be 1–100.')
        sort = params.get('sort', ['user_id'])[0]
        if sort not in {'user_id', 'followers', 'followees'}:
            raise ValueError('Unknown sort field.')
        order = params.get('order', ['asc'])[0]
        if order not in {'asc', 'desc'}:
            raise ValueError('Order must be asc or desc.')
        q = params.get('q', [''])[0].strip()
        if len(q) > 40 or (q and not ID.fullmatch(q)):
            raise ValueError('Search must contain numeric IDs only.')
        rows = [x for x in store.rows if q in x['user_id']] if q else store.rows
        if sort != 'user_id' or order != 'asc':
            rows = sorted(rows, key=lambda x: (int(x[sort]), int(x['user_id'])), reverse=order == 'desc')
        total = len(rows)
        return response(200, {'users': rows[(page - 1)*limit:page*limit], 'page': page,
                              'limit': limit, 'total': total, 'pages': (total+limit-1)//limit})
    except (ValueError, OverflowError) as exc:
        return response(400, {'error': str(exc)})


def lambda_handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', 'GET')
    status, headers, body = dispatch(event.get('rawPath', '/'), event.get('rawQueryString', ''), method)
    return {'statusCode': status, 'headers': headers, 'body': body.decode('utf-8'), 'isBase64Encoded': False}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        status, headers, body = dispatch(url.path, url.query)
        self.send_response(status)
        for key, value in headers.items(): self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def do_POST(self):
        self.send_error(405)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=5224)
    p.add_argument('--results', type=Path)
    args = p.parse_args()
    global STORE
    STORE = ResultStore(args.results)
    STORE.load()
    print(f'Lab 2 dashboard: http://{args.host}:{args.port}', flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
