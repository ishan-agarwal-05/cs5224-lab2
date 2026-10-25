import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


reference = load('reference', 'analysis/reference.py')
backend = load('backend', 'backend/server.py')


class LabTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(reference.parse_edge(' 001\t002\n'), ('1', '2'))
        for bad in ['one two', '1 2 3', '-1 2', '1.0 2', '']:
            self.assertIsNone(reference.parse_edge(bad))

    def test_dirty_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'edges.txt'
            path.write_text('1 2\n1 2\n2 1\n3 2\n4 4\n\nwrong row\n001 002\n')
            rows, meta = reference.analyse(path)
        self.assertEqual(rows, [dict(user_id='1', followers=1, followees=1),
                                dict(user_id='2', followers=2, followees=1),
                                dict(user_id='3', followers=0, followees=1)])
        self.assertEqual(meta['duplicate_edges_removed'], 2)
        self.assertEqual(meta['self_loop_lines'], 1)
        self.assertEqual(meta['blank_lines'], 1)
        self.assertEqual(meta['malformed_lines'], 1)

    def test_every_spark_result_matches_reference(self):
        with (ROOT / 'results/reference/users.csv').open() as f:
            expected = {r['user_id']: (int(r['followers']), int(r['followees'])) for r in csv.DictReader(f)}
        actual = {}
        for file in (ROOT / 'results/spark-local/users').glob('part-*.csv'):
            with file.open() as f:
                for row in csv.DictReader(f):
                    self.assertNotIn(row['user_id'], actual)
                    actual[row['user_id']] = (int(row['followers']), int(row['followees']))
        self.assertEqual(actual, expected)
        self.assertEqual(len(actual), 81306)
        self.assertEqual(actual['214328887'], (180, 214))
        self.assertEqual(actual['107830991'], (180, 157))

    def test_spark_audit_matches_reference(self):
        expected = json.loads((ROOT / 'results/reference/metadata.json').read_text())
        files = list((ROOT / 'results/spark-local/metadata').glob('part-*.json'))
        actual = json.loads(files[0].read_text())
        for key in ['raw_lines', 'blank_lines', 'malformed_lines', 'self_loop_lines',
                    'valid_nonself_lines', 'duplicate_edges_removed', 'unique_edges',
                    'unique_users', 'total_followers', 'total_followees']:
            self.assertEqual(actual[key], expected[key], key)

    def test_api(self):
        store = backend.ResultStore(ROOT / 'results/spark-local')
        def request(path, query=''):
            code, headers, body = backend.dispatch(path, query, store=store)
            return code, json.loads(body)
        self.assertEqual(request('/health')[0], 200)
        self.assertEqual(request('/api/users/214328887')[1]['followees'], 214)
        self.assertEqual(len(request('/api/users', 'ids=214328887,107830991')[1]['users']), 2)
        self.assertEqual(request('/api/users/999999999999999')[0], 404)
        self.assertEqual(request('/api/users/nope')[0], 400)
        self.assertEqual(request('/api/users', 'page=0')[0], 400)
        self.assertEqual(request('/api/users', 'limit=101')[0], 400)
        self.assertEqual(request('/api/users', 'sort=nope')[0], 400)
        self.assertEqual(request('/api/users', 'order=sideways')[0], 400)
        self.assertEqual(request('/api/users', 'q=%3Cscript%3E')[0], 400)
        self.assertEqual(request('/../twitter_combined.txt')[0], 404)
        self.assertEqual(request('/api/users', 'q=214328887')[1]['users'][0]['user_id'], '214328887')
        self.assertEqual(request('/api/users', 'page=1&limit=5')[1]['total'], 81306)
        counts = [r['followers'] for r in request('/api/users', 'sort=followers&order=desc&limit=5')[1]['users']]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_unavailable_results(self):
        with tempfile.TemporaryDirectory() as directory:
            code, _, _ = backend.dispatch('/api/users', store=backend.ResultStore(directory))
            self.assertEqual(code, 503)

    def test_lambda_adapter(self):
        response = backend.lambda_handler({'rawPath': '/api/users/107830991', 'requestContext': {'http': {'method': 'GET'}}}, None)
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body'])['followees'], 157)

    def test_every_cloud_result_matches_reference(self):
        with (ROOT / 'results/reference/users.csv').open() as file:
            expected = {r['user_id']: (int(r['followers']), int(r['followees'])) for r in csv.DictReader(file)}
        actual = {}
        shards = list((ROOT / 'results/cloud-20261011/users').glob('part-*.csv'))
        self.assertEqual(len(shards), 8)
        for file in shards:
            with file.open() as stream:
                for row in csv.DictReader(stream):
                    self.assertNotIn(row['user_id'], actual)
                    actual[row['user_id']] = (int(row['followers']), int(row['followees']))
        self.assertEqual(actual, expected)

    def test_cloud_execution_and_audit(self):
        state = json.loads((ROOT / 'evidence/cloud-state.json').read_text())
        self.assertEqual(state['job_run']['JobRunState'], 'SUCCEEDED')
        self.assertEqual(state['job_run']['Attempt'], 0)
        self.assertEqual(state['job_run']['NumberOfWorkers'], 3)
        self.assertEqual(state['job_run']['Timeout'], 10)
        reference_meta = json.loads((ROOT / 'results/reference/metadata.json').read_text())
        for key in ['raw_lines', 'blank_lines', 'malformed_lines', 'self_loop_lines',
                    'valid_nonself_lines', 'duplicate_edges_removed', 'unique_edges',
                    'unique_users', 'total_followers', 'total_followees']:
            self.assertEqual(state['metadata'][key], reference_meta[key], key)
        self.assertEqual(state['metadata']['execution_mode'], 'cloud/distributed')

    def test_default_api_uses_cloud_output(self):
        store = backend.ResultStore()
        code, _, body = backend.dispatch('/api/summary', store=store)
        summary = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(summary['execution_mode'], 'cloud/distributed')
        self.assertIn('app hosted locally', summary['source'])
        self.assertIn('AWS Glue', summary['source'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
