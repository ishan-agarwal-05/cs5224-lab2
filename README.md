# CS5224 Lab 2: Twitter follower counts

Source code for Part 2 of CS5224 Lab 2 (A0284673R). A Spark job on AWS Glue counts the followers and
followees of every user in `twitter_combined.txt`, and a small Python backend and HTML/JavaScript frontend
display the results.

## Layout

- `analysis/spark_job.py`: the PySpark job that ran on AWS Glue (cleaning, deduplication, counting)
- `analysis/reference.py`: plain Python version I used to check the Spark output
- `aws/glue_cloud.py`: job runner, run from AWS CloudShell. Uploads the input and script to S3, creates and starts the Glue job, waits for it and downloads the results
- `backend/server.py`: API server that loads the results and serves JSON and the frontend
- `frontend/`: the web page (`index.html`, `app.js`, `style.css`)
- `results/cloud-20261011/`: output files downloaded from S3 after the Glue run
- `evidence/`: AWS screenshots and the job run record
- `tests/`: unit tests for the API and the result files
- `report/`: report source (`report.html`) and `build.sh` for the PDF

The dataset itself is not in this repo. Put `twitter_combined.txt` in the top folder to run the Spark job
or `reference.py`.

## Running the app

Only the Python standard library is needed:

```
python3 backend/server.py --port 5224
```

Then open http://127.0.0.1:5224. It reads the Glue output in `results/cloud-20261011/`.

API endpoints:

- `GET /api/users?ids=214328887,107830991`
- `GET /api/users/214328887`
- `GET /api/users?page=1&limit=25&sort=followers&order=desc`
- `GET /api/summary`

## Tests

```
python3 -m unittest discover -s tests -v
```

## Running the Spark job locally

Needs Java 17 and `pip install -r requirements.txt`:

```
spark-submit --master 'local[4]' analysis/spark_job.py \
  --input twitter_combined.txt --output results/my-run --partitions 8
python3 backend/server.py --results results/my-run
```
