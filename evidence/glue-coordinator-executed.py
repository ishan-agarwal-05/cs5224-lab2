"""Run from authenticated AWS CloudShell after uploading lab2-cloud-input.tar.gz.
Creates only the user-approved private S3 bucket, Glue role and bounded Spark job.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

REGION = 'ap-southeast-1'
ACCOUNT = '350445708909'
BUCKET = 'cs5224-lab2-a0284673r-350445708909-20261011'
ROLE = 'cs5224-lab2-a0284673r-glue'
JOB = 'cs5224-lab2-a0284673r-twitter'
PREFIX = 'results/cloud-20261011'
INPUT_SHA = '0246d0cae6b4b3c874c968e5caed5e08753a98ce36be2ddb861fce982afdf92c'
ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / 'evidence/cloud-state.json'


def save(data):
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(data, indent=2, default=str) + '\n')


def load(): return json.loads(STATE.read_text())


def session():
    sess = boto3.Session(region_name=REGION)
    identity = sess.client('sts').get_caller_identity()
    if identity['Account'] != ACCOUNT: raise RuntimeError('Wrong AWS account; stopping.')
    return sess


def create(sess):
    if STATE.exists(): raise RuntimeError('State already exists. Inspect it instead of deploying twice.')
    digest = hashlib.sha256((ROOT / 'twitter_combined.txt').read_bytes()).hexdigest()
    if digest != INPUT_SHA: raise RuntimeError('Input differs from the validated course dataset.')
    s3, iam, glue = [sess.client(x) for x in ['s3', 'iam', 'glue']]
    # Refuse to overwrite pre-existing identities/resources.
    for client, method, args in [(iam, 'get_role', {'RoleName': ROLE}), (glue, 'get_job', {'JobName': JOB})]:
        try:
            getattr(client, method)(**args)
        except ClientError as exc:
            if exc.response['Error']['Code'] not in {'NoSuchEntity', 'EntityNotFoundException'}: raise
        else: raise RuntimeError('A named lab role/job already exists; stopping rather than modifying it.')
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={'LocationConstraint': REGION})
    state = dict(bucket=BUCKET, role=ROLE, job=JOB, region=REGION, account=ACCOUNT,
                 input_sha256=digest, results_prefix=PREFIX, stage='BUCKET_CREATED')
    save(state)
    s3.put_public_access_block(Bucket=BUCKET, PublicAccessBlockConfiguration={
        'BlockPublicAcls': True, 'IgnorePublicAcls': True, 'BlockPublicPolicy': True, 'RestrictPublicBuckets': True})
    s3.put_bucket_encryption(Bucket=BUCKET, ServerSideEncryptionConfiguration={'Rules': [
        {'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})
    s3.put_bucket_ownership_controls(Bucket=BUCKET, OwnershipControls={'Rules': [{'ObjectOwnership': 'BucketOwnerEnforced'}]})
    arn = f'arn:aws:s3:::{BUCKET}'
    s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps({'Version':'2012-10-17','Statement':[
        {'Effect':'Deny','Principal':'*','Action':'s3:*','Resource':[arn,arn+'/*'],
         'Condition':{'Bool':{'aws:SecureTransport':'false'}}}]}))
    s3.put_bucket_tagging(Bucket=BUCKET, Tagging={'TagSet':[{'Key':'Course','Value':'CS5224'},{'Key':'Lab','Value':'2'}]})
    s3.upload_file(str(ROOT / 'twitter_combined.txt'), BUCKET, 'input/twitter_combined.txt')
    s3.upload_file(str(ROOT / 'analysis/spark_job.py'), BUCKET, 'scripts/spark_job.py')
    trust={'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'glue.amazonaws.com'},'Action':'sts:AssumeRole'}]}
    result=iam.create_role(RoleName=ROLE,AssumeRolePolicyDocument=json.dumps(trust),
                           Description='CS5224 Lab 2: Glue access scoped to the private lab bucket.',
                           Tags=[{'Key':'Course','Value':'CS5224'},{'Key':'Lab','Value':'2'}])
    state.update(role_arn=result['Role']['Arn'],stage='ROLE_CREATED'); save(state)
    logs_arn=f'arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws-glue/jobs/*'
    policy={'Version':'2012-10-17','Statement':[
        {'Effect':'Allow','Action':['s3:GetBucketLocation'],'Resource':arn},
        {'Effect':'Allow','Action':['s3:ListBucket'],'Resource':arn,
         'Condition':{'StringLike':{'s3:prefix':['input/*','scripts/*','results/*','temporary/*','spark-events/*']}}},
        {'Effect':'Allow','Action':['s3:GetObject'],'Resource':[arn+'/input/*',arn+'/scripts/*',arn+'/results/*']},
        {'Effect':'Allow','Action':['s3:PutObject','s3:DeleteObject','s3:AbortMultipartUpload'],
         'Resource':[arn+'/results/*',arn+'/temporary/*',arn+'/spark-events/*']},
        {'Effect':'Allow','Action':['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],'Resource':logs_arn},
        {'Effect':'Allow','Action':'cloudwatch:PutMetricData','Resource':'*',
         'Condition':{'StringEquals':{'cloudwatch:namespace':'Glue'}}}
    ]}
    iam.put_role_policy(RoleName=ROLE,PolicyName='Lab2ScopedDataAndJobLogs',PolicyDocument=json.dumps(policy))
    time.sleep(15)  # IAM eventual consistency, not an automatic workload retry.
    glue.create_job(Name=JOB,Role=state['role_arn'],GlueVersion='5.0',
        Command={'Name':'glueetl','ScriptLocation':f's3://{BUCKET}/scripts/spark_job.py','PythonVersion':'3'},
        WorkerType='G.1X',NumberOfWorkers=3,Timeout=10,MaxRetries=0,
        ExecutionProperty={'MaxConcurrentRuns':1},ExecutionClass='STANDARD',
        DefaultArguments={'--input':f's3://{BUCKET}/input/twitter_combined.txt','--output':f's3://{BUCKET}/{PREFIX}',
                          '--partitions':'8','--TempDir':f's3://{BUCKET}/temporary/',
                          '--enable-spark-ui':'true','--spark-event-logs-path':f's3://{BUCKET}/spark-events/',
                          '--enable-metrics':'true','--job-bookmark-option':'job-bookmark-disable',
                          '--conf':'spark.sql.shuffle.partitions=8 --conf spark.sql.adaptive.enabled=false'},
        Tags={'Course':'CS5224','Lab':'2'})
    state.update(stage='READY',created_at=datetime.now(timezone.utc).isoformat());save(state)
    print(json.dumps(state,indent=2))


def run(sess):
    state=load()
    if state.get('run_id'): raise RuntimeError('A run was already submitted; no second run is authorised.')
    glue=sess.client('glue')
    run_id=glue.start_job_run(JobName=JOB)['JobRunId']
    state.update(run_id=run_id,stage='SUBMITTED',submitted_at=datetime.now(timezone.utc).isoformat());save(state)
    print('SUBMITTED',run_id,flush=True)
    start=time.monotonic()
    while time.monotonic()-start<900:
        job=glue.get_job_run(JobName=JOB,RunId=run_id,PredecessorsIncluded=False)['JobRun']
        status=job['JobRunState']
        print(f'{int(time.monotonic()-start)}s: {status}',flush=True)
        state.update(stage=status,job_run=job);save(state)
        if status=='SUCCEEDED': export(sess,state);return
        if status in {'FAILED','TIMEOUT','STOPPED','ERROR','EXPIRED'}:
            raise RuntimeError(f"Job {status}: {job.get('ErrorMessage','')}. No automatic retry.")
        time.sleep(20)
    raise TimeoutError('Local wait expired. Inspect the existing job; do not launch another.')


def export(sess,state):
    s3=sess.client('s3'); destination=ROOT/'results/cloud-20261011';destination.mkdir(parents=True,exist_ok=True)
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=PREFIX+'/'):
        for item in page.get('Contents',[]):
            relative=item['Key'][len(PREFIX)+1:]
            if relative and not relative.endswith('/'):
                path=destination/relative;path.parent.mkdir(parents=True,exist_ok=True)
                s3.download_file(BUCKET,item['Key'],str(path))
    expected={r['user_id']:(int(r['followers']),int(r['followees'])) for r in csv.DictReader((ROOT/'results/reference/users.csv').open())}
    actual={}
    for file in (destination/'users').glob('part-*.csv'):
        for row in csv.DictReader(file.open()):
            if row['user_id'] in actual:raise RuntimeError('Duplicate output user.')
            actual[row['user_id']]=(int(row['followers']),int(row['followees']))
    if actual!=expected:raise RuntimeError('Cloud results differ from independent reference.')
    metadata=json.loads(next((destination/'metadata').glob('part-*.json')).read_text())
    if metadata.get('execution_mode')=='local':raise RuntimeError('Unexpected local execution on Glue.')
    state.update(stage='VERIFIED',verified_users=len(actual),required_users={u:actual[u] for u in ['214328887','107830991']},metadata=metadata);save(state)
    # Seven-day retention on newly created Glue log groups; no changes to pre-existing groups.
    for group in ['/aws-glue/jobs/error','/aws-glue/jobs/output']:
        try:sess.client('logs').put_retention_policy(logGroupName=group,retentionInDays=7)
        except ClientError:pass
    output=ROOT/'lab2-cloud-output.zip'
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
        z.write(STATE,'evidence/cloud-state.json')
        for file in destination.rglob('*'):
            if file.is_file():z.write(file,str(file.relative_to(ROOT)))
    key='results/artifacts/lab2-cloud-output.zip'
    s3.upload_file(str(output),BUCKET,key)
    print('VERIFIED ALL USERS:',len(actual),flush=True)
    print(json.dumps(state,indent=2,default=str),flush=True)
    print('DOWNLOAD_URL='+s3.generate_presigned_url('get_object',Params={'Bucket':BUCKET,'Key':key},ExpiresIn=900),flush=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['create','run','status','export'])
    args=p.parse_args();sess=session()
    if args.command=='create':create(sess)
    elif args.command=='run':run(sess)
    elif args.command=='status':
        state=load();data=sess.client('glue').get_job_run(JobName=JOB,RunId=state['run_id'])['JobRun']
        print(json.dumps(data,indent=2,default=str))
    else:
        state=load()
        if sess.client('glue').get_job_run(JobName=JOB,RunId=state['run_id'])['JobRun']['JobRunState']!='SUCCEEDED':
            raise RuntimeError('Only successful runs may be exported.')
        export(sess,state)


if __name__=='__main__':main()
