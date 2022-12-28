from flask import Flask, request
from flask_restx import Resource, Api
import awswrangler as wr
import boto3
import pandas as pd
import os

api = Api()

app = Flask(__name__)
api.init_app(app)

boto3.setup_default_session(aws_access_key_id=os.environ['AWS-ACCESS-KEY'], aws_secret_access_key=os.environ['AWS-SECRET-ACCESS-KEY'])
s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS-ACCESS-KEY'], aws_secret_access_key=os.environ['AWS-SECRET-ACCESS-KEY'])
athena_session = boto3.Session(region_name=os.environ['AWS-DEFAULT-REGION'], aws_access_key_id=os.environ['AWS-ACCESS-KEY'], aws_secret_access_key=os.environ['AWS-SECRET-ACCESS-KEY'])

@api.route('/s3Example')
class S3Example(Resource):
    @api.doc(params={'key': 'key', 'bucket': 'bucket'})
    def get(self):
        bucket = request.args.get('bucket')
        key = request.args.get('key')
        path = f"s3://{bucket}/{key}"
        try:
            df = wr.s3.read_csv(path=path)
        except Exception as e:
            return str(e)
        lst = df.head(5)
        return lst.to_json()

    @api.doc(params={'key': 'key', 'bucket': 'bucket'})
    def post(self):
        lst = ['Java', 'Python', 'C', 'C++', 'JavaScript', 'Swift', 'Go']
        df = pd.DataFrame(lst)
        bucket = request.args.get('bucket')
        key = request.args.get('key')
        path=f"s3://{bucket}/{key}"
        try:
            wr.s3.to_csv(df, path)
            #s3_client.put_object(Body=df.to_csv(), Bucket=filepath, Key='my/test.csv')
        except Exception as e:
            return str(e)
        return "successfully upload sample file to requested path"


@api.route('/athenaExample<dbname>/<bucket>')
@api.doc(params={'dbname': 'dbname', 'bucket': 'bucket'})
class GlueCatalogExample(Resource):
    def get(self, dbname, bucket):
        try:
            query = "SELECT * from processed_tv_guide WHERE year = '2021' and month = '11' and day = '11' and channel_key ='teve2'"
            df = wr.athena.read_sql_query(query, database=dbname, ctas_approach=False, unload_approach=True,
                                          s3_output=f"s3://{bucket}/data/", boto3_session=athena_session)
        except Exception as e:
            return str(e)

        return "success"


if __name__ == '__main__':
    app.run()
