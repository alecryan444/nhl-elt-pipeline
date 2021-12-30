import awswrangler as wr
import boto3

def upload_json_to_s3(df, bucket, subfolder, partition_cols):
    if df.empty == False:
        #S3
        session = boto3.Session(profile_name='aryan')
        write_path = f's3://{bucket}/{subfolder}/'
        wr.s3.to_json(df, write_path, mode = 'overwrite_partitions', partition_cols = partition_cols, dataset=True, boto3_session = session)
    else:
        print("Dataframe is empty. Nothing to load.")