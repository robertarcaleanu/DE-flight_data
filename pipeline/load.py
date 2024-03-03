import polars as pl
import boto3

def load_parquet(data: pl.DataFrame, path: str) -> None:
    """This function loads the transformed data into a CSV.

    Args:
        data (pl.DataFrame): dataframe to save
        path (str): path to save
    """

    data.write_parquet(path)

def load_s3(AWS_REGION: str, AWS_ACCESS_KEY: str, AWS_SECRET_KEY: str,
            AWS_S3_BUCKET_NAME: str, LOCAL_FILE: str, NAME_FOR_S3: str) -> None:
    """This function saves the file in a S3 bucket in AWS.

    Args:
        AWS_REGION (str): aws region
        AWS_ACCESS_KEY (str): access key
        AWS_SECRET_KEY (str): secret key 
        AWS_S3_BUCKET_NAME (str): s3 bucket name
        LOCAL_FILE (str): path of the file in local
        NAME_FOR_S3 (str): path of the file to save in s3
    """
    
    s3_client = boto3.client(
        service_name='s3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    response = s3_client.upload_file(LOCAL_FILE, AWS_S3_BUCKET_NAME, NAME_FOR_S3)

    print(f'upload_log_to_aws response: {response}')