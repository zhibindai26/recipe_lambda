from io import StringIO
import requests
import boto3
import pandas as pd

RECIPE_CSV = "recipes.csv"
S3_BUCKET = "zd-basic-bucket"


def get_df():
    """ Download recipe CSV from S3 and convert it to a Dataframe """

    s3 = boto3.resource('s3')
    response = s3.get_object(BUCKET=S3_BUCKET, KEY=RECIPE_CSV)

    recipe_csv = response["Body"]

    df = pd.read_csv(recipe_csv, index_col=0)
    
    return df


def write_df_to_csv_on_s3(dataframe):
    """ Write a dataframe to a CSV on S3 """

    # Create buffer
    csv_buffer = StringIO()

    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep=",")

    # Create S3 object
    s3 = boto3.resource("s3")

    # Write buffer to S3 object
    s3.Object(S3_BUCKET, RECIPE_CSV).put(Body=csv_buffer.getvalue())


