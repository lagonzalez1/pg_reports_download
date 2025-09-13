import boto3
from botocore.exceptions import BotoCoreError, ClientError
from io import StringIO
from typing import Optional
import pandas as pd

## Asuuming the base role for CLI
s3 = boto3.client('s3')

class S3Instance:
    def __init__(self, bucket):
        self.bucket = bucket    

    def put_object(self, key, df: Optional[pd.DataFrame])-> bool:
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            print(f"Uploading to s3 with key {key}")
            s3.put_object(
                Bucket=self.bucket,
                Key=str("reports/"+ key),
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            return True
        except (BotoCoreError, ClientError) as e:
            return False
    


    