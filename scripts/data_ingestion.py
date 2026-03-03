# Part 3: Data Ingestion & Storage (Python + Boto3)
# Tasks
# 1. Upload raw CSV dataset from local system to S3
# 2. Organize data in S3 using folder structure (e.g., /raw/, /processed/)
# 3. List and download files from S3 using Python
# 4. Explain how older or infrequently accessed data can be archived to Amazon Glacier
import logging

from services.s3_services import S3FileService
from config import settings

def main():
    logging.basicConfig(level=logging.INFO)
    file_path = 'data/sales_dataset.csv'
    s3_key = 'raw/sales_dataset.csv'
    
    # Create S3FileService object
    s3 = S3FileService(settings.AWS_REGION)
    
    # upload file to s3
    s3.upload_file_to_s3(
        file_path,
        settings.BUCKET_NAME,
        s3_key
        )
    
    # List all files in s3 bucket
    s3.list_files_in_s3_bucket(settings.BUCKET_NAME)
    
    # download file from s3 
    s3.download_file_from_s3(
        s3_key,
        settings.BUCKET_NAME,
        settings.DOWNLOAD_DIR
    )
    
if __name__ == '__main__':
    main()
    
# Explaination
# 1. Explain how older or infrequently accessed data can be archived to 
# Amazon Glacier
# Ans: We configure S3 lifecycle policies to automatically transition 
#   objects to Glacier storage classes based on object age. 
#   For example, after 90 days, objects move to Glacier Flexible 
#   Retrieval. This helps reduce storage costs while maintaining 
#   durability. Data can later be restored if needed.

