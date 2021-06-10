import boto3
from botocore.exceptions import NoCredentialsError

#Credentials AWS


# UPLOAD TO AWS  
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


#uploaded = upload_to_aws('/home/arun/Documents/projectpyspark/scripts/geo1.csv', 'mybucket-147', 'geo1.csv')

print('hi this is written from distance')

###Download from AWS
def download_file( bucket, file_todownload, path_todownload):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.download_file( bucket, file_todownload, path_todownload)
        print("Download Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

downlaod_to_aws = download_file('mybucket-147','date2008_pipe.txt','/home/arun/Documents/projectpyspark/scripts/date2008_pipe.txt')
