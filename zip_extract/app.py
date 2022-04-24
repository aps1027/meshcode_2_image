import os
import zipfile
from concurrent import futures
from io import BytesIO
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print('Start Unzipping Files...')
    # Parse and prepare required items from event
    global bucket, path, zipdata
    event = next(iter(event['Records']))
    bucket = event['s3']['bucket']['name']
    key = event['s3']['object']['key']
    # To change destination path (e.g. catalog)
    path = 'catalog'
    # Fetch and load target file
    s3_resource = boto3.resource('s3')
    zip_obj = s3_resource.Object(bucket_name=bucket, key=key)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    zipdata = zipfile.ZipFile(buffer)
    # Call action method with using ThreadPool
    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_list = [
            executor.submit(extract, filename)
            for filename in zipdata.namelist()
        ]
    result = {'success': [], 'fail': []}
    for future in future_list:
        filename, status = future.result()
        result[status].append(filename)
    # Remove extracted archive file
    # s3.delete_object(Bucket=bucket, Key=key)  
    print('Finished Unzipping Files!!!')
    return result
    
def extract(filename):
    upload_status = 'success'
    # to check uploaded files
    # print(os.path.join(path, filename))
    try:
    # upload unzip file to S3
        s3.upload_fileobj(
            BytesIO(zipdata.read(filename)),
            bucket,
            os.path.join(path, filename)
        )
    except Exception as exception:
        upload_status = 'fail'
        print('fail')
        print(exception)
    finally:
        return filename, upload_status