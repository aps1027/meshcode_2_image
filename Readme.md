# AWS SAM Tutorial for S3 Trigger Events

There are 3 lambda functions
1. csv data (meshcode) to json data
2. json data to image
3. zip extraction

## Prerequisites
1. Create AWS Account
1. Install `aws-cli`
1. Install `sam-cli`

## Deployment
```
# building before deploy
$ sam build

# deployment
$ sam deploy --guided
```

## For csv_2_json
1. add trigger event in your s3 bucket
1. add prefix folder `csv/`
1. add suffix file `.csv`

## For json_2_image
1. add trigger event in your s3 bucket
1. add prefix folder `json/`
1. add suffix file `.json`

## For zip_extract
1. add trigger event in your s3 bucket
1. add prefix folder `upload/`
1. add suffix file `.zip`