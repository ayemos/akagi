import boto3

client = boto3.client('s3')
p = client.get_paginator('list_objects_v2')

r = p.paginate(
        MaxKeys=1000,
        Bucket='research.ap-northeast-1',
        Delimiter='/',
        Prefix='datasets/574_foods/573/')

for o in r:
    print(o)
