import boto3

r = boto3.resource('s3')
objs = r.Bucket('research.ap-northeast-1').objects.filter(Prefix='datasets/574_foods')

for obj in objs:
    print(obj)
