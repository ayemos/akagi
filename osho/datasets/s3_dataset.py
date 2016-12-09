from osho.dataset import Dataset
from osho.s3_helper import S3Helper


class S3Dataset(Dataset):
    @classmethod
    def by_prefix(self, bucket, prefix):
        keys = [obj.key for obj in S3Helper.resource().Bucket(bucket).objects
                .filter(Prefix=prefix)]

        return S3Dataset(bucket, keys)

    def __init__(self, bucket, keys):
        super(S3Dataset, self).__init__()

        self.bucket = bucket
        self.keys = keys

    def get_length(self):
        return len(self.keys)

    def get_example(self, i):
        # XXX: handle ClientError ?
        data = S3Helper.resource().Object(self.bucket, self.keys[i]).get()['Body'].read()

        return data
