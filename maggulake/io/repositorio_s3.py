import boto3


class RepositorioS3:
    def __init__(self, path: str, stage: str = 'dev'):
        self.bucket = self.bucket_name(stage)
        self.path = path
        self.client = boto3.client('s3')

    # TODO: adicionar return type hints em s3_url, da_ultima_pasta e ultima_pasta.
    def s3_url(self):
        return f's3://{self.bucket_name}/{self.path}'

    @classmethod
    def da_ultima_pasta(cls, base_path: str, stage: str = 'dev'):
        path = cls.ultima_pasta(base_path, stage)

        return cls(path, stage)

    @staticmethod
    def ultima_pasta(base_path: str, stage: str = 'dev') -> str:
        client = boto3.client('s3')
        response = client.list_objects_v2(
            Bucket=RepositorioS3.bucket_name(stage), Prefix=base_path, Delimiter='/'
        )

        folders = [i['Prefix'] for i in response['CommonPrefixes']]
        folders.sort()
        ultima = folders[-1]

        return ultima

    @staticmethod
    def bucket_name(stage: str) -> str:
        return f'maggu-datalake-{stage}'
