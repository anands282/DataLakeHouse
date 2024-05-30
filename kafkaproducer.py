import io
from minio import Minio
from minio_config import config
from minio.error import S3Error


def produce():
    minio_object = Minio(
        'localhost:8000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )
    bucket_name = 'bronze_zone'
    object_list = minio_object.list_objects(bucket_name, recursive=True)

    url_list = [minio_object.get_presigned_url('GET',bucket_name, obj.object_name) for obj in object_list if 'stock_price_file' in obj.object_name ]




if __name__ == "__main__":
    try:
        produce()
    except S3Error as s3e:
        print("S3 Error. Exit process. ", s3e)

