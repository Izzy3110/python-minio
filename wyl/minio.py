import datetime
import json
import os
import minio
from dotenv import load_dotenv
from minio import S3Error, Minio
from minio.versioningconfig import VersioningConfig

from wyl import response_headers_to_json

load_dotenv('.env.minio')

# MinIO Server Configuration
MINIO_ENDPOINT = "localhost:9010"  # Change to your MinIO server
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('ACCESS_SECRET')
BUCKET_NAME = os.getenv('BUCKET_NAME')


class MinioClient:
    def __init__(self, bucket_name=BUCKET_NAME):
        self.bucket_name = bucket_name
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False  # Change to False if not using HTTPS
        )

        # Check if bucket exists, create if not
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)
            print(f"Bucket {self.bucket_name} created.")

        config = self.client.get_bucket_versioning(self.bucket_name)
        if config.status != "Enabled":
            self.client.set_bucket_versioning(self.bucket_name, VersioningConfig())
            config = self.client.get_bucket_versioning(self.bucket_name)
            print(f"Versioning enabled: {config.status}")
        else:
            print(f"Versioning is already enabled for bucket {self.bucket_name}")

        self.objects_dirpath = os.path.join(os.getcwd(), "objects")
        self.buckets_dirpath = os.path.join(self.objects_dirpath, bucket_name)
        if not os.path.isdir(self.objects_dirpath):
            os.makedirs(self.objects_dirpath, exist_ok=True)
        if not os.path.isdir(self.buckets_dirpath):
            os.makedirs(self.buckets_dirpath, exist_ok=True)

    def list_buckets(self):
        bucket_list = []
        current_buckets = self.client.list_buckets()
        for bucket in current_buckets:
            bucket_list.append({
                bucket.name: {
                    "creation_date": bucket.creation_date
                }
            })
        return bucket_list

    def list_objects(self):
        object_list = []
        try:
            current_objects = self.client.list_objects(self.bucket_name, include_version=True)
            for obj in current_objects:
                self.get_object_by_version(obj.object_name, obj.version_id, object_etag=obj.etag)
                object_list.append({

                    obj.object_name: {
                        "last_modified": obj.last_modified.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3],
                        "etag": obj.etag,
                        "size": obj.size,
                        "object_name": obj.object_name,
                        "version_id": obj.version_id
                    }
                })
            return object_list

        except minio.error.S3Error as e:
            print(f"Error: {e}")
            pass

    def get_object_by_version(self, object_name, version_id, object_etag=None, target_filename=None):
        try:
            # Get the object by version ID
            response = self.client.get_object(self.bucket_name,
                                              object_name,
                                              version_id=version_id
                                              )

            # Read the object's content (you can handle it as you need)
            data = response.read()

            object_root_dirpath = os.path.join(self.buckets_dirpath, object_name)
            if not os.path.isdir(object_root_dirpath):
                os.makedirs(object_root_dirpath, exist_ok=True)

            file_name, file_ext = os.path.splitext(object_name)
            if target_filename is not None:
                new_filename = target_filename
            else:
                if object_etag is not None:
                    new_filename = f"{file_name}_{version_id}{file_ext}"
                else:
                    new_filename = f"{file_name}_{version_id}{file_ext}"

            target_filepath = os.path.join(object_root_dirpath, new_filename)
            if os.path.isfile(target_filepath):
                _stat_file = os.stat(target_filepath)
                ts_ = datetime.datetime.fromtimestamp(_stat_file.st_mtime)
                _last_mod_file_ = ts_.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

                # parsed_date = datetime.datetime.strptime(last_mod, "%d.%m.%Y %H:%M:%S.%f")
                # print(parsed_date.strftime("%d.%m.%Y %H:%M:%S"))
                # print(parse_date(response.headers.get('Last-Modified')))
                # print(_last_mod_file_)
            else:
                self.write_response_headers_file(new_filename, object_root_dirpath, response)
                with open(target_filepath, "wb") as file:
                    file.write(data)

        except S3Error as e:
            print(f"An error occurred: {e}")

    def upload_pdf(self, pdf_filepath, object_name, bucket_name, content_type="application/pdf"):
        try:
            # Upload PDF file (MinIO will automatically handle versioning)
            self.client.fput_object(bucket_name=bucket_name, object_name=object_name, file_path=pdf_filepath,
                                    content_type=content_type)
            print(f"File {pdf_filepath} uploaded successfully as {object_name}.")

        except S3Error as e:
            print(f"MinIO error: {e}")

    @staticmethod
    def write_response_headers_file(filename, base_dir, response):
        new_filename_meta, new_filename_meta_ext = os.path.splitext(filename)
        headers_json_filename = f"{new_filename_meta}.json"
        target_filepath_headers = os.path.join(base_dir, headers_json_filename)
        headers_json = response_headers_to_json(response.headers)
        with open(target_filepath_headers, "w") as json_header_file:
            json_header_file.write(json.dumps(headers_json, indent=4))
