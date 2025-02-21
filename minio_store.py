import datetime
import json
import os
import minio
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from minio.versioningconfig import VersioningConfig
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

load_dotenv('.env.minio')

# MinIO Server Configuration
MINIO_ENDPOINT = "localhost:9010"  # Change to your MinIO server
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('ACCESS_SECRET')
BUCKET_NAME = os.getenv('BUCKET_NAME')

debug = True

# Initialize MinIO client
client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False  # Change to False if not using HTTPS
)

objects_dirpath = os.path.join(os.getcwd(), "objects")
buckets_dirpath = os.path.join(objects_dirpath, BUCKET_NAME)
if not os.path.isdir(objects_dirpath):
    os.makedirs(objects_dirpath, exist_ok=True)
if not os.path.isdir(buckets_dirpath):
    os.makedirs(buckets_dirpath, exist_ok=True)


def list_buckets():
    bucket_list = []
    current_buckets = client.list_buckets()
    for bucket in current_buckets:
        bucket_list.append({
            bucket.name: {
                "creation_date": bucket.creation_date
            }
        })
    return bucket_list


def list_objects():
    object_list = []
    try:
        current_objects = client.list_objects(BUCKET_NAME, include_version=True)
        for obj in current_objects:
            get_object_by_version(obj.object_name, obj.version_id, object_etag=obj.etag,
                                  last_mod=obj.last_modified.strftime("%d.%m.%Y %H:%M:%S.%f")[:-3])
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


def parse_date(date_str):
    date_format = "%a, %d %b %Y %H:%M:%S GMT"
    parsed_date = datetime.datetime.strptime(date_str, date_format)
    return parsed_date.strftime("%d.%m.%Y %H:%M:%S")


def response_headers_to_json(response_headers) -> dict:
    headers_json = {}
    for header_key, header_item in response_headers.items():
        headers_json[header_key] = header_item.lstrip('"').rstrip('"')
    return headers_json


def write_response_headers_file(filename, base_dir, response):
    new_filename_meta, new_filename_meta_ext = os.path.splitext(filename)
    headers_json_filename = f"{new_filename_meta}.json"
    target_filepath_headers = os.path.join(base_dir, headers_json_filename)
    headers_json = response_headers_to_json(response.headers)
    with open(target_filepath_headers, "w") as json_header_file:
        json.dump(fp=json_header_file, obj=headers_json, indent=4)


def get_object_by_version(object_name, version_id, object_etag=None, target_filename=None, last_mod=None):
    try:
        # Get the object by version ID
        response = client.get_object(BUCKET_NAME,
                                     object_name,
                                     version_id=version_id
                                     )

        # Read the object's content (you can handle it as you need)
        data = response.read()

        object_root_dirpath = os.path.join(buckets_dirpath, object_name)
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
            write_response_headers_file(new_filename, object_root_dirpath, response)
            with open(target_filepath, "wb") as file:
                file.write(data)

    except S3Error as e:
        print(f"An error occurred: {e}")


def generate_pdf(filename):
    # Create a canvas object to generate the PDF
    c = canvas.Canvas(filename, pagesize=letter)

    # Set font and size for text
    c.setFont("Helvetica", 12)

    # Add some text to the PDF
    c.drawString(100, 750, "Hello, this is a sample PDF created with ReportLab!")
    now_date = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
    c.drawString(100, 730, f"This is another line of text. {now_date}")

    # Save the PDF to file
    c.save()


def upload_pdf(pdf_filepath, object_name, content_type="application/pdf"):
    try:
        # Upload PDF file (MinIO will automatically handle versioning)
        client.fput_object(bucket_name=BUCKET_NAME, object_name=object_name, file_path=pdf_filepath,
                           content_type=content_type)
        print(f"File {pdf_filepath} uploaded successfully as {object_name}.")

    except S3Error as e:
        print(f"MinIO error: {e}")


buckets = list_buckets()
objects = list_objects()

# Check if bucket exists, create if not
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} created.")

if debug:
    print(" -- objects")
    for object_ in objects:
        print(object_)

    print("")
    print(" -- buckets")
    print(buckets)

# Check and enable versioning if not enabled
config = client.get_bucket_versioning(BUCKET_NAME)
if config.status != "Enabled":
    client.set_bucket_versioning(BUCKET_NAME, VersioningConfig(Enabled=True))
    config = client.get_bucket_versioning(BUCKET_NAME)
    print(f"Versioning enabled: {config.status}")
else:
    print(f"Versioning is already enabled for bucket {BUCKET_NAME}")


def do_pdf_upload():
    PDF_FILE_PATH = "sample.pdf"
    OBJECT_NAME = "sample.pdf"  # Name in the bucket
    generate_pdf(PDF_FILE_PATH)
    upload_pdf(pdf_filepath=PDF_FILE_PATH, object_name=OBJECT_NAME)


if __name__ == '__main__':
    do_pdf_upload()
