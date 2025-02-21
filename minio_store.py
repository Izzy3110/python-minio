from wyl.generate_samples import generate_pdf
from wyl.minio import MinioClient


debug = True


minio_client = MinioClient()
buckets = minio_client.list_buckets()
objects = minio_client.list_objects()

if debug:
    print(" -- objects")
    for object_ in objects:
        print(object_)

    print("")
    print(" -- buckets")
    print(buckets)


def do_pdf_upload():
    PDF_FILE_PATH = "sample.pdf"
    OBJECT_NAME = "sample.pdf"  # Name in the bucket
    generate_pdf(PDF_FILE_PATH)
    minio_client.upload_pdf(pdf_filepath=PDF_FILE_PATH, object_name=OBJECT_NAME, bucket_name=minio_client.bucket_name)


if __name__ == '__main__':
    do_pdf_upload()
