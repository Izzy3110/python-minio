from wyl.minio import MinioClient

minio_client = MinioClient()


if __name__ == '__main__':
    minio_client.do_pdf_upload(pdf_filepath="sample.pdf", object_name="sample.pdf")
