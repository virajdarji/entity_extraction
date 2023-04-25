import boto3
import logging
import os

import pdf2image
from chalice import Chalice, Response
from io import BytesIO
from pdf2image import convert_from_bytes

app = Chalice(app_name='pdf2image')


DPI = 300

_SUPPORTED_IMAGE_EXTENSIONS = ["ppm", "jpeg", "png", "tiff"]
FMT = "jpeg"

_SUPPORTED_FILE_EXTENSION = '.pdf'


#@app.on_s3_event(bucket=ORIGIN_BUCKET,events=['s3:ObjectCreated:*'])
def lambda_handler(event, context):
    """Take a pdf fom an S3 bucket and convert it to a list of pillow images (one for each page of the pdf).
    :param event: A Lambda event (referring to an S3 event object created event).
    :return:
    """
    print(event)
    ORIGIN_BUCKET=event["Records"][0]["s3"]["bucket"]["name"]
    key=event["Records"][0]["s3"]["object"]["key"]
    DESTINATION_BUCKET=ORIGIN_BUCKET
    print(2)
    # Fetch the image bytes
    s3 = boto3.resource('s3')
    obj = s3.Object(event["Records"][0]["s3"]["bucket"]["name"], event["Records"][0]["s3"]["object"]["key"])
    infile = obj.get()['Body'].read()
    logging.info("Successfully retrieved S3 object.")
    print(3)

    # Set poppler path
    poppler_path = "/var/task/lib/poppler-utils-0.26/usr/bin"
    images = convert_from_bytes(infile,
                                dpi=DPI,
                                fmt=FMT)
    logging.info("Successfully converted pdf to image.")
    print(2)

    for page_num, image in enumerate(images):

        # The directory is: image/
        directory = "image/" 
        
        # Then save the image and name it: <name of the pdf>-page<page number>.FMT
        location = directory +  event["Records"][0]["s3"]["object"]["key"].split('.')[0].split('/')[1] + "-page" + str(page_num+1) + '.' + FMT

        logging.info(f"Saving page number {str(page_num)} to S3 at location: {DESTINATION_BUCKET}, {location}.")
        print(3)

        # Load it into the buffer and save the boytjie to S3
        buffer = BytesIO()
        image.save(buffer, FMT.upper())
        buffer.seek(0)
        s3.Object(
            DESTINATION_BUCKET,
            location
        ).put(
            Body=buffer,
            Metadata={
                'ORIGINAL_DOCUMENT_BUCKET': event["Records"][0]["s3"]["bucket"]["name"],
                'ORIGINAL_DOCUMENT_KEY': event["Records"][0]["s3"]["object"]["key"],
                'PAGE_NUMBER': str(page_num),
                'PAGE_COUNT': str(len(images))
            }
        )
        print(4)

    return ("pdf to jpg")
