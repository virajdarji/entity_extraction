import json

import boto3
import io
from PIL import Image              
import json
import os



def process_text_detection(bucket, document, region):
    
    # Get the document from Amazon S3
    s3_connection = boto3.resource("s3")
    print(4)
    print(bucket)
    print(document)

    # Connect to Amazon Textract to detect text in the document
    client = boto3.client("textract", region_name=region)

    # Get the response from Amazon S3
    s3_object = s3_connection.Object(bucket, document)
    s3_response = s3_object.get()
    print(1)

    # open binary stream using an in-memory bytes buffer
    stream = io.BytesIO(s3_response['Body'].read())

    # load stream into image
    image = Image.open(stream)
    print(2)
    # process using Amazon S3 object
    response = client.detect_document_text(Document={'S3Object': {'Bucket': bucket, 'Name': document}})
    print(3)

    # Get the text blocks
    blocks = response['Blocks']

    # List to store image lines in document
    line_list = []
    print(5)
    # Create image showing bounding box/polygon around the detected lines/text
    for block in blocks:
        if block["BlockType"] == "LINE":
            line_list.append(block["Text"])

    return line_list

def entity_detection(lines,  region):
    
    # Create a list to hold the entities found for every line
    response_entities = []
    
    
    # Connect to Amazon Comprehend
    comprehend = boto3.client(service_name='comprehend', region_name=region)

    
    # Iterate through the lines in the list of lines
    for line in lines:

        # construct a list to hold all found entities for a single line
        entities_list = []

        # Call the DetectEntities operation and pass it a line from lines
        found_entities = comprehend.detect_entities(Text=line, LanguageCode='en')
        for response_data, values in found_entities.items():
            for item in values:
                if "Text" in item:
                    print("Entities found:")
                    for text, val in item.items():
                        if text == "Text":
                            # Append the found entities to the list of entities
                            entities_list.append(val)
                            print(val)
        # Add all found entities for this line to the list of all entities found
        response_entities.append(entities_list)

    return response_entities

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.resource('s3')
    obj = s3.Object(event["Records"][0]["s3"]["bucket"]["name"], event["Records"][0]["s3"]["object"]["key"])
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    document = event["Records"][0]["s3"]["object"]["key"]
    region_name = 'us-east-1'
    lines = process_text_detection(bucket, document, region_name)
    print('Calling DetectEntities:')
    print("------")
    response_ents = entity_detection(lines, region_name)
    print("Text found: " + str(lines))
    entities_dict = {"Lines":lines, "Entities":response_ents}
    print(entities_dict)
    entities_dict = {"Lines":lines, "Entities":response_ents}
    file = "json/" + event["Records"][0]["s3"]["object"]["key"].split('.')[0].split('/')[1] + '.' + 'json'
    s3.Object(
            event["Records"][0]["s3"]["bucket"]["name"],
            file
        ).put(
            Body=json.dumps(entities_dict)
        )
    return ("textract and comprehend")