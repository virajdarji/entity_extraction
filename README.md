# entity_extraction
This Project uses AWS to extracts entity from a document.

It is in 2 parts.
1. It converts pdf to jpg
2. Uses AWS Textract to extract text from jpg and then uses those text to extract entity using AWS Comprehend.

2 Lambdas are written to do above steps.
