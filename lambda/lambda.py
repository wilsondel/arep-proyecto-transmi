import json
import boto3

s3 = boto3.resource('s3')
rekognition = boto3.client('rekognition')


def extractLabels(image, bucket):
    response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': image
            }
        }
    )
    return response


def extractLabelsFromVideo(video, bucket):
    response = rekognition.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': video
            }
        }
    )
    job_id = response['JobId']
    response = rekognition.get_label_detection(JobId=job_id)
    while True:
        response = rekognition.get_label_detection(JobId=job_id)
        if response['JobStatus'] in ['SUCCEEDED', 'FAILED']:
            break
    return response


def detectAllLabels(labels):
    detected_labels = []
    for label in labels:
        detected_labels.append(label['Name'])
    return detected_labels


def lambda_handler(event, context):
    # Obtener datos de la imagen o video subido a S3
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        file_extension = key.split('.')[-1].lower()
        print("THIS IS PRINTING", file_extension)
        # Analizar imagen o video según la extensión
        if file_extension in ['jpg', 'jpeg', 'png']:
            imageScan = extractLabels(key, bucket)
            labels = detectAllLabels(imageScan['Labels'])
            print(key, ":", labels)
        
        elif file_extension in ['mp4', 'mov', 'avi']:
            videoScan = extractLabelsFromVideo(key, bucket)
            for label_detection in videoScan['Labels']:
                label_name = label_detection['Label']['Name']
                detected_labels = detectAllLabels(label_detection['Label']['Instances'])
                print(key, ":", videoScan)
            print("THIS IS PRINTING", detected_labels)
        print("THIS IS PRINTING afterrr")
    return {
        'statusCode': 200,
    }
