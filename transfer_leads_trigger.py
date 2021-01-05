import json
import boto3
import uuid


def lambda_handler(event, context):
    data = json.loads(event['body'])
    if (data['host'][0]!='chromeriver.my.salesforce.com') and (data['host'][0]!='chromeriver--isell.na148.visual.force.com'):
        raise Exception(f"invalid host: {data['host'][0]}")
    s3 = boto3.client('s3')
    s3.put_object(
     Bucket='hamster-storage-prod',
     Key='transfer-leads-trigger/'+str(uuid.uuid1()),
     Body=json.dumps(data)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
