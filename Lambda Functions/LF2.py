import random
import json
import boto3
import decimal
import time
import datetime

# from botocore.vendored import requests
from boto3.dynamodb.conditions import Key, Attr
import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("yelp-restaurants")


def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()
    region = "us-east-1"
    service = "es"
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )
    sqsclient = boto3.client("sqs")
    queue_url = "https://sqs.us-east-1.amazonaws.com/145006476918/DiningOrderQueue"

    # Receive message from SQS queue
    response = sqsclient.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["SequenceNumber"],
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        VisibilityTimeout=0,
        WaitTimeSeconds=1,
    )

    if "Messages" in response:
        es_endpoint = (
            "search-restaurants-5eka23r4rdwvptk7gjl7yyp7ha.us-east-1.es.amazonaws.com"
        )

        es = Elasticsearch(
            hosts=[{"host": es_endpoint, "port": 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )

        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.Table("yelp-restaurants")

        for message in response["Messages"]:
            receipt_handle = message["ReceiptHandle"]
            req_attributes = message["MessageAttributes"]

            # Get the food category from queue message attributes.
            index_category = req_attributes["Categories"]["StringValue"]
            cuisine = req_attributes["Categories"]["StringValue"]
            phone = req_attributes["Phone"]["StringValue"]
            number = req_attributes["PeopleNum"]["StringValue"]
            time = req_attributes["DiningTime"]["StringValue"]
            location = req_attributes["Location"]["StringValue"]
            date = req_attributes["DiningDate"]["StringValue"]

            k = es.search(
                index="restaurants",
                doc_type="Restaurant",
                body={"query": {"match": {"categories.title": index_category}}},
                size=3,
            )

            restaurantIds = []
            ans = []
            res = json.dumps(k)
            for i in k["hits"]["hits"]:
                print(i["_id"])
                result = table.get_item(
                    Key={
                        "id": i["_id"],
                    }
                )
                ans.append(result)
            print(ans)

            message = (
                "Hey! Please check the "
                + str(cuisine)
                + " restaurant suggestions for "
                + str(number)
                + " people, for "
                + str(date)
                + "at"
                + str(time)
                + ":"
                + "\n"
            )
            for i in range(0, 3):
                response = ans[i]
                msg = (
                    str(i + 1)
                    + ")"
                    + str(response["Item"]["name"])
                    + " located at "
                    + str(response["Item"]["address"])
                )
                message = message + msg + "\n"
            message = message + "Enjoy your meal!"
            print(message)
            sns = boto3.client("sns")

            response = sns.publish(
                TopicArn="arn:aws:sns:us-east-1:145006476918:email",
                Message=message,
            )
            # status1 = sms_client.publish(Message=message,MessageStructure='string',PhoneNumber = '+19293932964')
            sqsclient.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

    else:
        return {
            "statusCode": 500,
            "body": json.dumps("Error fetching data from the queue."),
        }

    return {"statusCode": 200, "body": ans}
