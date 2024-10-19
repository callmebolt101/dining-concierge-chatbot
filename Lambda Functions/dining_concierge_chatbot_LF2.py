import json
import boto3
import requests
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Attr

sqs = boto3.client('sqs')
ses = boto3.client('ses')
dynamodb = boto3.resource('dynamodb')
history_table = dynamodb.Table('usersearchpreferences')
table = dynamodb.Table('yelp-restaurants')
region = 'us-east-1'
queue_url = 'https://sqs.us-east-1.amazonaws.com/975050055589/diningsuggestionsqueue'
open_search_endpoint = 'https://search-restaurants-dfpgb2gmqeqgcil7uat7vmjima.us-east-1.es.amazonaws.com/'
verified_email = 'abhishek.nitt101@gmail.com'

credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)

# Fetch restaurants from OpenSearch by cuisine
def fetch_restaurants_from_opensearch(cuisine):
    headers = {"Content-Type": "application/json"}
    query = {
        "size": 3,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }

    opensearch_url = f"{open_search_endpoint}/restaurants/_search"
    try:
        response = requests.get(opensearch_url, auth=aws_auth, headers=headers, data=json.dumps(query))

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} from OpenSearch")
            print(f"Response: {response.text}")
            return None

        response_json = response.json()
        print(f"OpenSearch response: {json.dumps(response_json)}")

        if 'hits' in response_json and response_json['hits']['total']['value'] > 0:
            return [hit['_source'] for hit in response_json['hits']['hits']]
        else:
            print("No hits found for the query")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from OpenSearch: {e}")
        return None

# Fetch restaurant details from DynamoDB by business ID
def fetch_restaurant_from_dynamodb(business_id):
    try:
        response = table.scan(
            FilterExpression=Attr('BusinessID').eq(business_id)
        )
        if 'Items' in response and len(response['Items']) > 0:
            return response['Items'][0]
        else:
            print(f"Restaurant with BusinessID {business_id} not found.")
            return None
    except Exception as e:
        print(f"Error fetching from DynamoDB: {e}")
        return None

# Send an email with restaurant recommendations
def send_email(recipient_email, subject, body_text):
    try:
        response = ses.send_email(
            Source=verified_email,
            Destination={
                'ToAddresses': [recipient_email]
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body_text
                    }
                }
            }
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending email: {e}")

# Handle incoming SQS messages, fetch restaurants, and send email recommendations
def lambda_handler(event, context):
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0
        )

        if 'Messages' not in response:
            print("No messages in the queue.")
            return {'statusCode': 200, 'body': json.dumps("No messages found in SQS queue")}

        for message in response['Messages']:
            try:
                message_body = json.loads(message['Body'])
                cuisine = message_body.get('cuisine') or message_body.get('Cuisine')
                recipient_email = message_body.get('email') or message_body.get('Email')
                location = message_body.get('location') or message_body.get('Location')
                dining_time = message_body.get('dining_time') or message_body.get('DiningTime')
                number_of_people = message_body.get('number_of_people') or message_body.get('NumberOfPeople')

                if not cuisine or not recipient_email:
                    print(f"Invalid message: {message_body}")
                    continue

                restaurants = fetch_restaurants_from_opensearch(cuisine)
                if not restaurants or len(restaurants) < 3:
                    print(f"Not enough restaurants found for {cuisine}")
                    continue

                restaurant_details_list = []
                restaurant_names = []
                for restaurant in restaurants:
                    restaurant_details = fetch_restaurant_from_dynamodb(restaurant.get('RestaurantID'))
                    if restaurant_details:
                        restaurant_details_list.append(restaurant_details)
                        restaurant_names.append(restaurant_details.get('Name', 'Unknown'))

                if len(restaurant_details_list) < 3:
                    print(f"Not enough restaurant details found for {cuisine}")
                    continue

                store_search_history(recipient_email, location, cuisine, dining_time, number_of_people, restaurant_names)

                subject = f"Your recommendations for {cuisine} cuisine are here"
                body_text = f"Hello! Here are my {cuisine} restaurant suggestions:\n\n"

                for idx, details in enumerate(restaurant_details_list):
                    body_text += f"{idx + 1}. {details.get('Name', 'Unknown')}, located at {details.get('Address', 'Unknown')}\n"

                body_text += "\nEnjoy your meal!"
                send_email(recipient_email, subject, body_text)

                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                
            except Exception as e:
                print(f"Error processing message {message}: {e}")
                continue

        return {'statusCode': 200, 'body': json.dumps(f"Recommendations sent for all valid messages in the queue")}
    
    except Exception as e:
        print(f"Error in lambda function: {e}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}

# Store user's search history in DynamoDB
def store_search_history(email, location, cuisine, dining_time, number_of_people, restaurant_names):
    try:
        history_table.put_item(
            Item={
                'email': email,
                'location': location,
                'cuisine': cuisine,
                'dining_time': dining_time,
                'number_of_people': number_of_people,
                'restaurant_names': restaurant_names
            }
        )
        print(f"Stored search history for {email}.")
    except Exception as e:
        print(f"Error storing search history: {str(e)}")
