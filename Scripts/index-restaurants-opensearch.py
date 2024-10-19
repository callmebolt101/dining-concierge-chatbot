import boto3
import json

# Initialize clients for Cognito and DynamoDB
client = boto3.client('cognito-idp')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserTokens') 

def lambda_handler(event, context):
    # Parse the request body to get email, password, and new_password
    body = json.loads(event.get('body', '{}'))
    email = body.get('email')
    password = body.get('password')
    new_password = body.get('new_password')
    
    if not email or not password:
        return {
            'statusCode': 400,
            'body': json.dumps("Missing email or password")
        }
    
    try:
        # Step 1: Initiate Auth (Basic Authentication)
        response = client.admin_initiate_auth(
            UserPoolId='us-east-1_HHPuBJHW5', 
            ClientId='4a3juhquj1upb0v0t3p9drfam5', 
            AuthFlow='ADMIN_NO_SRP_AUTH',
            AuthParameters={
                'USERNAME': email,
                'PASSWORD': password
            }
        )

        # Check if a NEW_PASSWORD_REQUIRED challenge is triggered
        if 'ChallengeName' in response and response['ChallengeName'] == 'NEW_PASSWORD_REQUIRED':
            # Step 2: Respond to the NEW_PASSWORD_REQUIRED challenge
            session = response['Session']
            challenge_response = client.admin_respond_to_auth_challenge(
                UserPoolId='us-east-1_HHPuBJHW5',
                ClientId='4a3juhquj1upb0v0t3p9drfam5',
                ChallengeName='NEW_PASSWORD_REQUIRED',
                Session=session,
                ChallengeResponses={
                    'USERNAME': email,
                    'NEW_PASSWORD': new_password
                }
            )
            
            # Return the authentication token after handling the challenge
            token = challenge_response['AuthenticationResult']['IdToken']
            
            # Step 3: Store the token in DynamoDB
            store_token_in_dynamodb(email, token)
            
            return {
                'statusCode': 200,
                'body': token
            }

        # If no challenge is required, just return the authentication token
        elif 'AuthenticationResult' in response:
            token = response['AuthenticationResult']['IdToken']
            
            # Store the token in DynamoDB
            store_token_in_dynamodb(email, token)
            
            return {
                'statusCode': 200,
                'body': token
            }
        else:
            return {
                'statusCode': 400,
                'body': f"Unexpected response: {response}"
            }
        
    except Exception as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }


# Function to store the token in DynamoDB
def store_token_in_dynamodb(email, token):
    try:
        table.put_item(
            Item={
                'email': email,
                'token': token
            }
        )
        print(f"Stored token for {email} in DynamoDB.")
    except Exception as e:
        print(f"Error storing token in DynamoDB: {str(e)}")
import json
import boto3
from boto3.dynamodb.conditions import Attr
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import logging

# Initialize DynamoDB and OpenSearch clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# OpenSearch configuration
host = 'search-restaurants-dfpgb2gmqeqgcil7uat7vmjima.us-east-1.es.amazonaws.com'
es = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to fetch restaurants by cuisine and location
def fetch_restaurants(cuisine, location='Manhattan', limit=50):
    try:
        results = []
        # Scan DynamoDB for restaurants with matching Cuisine and Location
        response = table.scan(
            FilterExpression=Attr('Cuisine').eq(cuisine) & Attr('Address').contains(location),
            Limit=limit
        )
        results.extend(response.get('Items', []))
        
        while 'LastEvaluatedKey' in response and len(results) < limit:
            response = table.scan(
                FilterExpression=Attr('Cuisine').eq(cuisine) & Attr('Address').contains(location),
                ExclusiveStartKey=response['LastEvaluatedKey'],
                Limit=limit - len(results) 
            )
            results.extend(response.get('Items', []))
        
        return results
    except Exception as e:
        logger.error(f"Error fetching {cuisine} restaurants from DynamoDB: {str(e)}")
        return []

# Function to index restaurants in OpenSearch
def index_in_opensearch(restaurants, cuisine):
    for restaurant in restaurants:
        document = {
            'RestaurantID': restaurant['BusinessID'],
            'Cuisine': cuisine
        }
        try:
            es.index(index='restaurants', body=document)
            logger.info(f"Successfully indexed {restaurant['BusinessID']} for {cuisine}")
        except Exception as e:
            logger.error(f"Error indexing {restaurant['BusinessID']} in OpenSearch: {str(e)}")

def lambda_handler(event, context):
    cuisines = ['Indian', 'Chinese', 'Italian']
    
    for cuisine in cuisines:
        logger.info(f"Fetching {cuisine} restaurants in Manhattan.")
        # Fetch 50 restaurants for each cuisine from DynamoDB
        restaurants = fetch_restaurants(cuisine, location='Manhattan', limit=50)
        
        # Index restaurants in OpenSearch (storing only RestaurantID and Cuisine)
        if restaurants:
            index_in_opensearch(restaurants, cuisine)
            logger.info(f"Indexed {len(restaurants)} {cuisine} restaurants in OpenSearch.")
        else:
            logger.warning(f"No restaurants found for {cuisine}.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully indexed restaurants in OpenSearch.')
    }
