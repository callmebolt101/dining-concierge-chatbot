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
