import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Lex client
lex_client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract user message from the request
        body = json.loads(event['body'])
        user_message = body['messages'][0]['unstructured']['text']
        session_id = '0937741d-6a93-4e6b-9b6'

        logger.info(f"User message: {user_message}, Session ID: {session_id}")

        # Call Lex chatbot to process the user message
        lex_response = lex_client.recognize_text(
            botId='NQ3LIN7QMZ',
            botAliasId='TSTALIASID',
            localeId='en_US',
            sessionId='0937741d-6a93-4e6b-9b60',
            text=user_message
        )

        logger.info(f"Lex response: {json.dumps(lex_response)}")

        # Extract the Lex response
        if 'messages' in lex_response and len(lex_response['messages']) > 0:
            response_text = lex_response['messages'][0]['content']
        else:
            response_text = "I'm sorry, I couldn't understand that."

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        response_text = "Oops! Something went wrong. Please try again."

    # Format the response message
    response_message = {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "text": response_text
                }
            }
        ],
        'sessionId': session_id
    }

    # Return response with CORS headers
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
        },
        'body': json.dumps(response_message)
    }
