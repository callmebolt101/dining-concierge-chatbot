import json
import boto3
from boto3.dynamodb.conditions import Key

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
history_table = dynamodb.Table('usersearchpreferences')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/975050055589/diningsuggestionsqueue'

SESSION_ID = '0937741d-6a93-4e6b-9b6' 

# Handle incoming Lex event and process dining suggestions
def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")
    
    session_id = SESSION_ID 
    print(f"Using session ID: {session_id}")
    
    intent_name = event['sessionState']['intent']['name']
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    slots = event['sessionState']['intent']['slots']

    # Handle GreetingIntent
    if intent_name == "GreetingIntent":
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': 'GreetingIntent',
                    'state': 'Fulfilled'
                }
            },
            'messages': [{
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'
            }]
        }
    
    # Handle ThankYouIntent
    elif intent_name == "ThankYouIntent":
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': 'ThankYouIntent',
                    'state': 'Fulfilled'
                }
            },
            'messages': [{
                'contentType': 'PlainText',
                'content': 'You are welcome! Feel free to ask anything else.'
            }]
        }

    # Handle DiningSuggestionsIntent
    if intent_name == "DiningSuggestionsIntent":
        email_slot = slots.get('Email', None)
        
        if email_slot is None or email_slot.get('value', {}).get('interpretedValue') is None:
            return elicit_slot('Email', slots, session_attributes, session_id)
        
        email = email_slot['value']['interpretedValue']
        previous_search = get_previous_search(email)
        
        if previous_search:
            confirmation_state = session_attributes.get('confirmation_state', 'not_asked')
            
            if confirmation_state == 'not_asked':
                session_attributes['confirmation_state'] = 'asked'
                return ask_for_confirmation(previous_search, slots, session_attributes, session_id)
            
            elif confirmation_state == 'asked':
                user_input = event.get('inputTranscript', '').lower()
                if user_input in ['yes', 'yeah', 'sure', 'okay']:
                    slots['Location'] = {'value': {'interpretedValue': previous_search['location']}}
                    slots['Cuisine'] = {'value': {'interpretedValue': previous_search['cuisine']}}
                    slots['Time'] = {'value': {'interpretedValue': previous_search['dining_time']}}
                    slots['Partysize'] = {'value': {'interpretedValue': previous_search['number_of_people']}}
                    
                    send_message_to_sqs(
                        previous_search['location'],
                        previous_search['cuisine'],
                        previous_search['dining_time'],
                        previous_search['number_of_people'],
                        email
                    )
                    
                    return {
                        'sessionState': {
                            'dialogAction': {
                                'type': 'Close'
                            },
                            'intent': {
                                'name': 'DiningSuggestionsIntent',
                                'state': 'Fulfilled'
                            },
                            'sessionAttributes': session_attributes,
                            'sessionId': session_id
                        },
                        'messages': [{
                            'contentType': 'PlainText',
                            'content': 'Thank you! You will receive an email with your previous dining suggestions shortly.'
                        }]
                    }
                else:
                    session_attributes['confirmation_state'] = 'denied'
                    return collect_new_slots(slots, session_attributes, session_id)

        return collect_new_slots(slots, session_attributes, session_id)

    return {
        'statusCode': 400,
        'body': json.dumps('Invalid intent')
    }

# Elicit a specific slot if it is not filled
def elicit_slot(slot_to_elicit, slots, session_attributes, session_id):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'slots': slots,
            },
            'sessionAttributes': session_attributes,
            'sessionId': session_id
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': f'Please provide your {slot_to_elicit}.'
        }]
    }

# Ask user to confirm if they want to use previous search data
def ask_for_confirmation(previous_search, slots, session_attributes, session_id):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': 'Location' 
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'slots': slots
            },
            'sessionAttributes': session_attributes,
            'sessionId': session_id
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': f"Your previous search was for {previous_search['location']} {previous_search['cuisine']} cuisine. Would you like to use the same search again? (Yes/No)"
        }]
    }

# Fetch user's previous search history from DynamoDB
def get_previous_search(email):
    try:
        response = history_table.get_item(Key={'email': email})
        return response.get('Item')
    except Exception as e:
        print(f"Error fetching previous search: {e}")
        return None

# Collect new slot values if previous search is not reused
def collect_new_slots(slots, session_attributes, session_id):
    required_slots = ['Location', 'Cuisine', 'Time', 'Partysize', 'Email']
    for slot in required_slots:
        if slots.get(slot) is None or slots[slot]['value']['interpretedValue'] == '':
            return elicit_slot(slot, slots, session_attributes, session_id)

    location = slots['Location']['value']['interpretedValue']
    cuisine = slots['Cuisine']['value']['interpretedValue']
    dining_time = slots['Time']['value']['interpretedValue']
    number_of_people = slots['Partysize']['value']['interpretedValue']
    email = slots['Email']['value']['interpretedValue']

    store_search_history(email, location, cuisine, dining_time, number_of_people)
    send_message_to_sqs(location, cuisine, dining_time, number_of_people, email)

    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'state': 'Fulfilled'
            },
            'sessionAttributes': session_attributes,
            'sessionId': session_id
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': 'Thank you! You will receive an email with new dining suggestions shortly.'
        }]
    }

# Store user's search history in DynamoDB
def store_search_history(email, location, cuisine, dining_time, number_of_people):
    try:
        history_table.put_item(
            Item={
                'email': email,
                'location': location,
                'cuisine': cuisine,
                'dining_time': dining_time,
                'number_of_people': number_of_people
            }
        )
        print(f"Stored search history for {email}.")
    except Exception as e:
        print(f"Error storing search history: {str(e)}")

# Send dining suggestion message to SQS queue
def send_message_to_sqs(location, cuisine, dining_time, number_of_people, email):
    message_body = json.dumps({
        'location': location,
        'cuisine': cuisine,
        'dining_time': dining_time,
        'number_of_people': number_of_people,
        'email': email
    })
    try:
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message_body
        )
        print(f"Message sent to SQS: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")
