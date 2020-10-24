import json
from datetime import *
import boto3

def lambda_handler(event, context):
    
    #Define client for boto-3
    client = boto3.client('lex-runtime')
    
    #SET INITAL VARIABLES
    lastUserMessage = event.get('messages')
    botMessage = "Something went wrong!! Please try again"
    lex_bot = 'DiningConcierge'
    bot_alias = 'DiningCon'
    
    if lastUserMessage is None or len(lastUserMessage) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    
    lastUserMessage = lastUserMessage[0]['unstructured']['text']

        
    response = client.post_text(
        botName = lex_bot,
        botAlias = bot_alias,
        inputText = lastUserMessage,
        userId='testuser')   
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
    
    if response['message'] is not None or len(response['message']) > 0:
        botMessage = response['message']
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': botMessage
        }
      }]
      
    return {
        'statusCode': 200,
        'headers': { "Access-Control-Allow-Origin": "*" },
        'messages': botResponse
    }


      
   