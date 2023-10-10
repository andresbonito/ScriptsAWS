import boto3
import string
import random


session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

RESULTS_TABLE = 'asb-bolaocopa-svc-ResultsTable'
GUESSES_TABLE = 'asb-bolaocopa-svc-GuessesTable'

def debug_log(msg) -> None:
    print('[DEBUG] - [searchMatchId]', msg)


def query_guesses(guessId):
    response = DDB_CLIENT.query(
        TableName=GUESSES_TABLE,
        KeyConditionExpression='#guessId = :guessId',
        ExpressionAttributeNames={
            '#guessId': 'guessId'
        },
        ExpressionAttributeValues={
            ':guessId': {
                'S': guessId
            }
        }
    )

    success = True if response['Count'] > 0 else False

    return success


# Função que cria o guessId
def create_guess_id(length, verify=True):
    debug_log(f'Primeiro Verify: {verify}')

    while verify:
        letters = string.ascii_uppercase + string.digits
        id = ""
        
        for _ in range(length):    
            id += letters[random.randint(0, len(letters) - 1)]
        debug_log(id.upper())

        verify = query_guesses(id.upper())
    debug_log(f'Segundo Verify: {verify}')

    debug_log(id.upper())
    return id.upper()


create_guess_id(10)