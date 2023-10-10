import boto3

session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

GUESSES_TABLE = 'asb-bolaocopa-svc-GuessesTable'

def query_guesses(matchId):
    response = DDB_CLIENT.query(
        TableName=GUESSES_TABLE,
        IndexName='matchId',
        KeyConditionExpression='#matchId = :matchId',
        ExpressionAttributeNames={
            '#matchId': 'matchId'
        },
        ExpressionAttributeValues={
            ':matchId': {
                'S': matchId
            }
        }
    )

    return response

    # items = response['Items']

    # for item in items:
    #     response_items = deep_clean_field(item)

    #     max_current = response_items['MaxCurrent']
    #     model = response_items['Model']
    
    # return max_current, model

print(query_guesses('match#1'))