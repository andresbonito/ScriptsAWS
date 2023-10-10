import boto3

session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

CHARGES_TABLE = 'asb-bolaocopa-svc-GuessesTable'

group = 'Group A'

def query_guesses(group):
    response = DDB_CLIENT.query(
        TableName=CHARGES_TABLE,
        IndexName='group',
        KeyConditionExpression='#group = :group',
        ExpressionAttributeNames={
            '#group': 'group'
        },
        ExpressionAttributeValues={
            ':group': {
                'S': group
            }
        },
        ProjectionExpression='matchId,guessId,winningTeam',
        ScanIndexForward=False,
        Limit=1
    )

    return response

print(query_guesses(group))