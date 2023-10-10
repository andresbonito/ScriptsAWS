import boto3

session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

# GUESSES_TABLE = 'asb-bolaocopa-svc-GuessesTable'
TEAMS_TABLE = 'asb-bolaocopa-svc-TeamsTable'

def get_teamId(team1, team2):
    response = DDB_CLIENT.query(
        TableName=TEAMS_TABLE,
        IndexName='team',
        KeyConditionExpression='#team1 = :team1 AND #team2 = :team2',
        ExpressionAttributeNames={
            '#team1': 'team',
            '#team2': 'team',
        },
        ExpressionAttributeValues={
            ':team1': {
                'S': team1
            },
            ':team2': {
                'S': team2
            }
        }
    )

    return response


print(get_teamId('BRAZIL', 'ARGENTINA'))