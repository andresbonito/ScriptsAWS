import boto3

session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

RESULTS_TABLE = 'asb-bolaocopa-svc-ResultsTable'
GUESSES_TABLE = 'asb-bolaocopa-svc-GuessesTable'

def debug_log(msg) -> None:
    print('[DEBUG] - [searchMatchId]', msg)

away_team = 'france'
home_team = 'argentina'

guesses = ['DLCNIAHFTC', 'FK975VZV5C', 'PPA29G63DZ', 'LDRTRIVY66', 'R1YZP2FI8W', 'X15ZBCG2RB']

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

    return response


def query_results(away_team, home_team):
    response = DDB_CLIENT.scan(
        TableName=RESULTS_TABLE,
        FilterExpression='away_team = :away_team AND home_team = :home_team',
        ExpressionAttributeValues={
            ':away_team': {
                'S': away_team.upper()
            },
            ':home_team': {
                'S': home_team.upper()
            }
        }
    )

    return response


for i in guesses:
    response = query_guesses(i)
    results = query_results(away_team, home_team)
    debug_log(response)
    debug_log(results)
    break
