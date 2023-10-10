import boto3

session = boto3.Session(profile_name='admbonito', region_name='us-east-1')
DDB_CLIENT = session.client('dynamodb')

GUESSES_TABLE = 'asb-bolaocopa-svc-GuessesTable'
TEAMS_TABLE = 'asb-bolaocopa-svc-TeamsTable'

def debug_log(msg) -> None:
    print('[DEBUG] - [updateTeams]', msg)

def error_log(msg) -> None:
    print('[ERROR] - [updateTeams]', msg)


def deep_clean_field(raw_dict: dict):
    response = {}

    for key, value in raw_dict.items():
        if key in ['S', 'N', 'B', 'L', 'NS', 'SS', 'BS', 'BOOL', ]:
            return value
        if key in ['NULL', ]:
            return None
        elif key in ['M', ]:
            return deep_clean_field(value)
        else:
            response.update({key: deep_clean_field(value)})

    return response

def query_teams(team):
    response = DDB_CLIENT.query(
        TableName=TEAMS_TABLE,
        IndexName='team',
        KeyConditionExpression='#team = :team',
        ExpressionAttributeNames={
            '#team': 'team'
        },
        ExpressionAttributeValues={
            ':team': {
                'S': team
            }
        }
    )

    return response

def update_teams(teamId, grupo):
    try:
        response = DDB_CLIENT.update_item(
            TableName=TEAMS_TABLE,
            Key={
                'teamId': {'S': teamId}
            },
            ConditionExpression='attribute_exists(teamId)',
            UpdateExpression='SET #group=:group',
            ExpressionAttributeNames={
                '#group': 'group'
            },
            ExpressionAttributeValues={
                ':group': {'S': grupo}
            }
        )

        return response
    
    except Exception as e:
        error_log(str(e))
        return {
            'error': 'DynamoDBError'
        }


def updating():
    group_a = {'teams': ['qatar', 'ecuador', 'senegal', 'netherlands'], 'group': 'Group A'}
    group_b = {'teams': ['england', 'iran', 'united states', 'wales'], 'group': 'Group B'}
    group_c = {'teams': ['argentina', 'saudi arabia', 'mexico', 'poland'], 'group': 'Group C'}
    group_d = {'teams': ['france', 'australia', 'denmark', 'tunisia'], 'group': 'Group D'}
    group_e = {'teams': ['spain', 'costa rica', 'germany', 'japan'], 'group': 'Group E'}
    group_f = {'teams': ['belgium', 'canada', 'morocco', 'croatia'], 'group': 'Group F'}
    group_g = {'teams': ['brazil', 'serbia', 'switzerland', 'cameroon'], 'group': 'Group G'}
    group_h = {'teams': ['portugal', 'ghana', 'uruguay', 'korea republic'], 'group': 'Group H'}

    groups = [group_a, group_b, group_c, group_d, group_e, group_f, group_g, group_h]

    for group in groups:
        teams = group['teams']
        grupo = group['group']
        for team in teams:
            time = team.upper()
            query = query_teams(time)

            items = query['Items'][0]
            response_items = deep_clean_field(items)

            response = update_teams(response_items['teamId'], grupo)

    return response


debug_log(f'Updapting - {updating()}')
