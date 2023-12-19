import json
import ast
import boto3
from github import Github
from slack_sdk import WebClient

print('Loading function')


print("Received trigger from SNS")
message = event['Records'][0]['Sns']

print(message)
event_dict = ast.literal_eval(message["Message"])
print(event_dict)
# slack details
session = boto3.session.Session()
secret_client = session.client(service_name='secretsmanager', region_name='us-east-1')
get_secret = secret_client.get_secret_value(SecretId='data/prod/airflow/variables/var_slack_token')
slack_secret = json.loads(get_secret['SecretString'])
slack_token = slack_secret['managed_airflow_bot_token']
slack_channel = 'data-pipeline-airflow-deploy'

# event details
timestamp = message["Timestamp"]
stage = event_dict["detail"]["stage"]
state = event_dict["detail"]["state"]
pipeline = event_dict["detail"]["pipeline"]
state = event_dict["detail"]["state"]
execution_id = event_dict["detail"]["execution-id"]
sourceActions = event_dict["additionalAttributes"]["sourceActions"][0]

# git details
branch = sourceActions['sourceActionVariables']['BranchName']
commit_id = sourceActions['sourceActionVariables']['CommitId']
repo_name = sourceActions['sourceActionVariables']['RepositoryName']

git_link = f"https://github.com/Beyond-Finance/{repo_name}/commit/{commit_id}"
execution_link = f'https://us-east-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/{pipeline}/executions/{execution_id}/timeline?region=us-east-1'

access_token = secret_client.get_secret_value(SecretId='data/prod/airflow/variables/var_github_access_token')[
    'SecretString']
git_token = json.loads(access_token)['git_access_token_for_auto_refinement']
g = Github(git_token)
repo = g.get_repo('Beyond-Finance/data-bi-airflow-dags')
commit_message = repo.get_commit(commit_id).raw_data['commit']['message']

green = '#36a64f'
red = '#FF0000'
orange = '#FFA500'
grey = '#A9A9A9'

if state == 'STARTED':
    color = orange
    emoji = ':announcement:'
elif state == 'SUCCEEDED':
    color = green
    emoji = ':white_check_mark:'
elif state == 'FAILED':
    color = red
    emoji = ':rotating_light:'
else:
    color = grey
    emoji = ':heavy_multiplication_x:'

slack_msg = {
    "attachments": [
        {
            "mrkdwn_in": [
                "text"
            ],
            "color": color,
            "fields": [
                {
                    "title": "Repo",
                    "value": repo_name,
                    "short": False
                },
                {
                    "title": "Execution ID",
                    "value": f"<{execution_link}|*{execution_id}*>",
                    "short": False
                },
                {
                    "title": "Stage",
                    "value": stage,
                    "short": True
                },
                {
                    "title": "State",
                    "value": state,
                    "short": True
                },
                {
                    "title": "Timestamp",
                    "value": timestamp,
                    "short": True
                },
                {
                    "title": "Branch",
                    "value": branch,
                    "short": True
                },
                {
                    "title": "Code revision",
                    "value": commit_message + "\n<" + git_link + "|*See on GitHub*>",
                    "short": False
                }
            ]
        }
    ]
}

client = WebClient(token=slack_token)
if stage == 'Deploy':
    response = client.chat_postMessage(
        channel=slack_channel,
        attachments=slack_msg['attachments'],
        text=emoji + " *Airflow Deployment Notification*"
    )
    print(response)
else:
    print(f"Stage is {stage} so not sending to slack")

