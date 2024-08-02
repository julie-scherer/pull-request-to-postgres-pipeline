import os
import psycopg2
import requests
from requests.exceptions import RequestException
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

token = os.environ.get('GITHUB_TOKEN')
database_url = os.environ.get("BLOG_DATABASE_URL")
json_path = os.environ.get('JSON_PATH', '')

def create_log_directory():
  logs_dir = 'logs'
  os.makedirs(logs_dir, exist_ok=True)

  timestamp = datetime.now().strftime('%Y_%m_%d-%H%M')
  timestamped_dir = os.path.join(logs_dir, timestamp)
  os.makedirs(timestamped_dir, exist_ok=True)

  return timestamped_dir


## TASK 1 - GET ASSIGNMENTS FROM POSTGRES
def query_assignments(extra_filter: str = '') -> dict:
  print(f"Fetching assignments data from the database")
  query = f"""
  SELECT assignment_id, assignment_title, assignment_link, github_org, repo_name
  FROM schema.assignments_table
  WHERE github_org IS NOT NULL AND repo_name IS NOT NULL
  {extra_filter}
  """
  print(f"Query to execute: \n{query}")
  with psycopg2.connect(database_url) as conn:
    assignments_df = pd.read_sql_query(query, conn)
  if assignments_df.empty:
    raise ValueError("No records found")
  print(f"Assignments found: \n{assignments_df.head(5)}")
  assignments = assignments_df.set_index('assignment_id').to_dict(
    orient='index')
  return assignments


## TASK 2 - GET ASSIGNMENT SUBMISSIONS FROM GITHUB

# task 2 supporting functions
def fetch_pull_requests(owner, repo, headers, logs_dir: str) -> list:
  pulls_url = f'https://api.github.com/repos/{owner}/{repo}/pulls'
  print(f"Fetching all pull requests in {owner}/{repo}: {pulls_url}")
  all_pulls = []
  page = 1
  while True:
    print(f"Fetching page {page}")
    response = requests.get(pulls_url,
                headers=headers,
                params={
                  'state': 'all',
                  'per_page': 100,
                  'page': page
                })
    response.raise_for_status()
    pulls_json = response.json()
    if not pulls_json:
      break
    all_pulls.extend(pulls_json)
    if 'Link' in response.headers:
      links = response.headers['Link']
      if 'rel="next"' not in links:
        break
    else:
      break
    page += 1
  with open(os.path.join(logs_dir, f'pulls_response_{repo}.json'), 'w') as f:
    json.dump(all_pulls, f)
  print(f"Found {len(all_pulls)} pull requests")
  return all_pulls


def fetch_workflow_runs(owner, repo, pr_number, head_sha, headers,
            logs_dir: str) -> list:
  actions_url = f'https://api.github.com/repos/{owner}/{repo}/actions/runs'
  print(f"Fetching list of workflow runs in {owner}/{repo} using head_sha = {head_sha}")
  actions_res = requests.get(actions_url,
                headers=headers,
                params={
                  'per_page': 20,
                  'page': 1,
                  'head_sha': head_sha
                })
  actions_res.raise_for_status()
  actions_json = actions_res.json()
  # print(json.dumps(actions_json, indent=2))
  out_dir = os.path.join(logs_dir, 'actions_response')
  os.makedirs(out_dir, exist_ok=True)
  with open(os.path.join(out_dir, f'{repo}_pr_{pr_number}.json'), 'w') as f:
    json.dump(actions_json, f)
  workflow_runs = actions_json.get('workflow_runs', [])
  print(f"Found {len(workflow_runs)} workflow runs")
  return workflow_runs


def fetch_workflow_run_id(owner, repo, run_id, headers, logs_dir: str) -> dict:
  workflow_run_url = f'https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}'
  print(f"Fetching workflow run data using run_id = {run_id}")
  workflow_res = requests.get(workflow_run_url, headers=headers)
  workflow_res.raise_for_status()
  workflow_json = workflow_res.json()
  # print(json.dumps(workflow_json, indent=2))
  out_dir = os.path.join(logs_dir, 'workflow_runs_response')
  os.makedirs(out_dir, exist_ok=True)
  with open(os.path.join(out_dir, f'{repo}_runid_{run_id}.json'), 'w') as f:
    json.dump(workflow_json, f)
  print(f"Successfully fetched {len(workflow_json)} records from `/repos/{owner}/{repo}/actions/runs/{run_id}`")
  return workflow_json


def process_workflow_runs(owner, repo, workflow_runs: list, headers: dict, logs_dir: str) -> dict:
  
  print(f"Searching for the best run out of {len(workflow_runs)} workflow runs")
  recent_successful_run = None
  most_recent_run = None
  for workflow_run in workflow_runs:
    run_id = workflow_run.get('id')
    if not run_id:
      continue
    workflow_json = fetch_workflow_run_id(owner, repo, run_id, headers, logs_dir)
    run_status = workflow_json['status']
    if run_status == 'completed':
      conclusion = workflow_json['conclusion']
      if conclusion == 'success':
        if not recent_successful_run or (
            workflow_json['updated_at']
            > recent_successful_run['updated_at']):
          recent_successful_run = {
            'run_id': run_id,
            'run_number': workflow_json['run_number'],
            'event': workflow_json['event'],
            'status': workflow_json['status'],
            'conclusion': conclusion,
            'created_at': workflow_json['created_at'],
            'updated_at': workflow_json['updated_at'],
            'workflow_url': workflow_json['html_url'],
            'head_repository': workflow_json['head_repository']['full_name'],
            'fork': workflow_json['head_repository']['fork'],
          }
    if not most_recent_run or (workflow_json['updated_at'] > most_recent_run['updated_at']):
      most_recent_run = {
        'run_id': run_id,
        'run_number': workflow_json['run_number'],
        'event': workflow_json['event'],
        'status': workflow_json['status'],
        'conclusion': workflow_json['conclusion'],
        'created_at': workflow_json['created_at'],
        'updated_at': workflow_json['updated_at'],
        'workflow_url': workflow_json['html_url'],
        'head_repository':
        workflow_json['head_repository']['full_name'],
        'fork': workflow_json['head_repository']['fork'],
      }
  best_run = recent_successful_run or most_recent_run
  print(f"Found best run: {best_run}")
  return best_run


# task 2 main function
def retrieve_assignment_submission_data(assignment_id: int, assignment_data: dict, logs_dir: str, token: str):
  headers = {
    'X-GitHub-Api-Version': '2022-11-28',
    'Authorization': f"Bearer {token}"
  }
  owner = assignment_data['github_org']
  repo = assignment_data['repo_name']
  assignment_data['pull_requests'] = assignment_data.get('pull_requests', {})
  try:
    pulls_json = fetch_pull_requests(owner, repo, headers, logs_dir)
    for pull_request in pulls_json:
      pr_number = pull_request['number']
      head_sha = pull_request['head']['sha']
      if not pr_number or not head_sha:
        continue
      pr_id = pull_request['id']
      state = pull_request['state']
      submission_link = pull_request['html_url']
      github_username = pull_request['user']['login']
      submission_time = pull_request['created_at']
      assignment_data['pull_requests'][pr_number] = {
        'pr_id': pr_id,
        'state': state,
        'submission_link': submission_link,
        'github_username': github_username,
        'submission_time': submission_time,
        'head_sha': head_sha,
        'workflow_run': {}
      }
      workflow_runs = fetch_workflow_runs(owner, repo, pr_number,head_sha, headers, logs_dir)
      best_run = process_workflow_runs(owner, repo, workflow_runs, headers, logs_dir)
      if best_run:
        assignment_data['pull_requests'][pr_number]['workflow_run'] = best_run
    with open(
        os.path.join(logs_dir,f'final_assignment_submissions_{assignment_id}.json'), 'w') as f:
      json.dump(assignment_data, f)
  except RequestException as e:
    print(f"Error processing assignment {assignment_id}: {e}")
    return False
  return assignment_data


## TASK 3 - UPDATE ASSIGNMENT SUBMISSIONS TBL IN POSTGRES
def add_assignment_submissions_records(assignment_id: int, assignment_data: dict, logs_dir: str):
    print(f"Adding assignment submission records to database")

    values = []
    pull_requests = assignment_data['pull_requests']
    for pr_number, pull_request_data in pull_requests.items():
        print(f"Successfully fetched data from PR #{pr_number}")
        pr_state = pull_request_data.get('state')
        github_username = pull_request_data.get('github_username')
        submission_link = pull_request_data.get('submission_link')
        workflow_run = pull_request_data.get('workflow_run', {})
        passes_preliminary_checks = workflow_run.get('conclusion') == 'success'
        submission_time = pull_request_data.get('submission_time')
        try:
            if github_username:
                query_user_id = f"""
                SELECT u.user_id 
                FROM schema.users_table u 
                WHERE u.social_metadata ->> 'Github' ILIKE '{github_username}'
                """
                with psycopg2.connect(database_url) as conn:
                  with conn.cursor() as cursor:
                      cursor.execute(query_user_id)
                      user_result = cursor.fetchone()
                      print(f"Successfully executed query: {query_user_id}")
                if not user_result:
                  print(f"User ID not found for GitHub username `{github_username}`")
                  user_id = 'null'
                else:
                  user_id = user_result[0]
                values.append(f"({user_id}, {assignment_id}, '{submission_link}', {passes_preliminary_checks}, '{submission_time}', '{json.dumps(pull_request_data)}', '{github_username}', '{pr_state}')")
        except Exception as e:
          print(f"An exception occurred: {str(e)}")
    return values

def execute_insert_query(insert_query: str):
  try:
    with psycopg2.connect(database_url) as conn:
      with conn.cursor() as cursor:
          print(f"Executing query: {insert_query}")
          cursor.execute(insert_query)
    print(f"Record successfully added!")
  except Exception as e:
      print(f"An exception occurred: {str(e)}")

def main(json_path: str = ''):
  logs_dir = create_log_directory()
  os.makedirs(logs_dir, exist_ok=True)
  
  filter_clause = f"AND github_org = '{os.environ.get('GIT_ORG')}' and repo_name ilike '{os.environ.get('GIT_REPO')}'"
  assignments = query_assignments(extra_filter=filter_clause)
  
  insert_query = """
  INSERT INTO schema.assignment_submissions_staging (student_user_id, assignment_id, submission_link, passes_preliminary_checks, submission_time, pull_request_data, github_username, pr_state) 
  """
  insert_into_log = os.path.join(logs_dir, f'insert_assignment_submissions_staging.sql')
  with open(insert_into_log, 'w') as f:
      f.write(insert_query)
   
  values = []
  for assignment_id, assignment_data in assignments.items():
    print(f"Processing assignment #{assignment_id}")
    owner = assignment_data.get('github_org')
    repo = assignment_data.get('repo_name')
    if not owner or not repo:
      raise ValueError('Missing GitHub organization or repository name.')
    if json_path:
      with open(json_path, 'r') as f:
        new_assignment_data = json.load(f)
    else:
      new_assignment_data = retrieve_assignment_submission_data(assignment_id, assignment_data, logs_dir, token)
    if new_assignment_data:
        values.extend(add_assignment_submissions_records(assignment_id, new_assignment_data, logs_dir))
  
  if values: 
    formatted_values = " VALUES " + ", ".join(values)
    with open(insert_into_log, 'a') as f:
      f.write(formatted_values)
    insert_query += formatted_values
    execute_insert_query(insert_query)
  else:
      print("No records to insert.")

if __name__ == "__main__":
  main(json_path)
