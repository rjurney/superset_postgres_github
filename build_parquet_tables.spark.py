import sys, os, re
import json

from dateutil.parser import parse as duparse
from pyspark.sql import Row

sc, spark # in attendence?

# Load all Github events for the year spanning 04-01-2018 to 03-31-2019
github_lines = sc.textFile('s3://github-dataset/*.json.gz') # Change me to entire bucket!

# Apply the function to every record
def parse_json(line):
    record = None
    try:
        record = json.loads(line)
    except json.JSONDecodeError as e:
        sys.stderr.write(str(e))
        record = {'error': 'Parse error'}
    return record

github_events = github_lines.map(parse_json)
github_events = github_events.filter(lambda x: 'error' not in x)

# Note there are all kinds of events in the pile
[x['type'] for x in github_events.take(10) if 'type' in x]

# Split our events out by type
split_types = lambda x, t: 'type' in x and x['type'] == t 

# See https://developer.github.com/v3/activity/events/types/
create_events         = github_events.filter(lambda x: split_types(x, 'CreateEvent'))
delete_events         = github_events.filter(lambda x: split_types(x, 'DeleteEvent'))
fork_events           = github_events.filter(lambda x: split_types(x, 'ForkEvent'))
gist_events           = github_events.filter(lambda x: split_types(x, 'GistEvent'))
issue_events          = github_events.filter(lambda x: split_types(x, 'IssuesEvent'))
issue_comment_events  = github_events.filter(lambda x: split_types(x, 'IssueCommentEvent'))
member_events         = github_events.filter(lambda x: split_types(x, 'MemberEvent'))
push_events           = github_events.filter(lambda x: split_types(x, 'PushEvent'))
pull_events           = github_events.filter(lambda x: split_types(x, 'PullRequestEvent'))
repo_events           = github_events.filter(lambda x: split_types(x, 'RepositoryEvent'))
star_events           = github_events.filter(lambda x: split_types(x, 'StarEvent'))

# Check: did it work? - may have to run more than once...
# [(x[0], x[1]['type']) for x in [
#     ('CreateEvent', create_events.first()), 
#     ('DeleteEvent', delete_events.first()), 
#     ('ForkEvent',   fork_events.first())
# ]]

# See: https://developer.github.com/v3/activity/events/types/#forkevent
def extract_fork(f):
    """Extracts Rows of ForkEvents and their associated fields from a gharchive.org event dict"""
    
    # Out forks...
    out_f = {
        'id': f['id'],
        'created_at': duparse(f['created_at']),
        'type': 'ForkEvent',
        'public': f['public']
    }
    
    actor = f['actor']
    out_f['actor_user_id'] = actor['id']
    out_f['actor_user_name'] = actor['login']
    
    org = f['org'] if 'org' in f else {}
    out_f['from_org_id'] = org['id'] if 'id' in org else ''
    out_f['from_org_login'] = org['login'] if 'login' in org else ''
    
    repo = f['repo']
    out_f['from_repo_id'] = repo['id']
    out_f['from_repo_name'] = repo['name']
    
    payload = f['payload']
    forkee = payload['forkee']
    
    owner = forkee['owner']
    out_f['to_user_id'] = owner['id']
    out_f['to_user_name'] = owner['login']
    
    out_f['to_repo_created_at'] = duparse(forkee['created_at'])
    out_f['to_repo_updated_at'] = duparse(forkee['updated_at'])
    out_f['to_repo_pushed_at'] = duparse(forkee['pushed_at'])
    
    out_f['to_repo_size'] = forkee['size']
    out_f['to_repo_stargazer_count'] = forkee['stargazers_count']
    out_f['to_repo_watcher_count'] = forkee['watchers_count']
    out_f['to_repo_forks_count'] = forkee['forks_count']
    
    license = forkee['license'] if 'license' in forkee and isinstance(forkee['license'], dict) else {}
    out_f['to_license_key'] = license['key'] if 'key' in license else ''
    out_f['to_license_name'] = license['name'] if 'name' in license else ''
    
    return Row(**out_f)

forks = fork_events.map(extract_fork).toDF().select(
    'id',
    'type',
    'created_at',
    'actor_user_id',
    'actor_user_name',
    'from_org_id',
    'from_org_login',
    'to_user_id',
    'to_user_name',
    'to_repo_created_at',
    'to_repo_updated_at',
    'to_repo_pushed_at',
    'to_repo_size',
    'to_repo_stargazer_count',
    'to_repo_watcher_count',
    'to_repo_forks_count',
    'to_license_key',
    'to_license_name',
    'public'
)
forks.write.mode('overwrite').parquet('s3://github-superset-parquet/ForkEvents.parquet')

forks = spark.read.parquet('s3://github-superset-parquet/ForkEvents.parquet')
forks.show(5)

# See: https://developer.github.com/v3/activity/events/types/#pushevent
def extract_push(p):
    """Extracts Rows of PushEvents and their associated Commits from a gharchive.org event dict"""
    
    # Out pushes...
    out_p = {
        'type': 'PushEvent',
        'id': p['id'],
        'created_at': duparse(p['created_at']),
        'public': p['public']
    }
    
    # Who pushed it?
    actor = p['actor']
    out_p['actor_id'] = actor['id']
    out_p['actor_user_name'] = actor['login']
    
    # To what repo?
    repo = p['repo']
    out_p['repo_id'] = repo['id']
    out_p['repo_name'] = repo['name']
    
    # What did they push?
    payload = p['payload']
    out_p['push_id'] = payload['push_id']
    out_p['push_size'] = payload['size']
    out_p['push_ref'] = payload['ref']
    out_p['push_head'] = payload['head']
    out_p['push_before'] = payload['before']
    
    # Out commits...
    out_cs = []
    commits = payload['commits']
    for c in commits:
        out_c = {
            'type': 'Commit',
            'sha': c['sha'],
            'repo_id': out_p['repo_id'],
            'repo_name': out_p['repo_name'],
            'push_id': out_p['push_id'],
            'actor_id': out_p['actor_id'],
            'actor_user_name': out_p['actor_user_name'],
            'author_name': c['author']['name'],
            'url': c['url'],
            'message': c['message'],
            'push_created_at': out_p['created_at'],
            'public': out_p['public']
        }
        out_cs.append(Row(**out_c))
        
    return [Row(**out_p)] + (out_cs)

# Generate both PushEvents and Commits in varyin length lists with flatMap...
push_and_commits = push_events.flatMap(extract_push)

# Split pushes, make DataFrame and store as CSV for a SQL DB
pushes_raw = push_and_commits.filter(lambda x: x['type'] == 'PushEvent')
pushes = pushes_raw.toDF().select(
    'id',
    'type',
    'actor_id',
    'actor_user_name',
    'repo_id',
    'repo_name',
    'push_id',
    'push_size',
    'push_ref',
    'push_head',
    'push_before',
    'created_at',
    'public'
)
pushes.write.mode('overwrite').parquet('s3://github-superset-parquet/PushEvents.Parquet')

pushes = spark.read.parquet('s3://github-superset-parquet/PushEvents.Parquet')
pushes.show(5)

# Split commits, make DataFrame and store as CSV for a SQL DB
commits_raw = push_and_commits.filter(lambda x: x['type'] == 'Commit')
commits = commits_raw.toDF().select(
    'sha',
    'type',
    'push_id',
    'actor_id',
    'repo_id',
    'repo_name',
    'actor_user_name',
    'author_name',
    'url',
    'message',
    'push_created_at',
    'public'
)
commits.write.mode('overwrite').parquet('s3://github-superset-parquet/Commits.parquet')

# See https://developer.github.com/v3/activity/events/types/#createvent
def extract_create(c):
    """Extract Rows of CreateEvents and their associated fields from a gharchive.org event dict"""
    
    # Out creates...
    out_c = {
        'id': c['id'],
        'created_at': duparse(c['created_at']),
        'type': 'CreateEvent',
    }
    
    actor = c['actor']
    out_c['actor_id'] = actor['id']
    out_c['actor_user_name'] = actor['login']
    
    repo = c['repo']
    out_c['repo_id'] = repo['id']
    out_c['repo_name'] = repo['name']
    
    out_c['public'] = c['public']
    
    return Row(**out_c)

create_events.map(extract_create).toDF().select(
    'id',
    'type',
    'created_at',
    'actor_id',
    'actor_user_name',
    'repo_id',
    'repo_name',
    'public'
).write.mode('overwrite').parquet('s3://github-superset-parquet/Creates.parquet')

creates = spark.read.parquet('s3://github-superset-parquet/Creates.parquet')
creates.show(5)

# See https://developer.github.com/v3/activity/events/types/#deleteevent
def extract_delete(d):
    """Extract Rows of DeleteEvents and their associated fields from a gharchive.org event dict"""
    
    # Out deletes...
    out_d = {
        'id': d['id'],
        'type': 'DeleteEvent',
        'created_at': duparse(d['created_at'])
    }
    
    actor = d['actor']
    out_d['actor_id'] = actor['id']
    out_d['actor_user_name'] = actor['login']
    
    repo = d['repo']
    out_d['repo_id'] = repo['id']
    out_d['repo_name'] = repo['name']
    
    org = d['org'] if 'org' in d else {}
    out_d['org_id'] = org['id'] if 'id' in org else ''
    out_d['org_name'] = org['login'] if 'login' in org else ''
    
    out_d['public'] = d['public']
    
    return Row(**out_d)

delete_events.map(extract_delete).toDF().select(
    'id',
    'type',
    'created_at',
    'actor_id',
    'actor_user_name',
    'repo_id',
    'repo_name',
    'org_id',
    'org_name',
    'public'
).write.mode('overwrite').parquet('s3://github-superset-parquet/Deletes.parquet')

deletes = spark.read.parquet('s3://github-superset-parquet/Deletes.parquet')
deletes.show(5)

# See https://developer.github.com/v3/activity/events/types/#issueevent
def extract_issue(i):
    """Extract Rows of IssueEvents and their associated fields from a gharchive.org event dict"""
    
    # Out issues...
    out_i = {
        'id': i['id'],
        'type': 'IssuesEvent',
        'created_at': duparse(i['created_at']),
        'public': i['public']
    }
    
    actor = i['actor']
    out_i['actor_id'] = actor['id']
    out_i['actor_user_name'] = actor['login']
    
    repo = i['repo']
    out_i['repo_id'] = repo['id']
    out_i['repo_name'] = repo['name']
    
    payload = i['payload']
    out_i['action'] = payload['action']

    issue = payload['issue']
    out_i['assignee'] = issue['assignee']
    out_i['assignees'] = issue['assignees']
    out_i['body'] = issue['body']
    out_i['closed_at'] = duparse(issue['closed_at']) if issue['closed_at'] else None
    out_i['comments'] = issue['comments']
    out_i['issue_id'] = issue['id']
    out_i['labels'] = issue['labels']
    out_i['locked'] = issue['locked']
    out_i['number'] = issue['number']
    out_i['title'] = issue['title']
    out_i['updated_at'] = issue['updated_at']
    
    user = issue['user']
    out_i['user_id'] = user['id']
    out_i['user_name'] = user['login']
    
    return Row(**out_i)

issue_events.map(extract_issue).toDF().select(
    'id',
    'type',
    'created_at',
    'updated_at',
    'closed_at',
    'actor_id',
    'actor_user_name',
    'repo_id',
    'repo_name',
    'user_id',
    'user_name',
    'action',
    'assignee',
    'assignees',
    'title',
    'body',
    'comments',
    'issue_id',
    'labels',
    'locked',
    'number',
    'public'
).write.mode('overwrite').parquet('s3://github-superset-parquet/Issues.parquet')

issues = spark.read.parquet('s3://github-superset-parquet/Issues.parquet')
issues.show(5)

# See https://developer.github.com/v3/activity/events/types/#memberevent
def extract_member(m):
    """Extract Rows of MemberEvents and their associated fields from a gharchive.org event dict"""
    
    # Out members...
    out_m = {
        'id': m['id'],
        'type': 'MemberEvent',
        'created_at': duparse(m['created_at']),
        'public': m['public']
    }
    
    actor = m['actor']
    out_m['actor_id'] = actor['id']
    out_m['actor_user_name'] = actor['login']
    
    payload = m['payload']
    out_m['action'] = payload['action']

    member = payload['member']
    out_m['member_id'] = member['id']
    out_m['member_name'] = member['login']
    out_m['site_admin'] = member['site_admin']
    
    repo = m['repo']
    out_m['repo_id'] = repo['id']
    out_m['repo_name'] = repo['name']
    
    return out_m

member_events.map(extract_member).toDF().select(
    'id',
    'type',
    'created_at',
    'actor_id',
    'actor_user_name',
    'action',
    'member_id',
    'member_name',
    'site_admin',
    'repo_id',
    'repo_name',
    'public'
).write.mode('overwrite').parquet('s3://github-superset-parquet/Members.parquet')

members = spark.read.parquet('s3://github-superset-parquet/Members.parquet')
members.show(5)

# See https://developer.github.com/v3/activity/events/types/#pullrequestevent
def extract_pull(p):
    """Extract Rows of PullRequestEvents and their associated fields from a gharchive.org event dict"""
    
    # Out pull requests...
    out_p = {
        'id': p['id'],
        'type': 'PullRequestEvent',
        'created_at': duparse(p['created_at']),
        'public': p['public']
    }
    
    actor = p['actor']
    out_p['actor_id'] = actor['id']
    out_p['actor_user_name'] = actor['login']
    
    org = p['org'] if 'org' in p else {}
    out_p['org_id'] = org['id'] if 'id' in org else None
    out_p['org_name'] = org['login'] if 'login' in org else None
    
    payload = p['payload']
    out_p['action'] = payload['action']
    out_p['number'] = payload['number']
    
    pull_request = payload['pull_request']
    out_p['additions'] = pull_request['additions']
    out_p['assignee'] = pull_request['assignee']
    out_p['assignees'] = pull_request['assignees']
    out_p['author_association'] = pull_request['author_association']
    
    base = pull_request['base']
    out_p['base_label'] = base['label']
    out_p['base_ref'] = base['ref']
    
    base_repo = base['repo']
    out_p['base_repo_created_at'] = duparse(base_repo['created_at'])
    out_p['base_repo_default_branch'] = base_repo['default_branch'] if 'default_branch' in base_repo else None
    out_p['base_repo_description'] = base_repo['description']
    out_p['base_repo_fork'] = base_repo['fork']
    out_p['base_repo_forks'] = base_repo['forks']
    out_p['base_repo_full_name'] = base_repo['full_name']
    out_p['base_repo_id'] = base_repo['id']
    out_p['base_repo_language'] = base_repo['language']
    
    license = base_repo['license'] if isinstance(base_repo['license'], dict) else {}
    out_p['base_repo_license_key'] = license['key'] if 'key' in license else None
    out_p['base_repo_license_name'] = license['name'] if 'name' in license else None
    
    out_p['base_repo_name'] = base_repo['name']
    out_p['base_repo_open_issues'] = base_repo['open_issues']
    
    owner = base_repo['owner']
    out_p['base_repo_owner_id'] = owner['id']
    out_p['base_repo_owner_user_name'] = owner['login']
    out_p['base_repo_owner_site_admin'] = owner['site_admin']
    
    out_p['base_repo_private'] = base_repo['private']
    out_p['base_repo_pushed_at'] = duparse(base_repo['pushed_at'])
    out_p['base_repo_size'] = base_repo['size']
    out_p['base_repo_stargazers_count'] = base_repo['stargazers_count']
    out_p['base_repo_updated_at'] = duparse(base_repo['updated_at'])
    out_p['base_repo_watchers'] = base_repo['watchers']
    
    out_p['base_sha'] = base['sha']
    
    base_user = base['user']
    out_p['base_user_id'] = base_user['id']
    out_p['base_user_user_name'] = base_user['login']
    out_p['base_user_site_admin'] = base_user['site_admin']
    
    out_p['body'] = pull_request['body']
    out_p['changed_files'] = pull_request['changed_files']
    out_p['closed_at'] = duparse(pull_request['closed_at']) if pull_request['closed_at'] else None
    out_p['comments'] = pull_request['comments']
    out_p['commits'] = pull_request['commits']
    out_p['created_at'] = duparse(pull_request['created_at'])
    out_p['deletions'] = pull_request['deletions']
    
    head = pull_request['head']
    out_p['head_label'] = head['label']
    out_p['head_ref'] = head['ref']
    
    head_repo = head['repo'] if 'repo' in head and isinstance(head['repo'], dict) else {}
    out_p['head_repo_created_at'] = duparse(head_repo['created_at']) if 'created_at' in head_repo and head_repo['created_at'] else None
    out_p['head_repo_default_branch'] = head_repo['default_branch'] if 'default_branch' in head_repo else None
    out_p['head_repo_description'] = head_repo['description'] if 'description' in head_repo else None
    out_p['head_repo_fork'] = head_repo['fork'] if 'fork' in head_repo else None
    out_p['head_repo_forks'] = head_repo['forks'] if 'forks' in head_repo else None
    out_p['head_repo_full_name'] = head_repo['full_name'] if 'full_name' in head_repo else None
    out_p['head_repo_id'] = head_repo['id'] if 'id' in head_repo else None
    out_p['head_repo_language'] = head_repo['language'] if 'language' in head_repo else None
    out_p['head_repo_languages'] = head_repo['languages'] if 'languages' in head_repo else ''
    
    head_repo_license = head_repo['license'] if 'license' in head_repo and isinstance(head_repo['license'], dict) else {}
    out_p['head_repo_license_key'] = head_repo_license['key'] if 'key' in head_repo_license else None
    out_p['head_repo_license_name'] = head_repo_license['name'] if 'name' in head_repo_license else None
    
    out_p['head_repo_name'] = head_repo['name'] if 'name' in head_repo else None
    out_p['head_repo_open_issues'] = head_repo['open_issues'] if 'open_issues' in head_repo else None
    
    head_repo_owner = head_repo['owner'] if 'owner' in head_repo else {}
    out_p['head_repo_owner_id'] = head_repo_owner['id'] if 'id' in head_repo_owner else None
    out_p['head_repo_owner_user_name'] = head_repo_owner['login'] if 'login' in head_repo_owner else None
    out_p['head_repo_owner_site_admin'] = head_repo_owner['site_admin'] if 'site_admin' in head_repo_owner else None
    
    out_p['head_repo_private'] = head_repo['private'] if 'private' in head_repo else None
    out_p['head_repo_pushed_at'] = duparse(head_repo['pushed_at']) if 'pushed_at' in head_repo else None
    out_p['head_repo_size'] = head_repo['size'] if 'size' in head_repo else None
    out_p['head_repo_stargazers_count'] = head_repo['stargazers_count'] if 'stargazers_count' in head_repo else None
    out_p['head_repo_updated_at'] = duparse(head_repo['updated_at']) if 'updated_at' in head_repo else None
    out_p['head_repo_watchers'] = head_repo['watchers'] if 'watchers' in head_repo else None
    
    out_p['head_sha'] = head['sha']
    
    head_user = head['user']
    out_p['head_user_id'] = head_user['id']
    out_p['head_user_name'] = head_user['login']
    out_p['head_user_site_admin'] = head_user['site_admin']
    
    out_p['id'] = pull_request['id']
    # out_p['labels'] = pull_request['labels']
    out_p['locked'] = pull_request['locked']
    out_p['merge_commit_sha'] = pull_request['merge_commit_sha']
    out_p['mergeable'] = pull_request['mergeable']
    out_p['merged'] = pull_request['merged']
    out_p['merged_at'] = duparse(pull_request['merged_at']) if 'merged_at' in pull_request and pull_request['merged_at'] else None
    out_p['merged_by'] = pull_request['merged_by']
    out_p['milestone'] = pull_request['milestone']
    out_p['number'] = pull_request['number']
    out_p['rebaseable'] = pull_request['rebaseable']
    out_p['requested_reviewers'] = pull_request['requested_reviewers']
    out_p['requested_teams'] = pull_request['requested_teams']
    out_p['review_comments'] = pull_request['review_comments']
    out_p['state'] = pull_request['state']
    out_p['title'] = pull_request['title']
    out_p['updated_at'] = duparse(pull_request['updated_at']) if 'updated_at' in pull_request else None
    
    user = pull_request['user']
    out_p['user_id'] = user['id']
    out_p['user_name'] = user['login']
    out_p['user_site_admin'] = user['site_admin']
    
    out_p['public'] = p['public']
    
    repo = p['repo']
    out_p['repo_id'] = repo['id']
    out_p['repo_name'] = repo['name']
    
    return Row(**out_p)

from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

pull_events.map(extract_pull).take(1)[0]

pull_events.map(extract_pull).toDF(sampleRatio=0.01).write.mode('overwrite').parquet(
    's3://github-superset-parquet/PullRequests.parquet'
)

pull_requests = spark.read.parquet('s3://github-superset-parquet/PullRequests.parquet')
pull_requests.show(5)

# # I return no records. Why is that?
# # See https://developer.github.com/v3/activity/events/types/#repositoryevent
# def extract_repo(r):
#     """Extract Rows of RepositoryEvents and their associated fields from a gharchive.org event dict"""
#    
#     # Our repos...
#     out_r = {
#         'id': r['id'],
#         'created_at': duparse(r['created_at'])
#     }
#    
#     return r
#
# I return no records. Why is that?
# repo_events.map(extract_repo).take(10)

# I return no records. Why is that?
# # See https://developer.github.com/v3/activity/events/types/#watchevent
# def extract_star(s):
#     """Extract Rows of WatchEvents and their associated fields from a gharchive.org event dict"""
#    
#     # Out stars...
#     out_s = {
#         'id': s['id'],
#         'created_at': s['created_at']
#     }
#    
#     return s
#
# I return no records. Why is that?
# star_events.map(extract_star).take(10)
