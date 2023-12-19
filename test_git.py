# access_token = json.loads(Variable.get('var_github_access_token'))['git_access_token_for_auto_refinement']
from github import Github

'''
    try:
        file = repo.get_contents(path=folder + filename, ref='curate_heap')
        repo.update_file(path=folder + filename, message=f'updating file: {filename}', sha=file.sha,
                         content=merge_sql, branch='curate_heap')
        logging.info('updated the file: %s in %s', filename, folder)
    except:
        repo.create_file(path=folder + filename, message=f"adding file: {filename}",
                         content=merge_sql, branch='curate_heap')
        logging.info(f'created the file: %s in %s', filename, folder)
    list_of_pr_titles = []
    for pr in repo.get_pulls(state='open', sort='created', base='main', head='curate_heap'):
        list_of_pr_titles.append(pr.title)

    if "curate_heap" not in list_of_pr_titles:
        logging.info("creating PR for the changes as there no existing PR with OPEN status")
        pr = repo.create_pull(title="curate_heap", body="curate_heap",
                      head="curate_heap", base="develop")
        pr.create_review_request(team_reviewers=['data-engineering'])
        # g.get_repo("PyGithub/PyGithub").get_branch("master")
        repo.merge("curate_heap", head.commit.sha, f"adding file: {filename}")
    else:
        logging.info("PR is already requested for previous changes and has OPEN status")
    logging.info('completed uploading to %s', airflow_repo)

'''

git_repo = 'yeswanth-bf/webapp-for-standup'
base = 'main'
target = 'develop'
g = Github('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')
repo = g.get_repo(git_repo)

try:
    # base = repo.get_branch(base)
    head = repo.get_branch(target)

    merge_to_master = repo.merge(base,
                        head.commit.sha, "merge to {}".format(base))

except Exception as ex:
    print(ex)