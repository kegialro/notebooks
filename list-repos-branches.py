def list_repositories_with_branches(region, search_string):
    # Create a CodeCommit client
    codecommit = session.client('codecommit', region_name=region)

    # List repositories
    repositories_response = codecommit.list_repositories()
    repositories = repositories_response.get('repositories', [])
    file_path = 'repos.txt'


    # Open the file in write mode ('w' for write)
    with open(file_path, 'w') as file:

        for repo in repositories:
            repo_name = repo['repositoryName']

            # Check if the repository name contains the search string
            if search_string.lower() in repo_name.lower():
                print(f"Repository: {repo_name}")
                file.write(f"Repository: {repo_name}\n")
                # List branches for the repository
                branches_response = codecommit.list_branches(repositoryName=repo_name)
                branches = branches_response.get('branches', [])

                print("Branches:")
                file.write("Branches:\n")
                for branch in branches:
                    
                    print(f" - {branch}")
                    file.write(f" - {branch}\n")
                print("\n")

if __name__ == "__main__":
    # AWS CodeCommit settings
    aws_region = 'us-west-2'
    search_string = 'lakehouse'

    # List repositories and branches
    list_repositories_with_branches(aws_region, search_string)



def list_open_pull_requests(repository_name):
    client = session.client('codecommit')

    # List pull requests for the specified repository with the status 'open'
    response = client.list_pull_requests(
        repositoryName=repository_name,
        pullRequestStatus='OPEN'
    )

    # Print details of each open pull request
    for pull_request in response.get('pullRequestIds', []):
        pull_request_details = client.get_pull_request(
            pullRequestId=pull_request
        )
        #print(pull_request_details)
        print(f"Repository: {repository_name}")
        print(f"Pull Request ID: {pull_request}")
        print(f"Title: {pull_request_details['pullRequest']['title']}")
        #print(f"Description: {pull_request_details['pullRequest']['description']}")
        print(f"Author: {pull_request_details['pullRequest']['authorArn']}")
        print(f"Last Activity Date: {pull_request_details['pullRequest']['lastActivityDate']}")
        print(f"Creation Date: {pull_request_details['pullRequest']['creationDate']}")
        print("------------------------------")


repositories_list = [
'lakehouse-repo1','lakehouse-repo2','lakehouse-repo3'
]

for repo in repositories_list:

    list_open_pull_requests(repo)
