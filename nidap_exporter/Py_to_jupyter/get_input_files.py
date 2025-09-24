import subprocess
import json
import requests

token_location = "/home/frenchth/.nidap.token"
dataset_rid="ri.foundry.main.dataset.c1de84b7-3497-4d32-9817-843e50ae582b"
# dataset_rid="ri.foundry.main.dataset.d6c0dd02-90a9-4842-91c7-cdfd5664f97b"
output_location="/data/NIDAP-JOBS/python_workbook_to_jupyter/SPAC-analysis-Hu/input_data"

with open(token_location) as f:
    TOKEN = f.read().strip()

HOSTNAME="nidap.nih.gov"


result = subprocess.run([
    "curl",
    "-H", f"Authorization: Bearer {TOKEN}",
    f"https://{HOSTNAME}/api/v2/datasets/{dataset_rid}/files?branchName=master"
] , capture_output=True, text=True)

file_data = json.loads(result.stdout)

paths = [p["path"] for p in file_data["data"]]

for index, p in enumerate(paths):
    print(f"Fetching content for file {index + 1}/{len(paths)}: {p}")
 
    headers = {
    "authorization": "Bearer {}".format(TOKEN)
    }

    response = requests.get(f'https://{HOSTNAME}/api/v2/datasets/{dataset_rid}/files/{p}/content', headers=headers)
    print(response)
    text = response.text
    with open(f"{output_location}/{p}", "w") as f:
        f.write(text)
  