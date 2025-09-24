import os
import requests
from pathlib import Path
from urllib.parse import quote
import pandas as pd


def get_file_list(func_data, headers, HOSTNAME, branch, logger):
    #need to write
    #function name, dataset rid, file name     
    rid = func_data["output_rid"]
    
    file_list = []
    page_token = None
    while True:
        if page_token:
            url = f'https://{HOSTNAME}/api/v2/datasets/{rid}/files?branchName={branch}&pageSize=1000&pageToken={page_token}'
        else:
            url = f'https://{HOSTNAME}/api/v2/datasets/{rid}/files?branchName={branch}&pageSize=1000'
        
        response = requests.get(
            url,
            headers=headers
        )

        if response.status_code != 200:
            # raise Exception
            logger.error(f"Failed to fetch files for dataset {func_data['name']} with RID {rid}. Status code: {response.status_code}")
            continue
        
        response = response.json()

        file_list.extend(response["data"])
        
        if "nextPageToken" in response:
            page_token = response["nextPageToken"]
        else:
            break

    return file_list
            

def download_files(manifest, headers, HOSTNAME, branch, logger):

    # to_download = [entry for entry in manifest if entry["type"] == "dataset" and not entry["downloaded"]]
    # updated_manifest = []

    try:
        for index, file_data in enumerate(manifest):
            if file_data["type"] != "dataset" or file_data["downloaded"]:
                continue

            path = file_data["foundry_path"]
            path = quote(path, safe='')
            download_command = f'https://{HOSTNAME}/api/v2/datasets/{file_data["rid"]}/files/{path}/content?branchName={branch}'

            file_response = requests.get(
                download_command,
                headers=headers
            )
            if file_response.status_code != 200:
                logger.error(f"Failed to download file {path} for dataset {file_data['function']} with RID {file_data['rid']}. Status code: {file_response.status_code}")
                continue

            
            with open(file_data["bw_path"], "wb") as f:
                f.write(file_response.content)

            manifest[index]["downloaded"] = True
    
    except Exception as e:
        logger.error(f"Error downloading files: {e}")

    return manifest



def download_datasets(func_dict, repo_dir, config, branch, logger):
    data_dir = repo_dir / "foundry_data"
    data_dir.mkdir(exist_ok=True)

    nidap_token_location = Path.home() / config["default"]["nidap_token_location"]
    with open(nidap_token_location, "r") as f:
        nidap_token = f.read().strip()

    HOSTNAME = "nidap.nih.gov"
    headers = {
        f"authorization": f"Bearer {nidap_token}"
    }
    logger.info(f"Downloading {len(func_dict)} datasets to {data_dir}")
    
    data_manifest = []

    manifest_path = repo_dir / "data_manifest.csv"
    if manifest_path.exists():
        df = pd.read_csv(manifest_path)
        df = df[df['type'] == "dataset"]
        if all(df["downloaded"]):
            logger.info(f"All datasets already downloaded, skipping download.")
            return
        

    print(f"Downloading datasets to {data_dir}")
    for index, func in enumerate(func_dict):
        func_data = func_dict[func]
        rid = func_data["output_rid"]

        if rid.startswith("ri.vector.main.execute."):
            # logger.info(f"Skipping dataset {func_data['name']} with RID {rid}, no data associated with node.")
            data_manifest.append({
                "function": func,
                "rid": rid,
                "type": "execute",
                "foundry_path": None,
                "bw_path": None,
                "downloaded": None,
            })

        else:        
            file_list = get_file_list(func_dict[func], headers, HOSTNAME, branch, logger)
            
            download_location = data_dir


            if len(file_list) > 1:
                download_location = data_dir / func_dict[func]["name"]
                download_location.mkdir(exist_ok=True)

            for file_data in file_list:
                path = file_data["path"]
                bw_path = download_location / f'{func}-{path.replace("/", "-")}'

                downloaded = bw_path.exists()
                data_manifest.append({
                    "function": func,
                    "rid": rid,
                    "type": "dataset",
                    "foundry_path": path,
                    "bw_path": bw_path,
                    "downloaded": downloaded,
                })

    manifest_df = pd.DataFrame(data_manifest)
   
    manifest_df.to_csv(manifest_path, index=False)
    
    data_manifest = download_files(data_manifest, headers, HOSTNAME, branch, logger)
    manifest_df = pd.DataFrame(data_manifest)
    manifest_df.to_csv(manifest_path, index=False)