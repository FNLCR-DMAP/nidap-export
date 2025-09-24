import configparser
from pathlib import Path
import subprocess
from datetime import datetime
import shutil 

def setup_job_folder(config, repo_path, func_name):
    data_folder = Path(config["default"]["data_dir"])

    data_dir = repo_path / data_folder
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    job_dir = data_dir / f"hpc_jobs/{func_name}-{timestamp}"
    
    job_dir.mkdir(parents=True, exist_ok=True)
    input_dir = job_dir / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir = job_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    return job_dir


def submit_hpc_job(input_file_path, 
                   app, 
                   mode, 
                   func_name,
                   repo_path, 
                   config_path, 
                   template_param_path,
                   output_file_name):
    if not template_param_path:
        raise Exception("No Parameter file path found")
    config = configparser.ConfigParser()
    config.read(config_path)

    job_dir = setup_job_folder(config, repo_path, func_name)
    shutil.copy(input_file_path, job_dir / "input")
    shutil.copy(template_param_path, job_dir /  "input/node_template_parameters.json")
    input_file_name = Path(input_file_path).name

    hpc_pipelines_dir = Path(config.get("default", "hpc_pipelines_dir"))
    job_script_path = hpc_pipelines_dir / Path(config.get("default", "hpc_job_script"))
    tool_dir = hpc_pipelines_dir / Path(config.get("default", "hpc_tool_dir"))

    app_dir = hpc_pipelines_dir / app / mode
    print("running on hpc with options:")
    print(f"  tool_dir: {tool_dir}")
    print(f"  input_file_name: {input_file_name}")
    print(f"  job_dir: {job_dir}")
    print(f"  app_dir: {app_dir}")
    command = [
            str(job_script_path),
            "--tool_dir",
            str(tool_dir),
            "--input_file",
            input_file_name,
            "--app_dir",
            str(app_dir),
            "--job_dir",
            str(job_dir),

        ]
    print(f"running command {' '.join(command)}")
    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise Exception(f"Job submission failed: \n\tstdout: {result.stdout}\n\tstderr: {result.stderr}")
    else:
        print(f"Job completed successfully")
        shutil.copy(job_dir / f"output/{input_file_name}", repo_path / "data")

'''
/vf/users/NIDAP-JOBS/hpc-pipelines-master/hpc_pipelines/hpc_code_workbook_connector/src/job_script.sh \\
    --tool_dir "/vf/users/NIDAP-JOBS/hpc-pipelines-master/hpc_pipelines/hpc_code_workbook_connector/src" \\
    --input_file "transform_output.pickle" \\
    # --job_dir "//data/NIDAP-JOBS/hpc-connecter-jobs-output/hpc_code_workbook_connector_jobs//rui.he--2025-08-05-07-04-19-29aa1" \\
    --app_dir "/vf/users/NIDAP-JOBS/hpc-pipelines-master/hpc_pipelines/spac_phenograph/CPU" ##################################################
'''