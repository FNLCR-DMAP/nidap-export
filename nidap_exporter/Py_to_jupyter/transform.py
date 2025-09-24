import argparse
import ast 
from utils.ast_code_transforms import (
    add_import,
    configure_default_args,
    configure_func_calls,
    configure_non_function_output,
    get_func_args_metadata,
    get_function_calls,
    get_output_file_info,
    
)
from utils.configure_load_csv_files import  configure_load_csv_files
from utils.configure_pickles import configure_pickles
from utils.configure_imports import configure_imports
from utils.configure_hpc_call import configure_hpc_call
from utils.spark_to_pandas import spark_to_pandas_root_nodes
from utils.configure_default_values import configure_default_vals
from utils.remove_foundry_artifiacts import remove_foundry_artifacts
from utils.configure_return import configure_return
from utils.download_datasets import download_datasets

from utils.dag_and_jupyter import (
    get_dependents,
    dag_to_jupyter
)
from submit_hpc_job import submit_hpc_job


import configparser
from graphlib import TopologicalSorter
import inspect
import json
import logging
import pandas as pd
from pathlib import Path
import sys
import subprocess
import pprint



logger = logging.getLogger(__name__)


def get_func_metadata(funcs, repo_dir, func_dict):
    func_data = {}
    name_output_mapping = {}
    global_funcs = []
    print("getting function metadata")
    # pprint.pprint(funcs)

    for fun in funcs:
        if fun.decorator_list:

            transform_dec = None
            
            for dec in fun.decorator_list:
                if isinstance(dec, ast.Call):
                    if dec.func.id == "transform_pandas":
                        transform_dec = dec

            if transform_dec is None:
                logger.warning(f"Function {fun.name} does not have the transform_pandas decorator")
                continue

            input_funcs = [kw for kw in transform_dec.keywords if kw.value.func.id == "Input"]
            input_var_rid_mapping = { kw.arg:kw.value.keywords[0].value.value for kw in input_funcs}
            has_output = transform_dec.args and transform_dec.args[0].func.id == "Output"
             
            func_metadata = {
                    "name": fun.name,
                    "input_rids": [kw.value.keywords[0].value.value for kw in input_funcs],
                    "output_rid": transform_dec.args[0].keywords[0].value.value if has_output else None,
                    "input_var_rid_mapping": input_var_rid_mapping,
                    "function": fun
            }
            out_file_name, out_var_name = get_output_file_info(fun,fun.name, repo_dir, logger)
            if out_file_name:
                # out_file_name_suffix = out_file_name.split(".")[-1]
                # out_file_name_prefix = out_file_name.split(".")[0]
                # out_file_name = f"{out_file_name_prefix}-{func_metadata['output_rid']}.{out_file_name_suffix}"
                func_metadata["output_file_name"] = str(repo_dir/ "data" / out_file_name)
                func_metadata["output_var_name"] = out_var_name
            else:
                func_metadata["output_file_name"] = str(repo_dir / "data" / f"{fun.name}-{func_metadata['output_rid']}.pickle")
                func_metadata["output_var_name"] = None
            
            func_metadata["args_metadata"] = get_func_args_metadata(fun, logger)
            
            func_data[fun.name] = func_metadata
            name_output_mapping[transform_dec.args[0].keywords[0].value.value] = fun.name
            
        else:
            logger.info(f"Function {fun.name} added to global funcs")
            fun.body.insert(0,ast.parse("import pandas as pd").body[0])
            fun.body.insert(0,ast.parse("import numpy as np").body[0])
            global_funcs.append(fun)
    
    return func_data, global_funcs

def configure_function(func_dict, root_nodes):
    
    for func_name in func_dict:
        
        if "function" in func_dict[func_name]:
            if func_dict[func_name]["function"].decorator_list:
                func_dict[func_name]["function"].decorator_list = []

            func_dict[func_name]["function"] = remove_foundry_artifacts(func_dict[func_name]["function"], logger) 
            func_dict[func_name]["function"] = add_import(func_dict[func_name]["function"])
         
            for arg in func_dict[func_name]["args_metadata"]:
                
                if ( "Load CSV Files" in func_dict[func_name]["template"] or
                    "Acquire CSV File Schemas" in func_dict[func_name]["template"] or
                    "Load Specified CSV Columns" in func_dict[func_name]["template"]
                ):
                    func_dict[func_name]["function"] = configure_load_csv_files(
                        func_dict[func_name], 
                        arg,
                        logger)

                func_dict[func_name]["function"] = configure_pickles(
                    func_dict,
                    func_name, 
                    arg, 
                    root_nodes,
                    logger
                )

                func_dict[func_name]["function"] = configure_non_function_output(
                    func_dict[func_name]["function"],  
                    func_dict[func_name]["args_metadata"],
                    logger
                )


    return func_dict

def get_template_version(code, func_names, logger):
    code = code.split("\n")
    template_mapping = {}
    for name in func_names:
        for i, line in enumerate(code):
            if f"def {name}(" in line:
                if code[i-2].startswith("#"):
                    template_comment = code[i-2].split("#")[1]
                elif code[i-1].startswith("#"):
                    template_comment = code[i-1].split("#")[1]
                else:
                    logger.warning(f"No template comment found for function {name}")
                    template_comment = ""
                
                template_mapping[name] = template_comment
                break
    return template_mapping

def get_manual_datasets(func_dict, logger):
    manual_datasets = {}
    for func_name in func_dict:
        for arg in func_dict[func_name]["input_var_rid_mapping"]:
            if arg not in func_dict and arg not in manual_datasets:
                logger.info(f"MANUAL DATASET FOUND:\n\tFound an arg without an input function: {arg}, making this a manual dataset")
                manual_datasets[arg] = {
                    "name": arg,
                    "input_rids": [],
                    "output_rid": func_dict[func_name]["input_var_rid_mapping"][arg],
                }

    return manual_datasets

def get_workbook_params(func_dict, config, branch, repo_dir, logger, download_params):
    if download_params:
        nidap_token_location = Path.home() / config["default"]["nidap_token_location"]
        with open(nidap_token_location, "r") as f:
            nidap_token = f.read().strip()


        template_script_dir = Path(config["default"]["template_param_script_dir"])
        hpc_pipelines_dir = Path(config["default"]["hpc_pipelines_dir"])
        template_script = hpc_pipelines_dir / template_script_dir

        download_dir = repo_dir / "template_params"
        download_dir.mkdir(parents=True, exist_ok=True)
        log_file = download_dir / "get_workbook_params.log"

        for func in func_dict:
            param_file = download_dir / f"{func_dict[func]['name']}_params.json"
            if param_file.exists():
                # logger.info(f"Parameter file {param_file} already exists, skipping download")
                continue
            
            logger.info(f"downloading parameters for {func}")
            (download_dir / "param_json_response.json").unlink(missing_ok=True)
            (download_dir / "node_template_parameters.json").unlink(missing_ok=True)
            rid = func_dict[func]["output_rid"]
            
            
            result = subprocess.run(
                [
                    str(template_script), 
                    rid, 
                    branch,
                    nidap_token,
                    download_dir,
                    log_file

                ],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise Exception(f"Failed to get workbook params for function {func}: {result.stderr}")
                
            else:
                with open(download_dir / "node_template_parameters.json", 'r') as f:
                    response = f.read()
                    if response.startswith("JSON parsing error:"):    
                        split = response.split("\n")
                        if len(split) > 1 and split[1] == "None":
                            response = "No Parameters"
                        else:
                            logger.warning(f"Unexpected response format for function {func}: {response}")
                    else:
                        func_dict[func]["template_param_path"] = param_file

                    with open(param_file, 'w') as pf:
                        pf.write(response)
                        

                    func_dict[func]["template_params"] = response
    
    return func_dict


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform your python Code Workbook into a Jupter Notebook"
    )
    
    # Positional argument (required)
    parser.add_argument(
        'repo_dir',
        type=str,
        help='Path to the input file'
    )
    
    # Optional argument with a default value
    # parser.add_argument(
    #     '--download_params',
    #     type=bool,
    #     default=False,
    #     help='Whether to download parameters (default: False)'
    # )
    parser.add_argument(
        '--download_params',
        action=argparse.BooleanOptionalAction,
        default=True,
        help='Whether to download parameters, --no_download_params for false (default: True)'
    )

    parser.add_argument(
        "--config_file_path",
        type=str,
        default="./transformer_config.cfg",
        help="Path to the configuration file"
    )
    parser.add_argument(
        "--branch",
        type=str,
        default="master",
        help="Git branch to use (default: master)"
    )
    parser.add_argument(
        "--to_make_root",
        nargs='+',
        default=[],
        help="List of root nodes to make (default: [])"
    )
    parser.add_argument(
        "--download_all_datasets",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Whether to download all datasets (default: False)"
    )
    return parser.parse_args()
def main():
    args = parse_args()
    args.repo_dir = Path(args.repo_dir)

    logging.basicConfig(level=logging.INFO)
    config = configparser.ConfigParser()
    config.read(args.config_file_path)

    python_file = args.repo_dir / "pipeline.py"
    print(f"repo dir: {args.repo_dir}")
    print(f"Reading python file from {python_file}")
    with open(python_file, 'r') as f:
        code = f.read()
    #this is a syntax error. it shows up between the decorators
    code = code.replace("from pyspark.sql.types import *", "") 
    code = code.replace("from pyspark.sql import functions as F", "")
    code = code.replace("from pyspark.sql import Row", "")
    code = code.replace("import pandas as pd", "")

    tree = ast.parse(code)
    all_code = [node for node in tree.body]
    named_funcs = {c.name: c for c in all_code if isinstance(c, ast.FunctionDef)}
   
    func_dict, global_funcs = get_func_metadata(named_funcs.values(), args.repo_dir, named_funcs) 
    
    # if False:
    func_dict = get_workbook_params(func_dict, config, args.branch, args.repo_dir, logger, args.download_params)
    
    manual_datasets = get_manual_datasets(func_dict, logger)

    hpc_sumbit_function = ast.parse(inspect.getsource(submit_hpc_job)).body[0]
    global_funcs.append(hpc_sumbit_function)

    func_dict = {**func_dict, **manual_datasets}
    template_mapping = get_template_version(code, named_funcs.keys(), logger)
    
    for func_name in func_dict:
        if func_name in template_mapping:
            func_dict[func_name]["template"] = template_mapping[func_name]
        else:
            func_dict[func_name]["template"] = "No Template"
    with open(args.repo_dir / "function_metadata.json", 'w') as f:
        func_dict_cleaned = {key: {subkey:str(sub_val) for subkey, sub_val in value.items()} for key, value in func_dict.items()}
        json.dump(func_dict_cleaned, f, indent=4)     
    if args.download_all_datasets:
        download_datasets(func_dict, args.repo_dir, config, args.branch, logger)
    else:

        dependants, root_nodes = get_dependents(func_dict, to_make_root=args.to_make_root)

        notebook_file = args.repo_dir / "pipeline.ipynb"
        func_dict_cleaned = configure_function(func_dict, root_nodes) #edits the functions 
        func_dict_cleaned = get_function_calls(func_dict_cleaned, root_nodes, logger)
        func_dict_cleaned = spark_to_pandas_root_nodes(func_dict_cleaned, root_nodes, logger)
        func_dict_cleaned = configure_imports(func_dict_cleaned, logger)
        func_dict_cleaned = configure_default_vals(func_dict_cleaned, logger)
        func_dict_cleaned = configure_hpc_call(func_dict_cleaned, "./transformer_config.cfg", args.repo_dir, logger)
        # func_dict_cleaned = configure_return(func_dict_cleaned, logger)
        ts = TopologicalSorter(dependants)

        sorted = list(ts.static_order())
        sorted.reverse()

        data_dir = args.repo_dir / "data"
        data_dir.mkdir(exist_ok=True)

        #TODO unnamed_54 (Spatial plot) not working
        dag_to_jupyter(sorted, func_dict_cleaned, root_nodes,  global_funcs, notebook_file, dependants, args.repo_dir,logger, to_make_root=args.to_make_root)


if __name__ == "__main__":
    # main(
    #     Path(sys.argv[1]),
    #     sys.argv[2],
    #     Path("./transformer_config.cfg")
    # )
    main()