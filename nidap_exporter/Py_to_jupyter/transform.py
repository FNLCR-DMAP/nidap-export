import sys
import json
from pathlib import Path
import ast 
from collections import defaultdict 
import os
from ast_code_transforms import configure_pickles
from ast_code_transforms import get_output_file_info
from ast_code_transforms import configure_default_args
from ast_code_transforms import get_func_args_metadata
from ast_code_transforms import configure_func_calls
from ast_code_transforms import configure_non_function_output
from ast_code_transforms import get_function_calls
from ast_code_transforms import remove_foundry_artifacts
from ast_code_transforms import configure_load_csv_files
from ast_code_transforms import spark_to_pandas_root_nodes
from ast_code_transforms import configure_imports
import logging
import pprint
from graphlib import TopologicalSorter


logger = logging.getLogger(__name__)


def get_func_metadata(funcs):
    func_data = {}
    name_output_mapping = {}
    global_funcs = []
    for fun in funcs:
        if fun.decorator_list:
            transform_dec = None
            
            for dec in fun.decorator_list:
                if isinstance(dec, ast.Call):
                    if dec.func.id == "transform_pandas":
                        transform_dec = dec

            if transform_dec is None:
                logger.error(f"Function {fun.name} does not have the transform_pandas decorator")
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
            out_file_name, out_var_name = get_output_file_info(fun, logger)
            if out_file_name:
                out_file_name_suffix = out_file_name.split(".")[-1]
                out_file_name_prefix = out_file_name.split(".")[0]
                out_file_name = f"{out_file_name_prefix}-{func_metadata['output_rid']}.{out_file_name_suffix}"
                func_metadata["output_file_name"] = out_file_name
                func_metadata["output_var_name"] = out_var_name
            else:
                func_metadata["output_file_name"] = f"{fun.name}-{func_metadata['output_rid']}.pickle"
                func_metadata["output_var_name"] = None
            
            func_metadata["args_metadata"] = get_func_args_metadata(fun, logger)
            
            func_data[fun.name] = func_metadata
            name_output_mapping[transform_dec.args[0].keywords[0].value.value] = fun.name
            
        else:
            #TODO handle global functions
            logger.info(f"Function {fun.name} added to global funcs")
            global_funcs.append(fun)
    
    return func_data, global_funcs

def get_dependents(funcs):
       
    output_to_func = {funcs[func_name]["output_rid"]: func_name for func_name in funcs}
    dependents = defaultdict(set)
    root_nodes = []

    for func_name in funcs:
        if funcs[func_name]["input_rids"]:
            for in_rid in funcs[func_name]["input_rids"]:
                producer = output_to_func.get(in_rid)
                if producer:
                    dependents[producer].add(func_name)
                else:
                    dependents[func_name] = set()

    for has_child_func in dependents:
        has_parent = any([d for d in dependents if has_child_func in dependents[d]])
        if not has_parent:
            root_nodes.append(has_child_func)



    return dependents, root_nodes

def get_dag(funcs, dependents, logger):

    def build_tree(func):
        
        downstream = dependents.get(func["name"], None)
        
        if downstream:
            return {
                "name": func["name"],
                "function": func["function"],
                "children": [build_tree(child) for child in downstream]

            }
        else:
            return {
                "name": func["name"],
                "function": func["function"],
                "children": []
            }

    roots = [func for func in funcs if not func["input_rids"]]
    dag = [build_tree(func) for func in roots]
    
    #TODO prune zero depth nodes? e.g. sticky notes
    return dag

def dag_to_jupyter(func_order, all_funcs, root_nodes, global_func_list, notebook_file_path):

    import nbformat as nbf
    nb = nbf.v4.new_notebook()
    
    cells = []
    
    # func_list = []
    # for func in sorted:
    #     if func not in root_nodes:
    #         func_data = func_dict_cleaned[func]
    #         func_list.append(func_data)

    cells.append(nbf.v4.new_markdown_cell("# Global Functions", metadata={"collapsed":True}))
    for func in global_func_list:
        code_text = ast.unparse(func)
        cells.append(nbf.v4.new_code_cell(code_text))

    cells.append(nbf.v4.new_markdown_cell("# Input Data", metadata={"collapsed":True}))
    opening_code_text = "import pandas as pd\n" 
    for node in root_nodes:
        if node in all_funcs and "function" in all_funcs[node]:
            opening_code_text += "\n"
            all_funcs[node]["function"].name = f"get_{node}"
            all_funcs[node]["function"].decorator_list = []
            opening_code_text += ast.unparse(all_funcs[node]["function"])
            opening_code_text += f"\n{node} = get_{node}()"
            opening_code_text += f"\n"
            func_order.remove(node)
        else:
            opening_code_text += f"\n{node} = /path/to/your/data\n"
    
    cells.append(nbf.v4.new_code_cell(opening_code_text, metadata={"collapsed":True}))
    cells.append(nbf.v4.new_markdown_cell("# Code", metadata={"collapsed":True}))


    for f in func_order:
        func = all_funcs[f]
        if "function" in func:
            md_text = f"## {func['name']}\n{func['template']}"
            #TODO add links to children in markdown 
            cells.append(nbf.v4.new_markdown_cell(md_text, metadata={"collapsed":True}))
            
            code_text = ast.unparse(func["function"])
            call_text = ast.unparse(func["func_call"])
            cells.append(nbf.v4.new_code_cell(code_text))
            cells.append(nbf.v4.new_code_cell(call_text))

    
    nb["cells"] = cells
    print(f"num cells: {len(cells)/2}")
    nbf.write(nb, notebook_file_path)

def configure_function(func_dict):
    
    for func_name in func_dict:
        func_dict[func_name]["function"] = remove_foundry_artifacts(func_dict[func_name]["function"], logger) 
        
        for arg in func_dict[func_name]["args_metadata"]:
            if "Load CSV Files" in func_dict[func_name]["template"]:            
                func_dict[func_name]["function"] = configure_load_csv_files(
                    func_dict[func_name]["function"], 
                    arg,
                    func_dict[func_name]["args_metadata"][arg],
                    logger)
                # logger.info(f"Removing foundry isms from {func['name']}, {func['function']}")
            # else:
            func, default_args = configure_default_args(func_dict[func_name]["function"], logger)
            func_dict[func_name]["function"] = func
            func_dict[func_name]["default_args"] = default_args

            func_dict[func_name]["function"] = configure_pickles(
                func_dict[func_name], 
                arg, 
                func_dict[func_name]["args_metadata"][arg], 
                logger
            )
            
            if func_dict[func_name]["args_metadata"][arg]["arg_type"] == "sub_func_call":
                func_dict[func_name]["function"] = configure_func_calls(
                    func_dict[func_name], 
                    arg, 
                    func_dict[func_name]["args_metadata"][arg], 
                    logger
                )
            
            # func["function"] = configure_default_params(func, logger)
            ast.fix_missing_locations(func_dict[func_name]["function"]) 
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

def main(repo_dir):

    logging.basicConfig(level=logging.INFO)

    python_file = repo_dir / "pipeline.py"
    
    with open(python_file, 'r') as f:
        code = f.read()
    #this is a syntax error apparantly. 
    #it shows up between the decorators
    code = code.replace("from pyspark.sql.types import *", "") 
    tree = ast.parse(code)
    
    all_code = [node for node in tree.body]

    named_funcs = {c.name: c for c in all_code if isinstance(c, ast.FunctionDef)}
    
    template_mapping = get_template_version(code, named_funcs.keys(), logger)
    
    manual_datasets = { #assuming these are manually put in the data directory
        "mcmicro_output_annotation": {
            "name": "mcmicro_output_annotation",
            # "input_file_path": repo_dir / "data" / "updated_mcmicro_output_with_detailed_and_broad_cell_types.csv",
            "input_rids": [],
            "output_rid": "ri.foundry.main.dataset.162f602c-6d49-4f8c-a5ca-e7a91249388f",
            },
        "rename_observations" : {
            "name": "rename_observations",
            "input_rids": [],
            # "input_file_path": repo_dir / "data" / "rename_observations-transform_output.pickle",
            "output_rid": "ri.foundry.main.dataset.628a0f9f-7dbd-4841-8f9b-84ec1562bcc3",
        }
    }

    func_list, global_funcs = get_func_metadata(named_funcs.values())
    for func_name in func_list:
        func_list[func_name]["template"] = template_mapping[func_name]

    notebook_file = repo_dir / "pipeline.ipynb"
    func_dict_cleaned = configure_function(func_list)

    func_dict_cleaned = {**func_dict_cleaned, **manual_datasets}
    
    dependants, root_nodes = get_dependents(func_dict_cleaned)
    func_dict_cleaned = get_function_calls(func_dict_cleaned, root_nodes, logger)
    func_dict_cleaned = spark_to_pandas_root_nodes(func_dict_cleaned, root_nodes, logger)
    func_dict_cleaned = configure_imports(func_dict_cleaned, logger)
    
    ts = TopologicalSorter(dependants)

    sorted = list(ts.static_order())
    sorted.reverse()

    #TODO update code workbook utils.utils import text to value lines
    
    dag_to_jupyter(sorted, func_dict_cleaned, root_nodes,  global_funcs, notebook_file)


if __name__ == "__main__":
    main(
        # Path("/Users/frenchth/Foundry_Migration/workbook-migration/pipeline-extractor-python/nidap-export/test_repo/SPAC-v0-9-0-SCIMAP-Workbook")
        Path(sys.argv[1])
    )