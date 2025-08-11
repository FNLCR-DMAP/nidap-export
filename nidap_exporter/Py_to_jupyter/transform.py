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
            # print("getting metadata for function", fun.name)
            # print("decorator", ast.dump(transform_dec, indent=4))

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
            global_funcs.append(fun)
    
    return func_data, global_funcs

def get_dependents(funcs):
    output_to_func = {}
    for func_name in funcs:
        out_rid = funcs[func_name]["output_rid"]
        if out_rid in output_to_func:
            raise Exception(f"Duplicate output rid {out_rid} for functions {output_to_func[out_rid]} and {func_name}")
        output_to_func[out_rid] = func_name
   
    #key: a function name
    #value: set of function names that depend on the key
    #i.e. key:parent, value: children
    # dependents = defaultdict(set) 
    dependents = {}
    root_nodes = []

    for func_name in funcs:
        if funcs[func_name]["input_rids"]:
            for in_rid in funcs[func_name]["input_rids"]:
                producer = output_to_func.get(in_rid)
                if producer:
                    if producer not in dependents:
                        dependents[producer] = set()
                    dependents[producer].add(func_name)
                else:
                    dependents[func_name] = set()
        # else:
            # root_nodes.append(func_name)
    

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

def get_cell_markdown_text(func, dependants):
    # print(f"getting markdown for {func}")

    name = func["name"]
    link = f"<a id='{name}'></a>\n"
    header_text = f"**{name}**\n\n{func['template']}\n\n"
    
    parent_text = ""
    dep_list = [d for d in dependants if func["name"] in dependants[d]]
    
    if dep_list:
        # print(f"adding parents for {name}")
        # print(dep_list)
        parent_text = "Parents \n\n"
        for d in dep_list:
            parent_text += f" - [{d}](#{d})\n\n"
        parent_text += "\n\n"

    child_text = ""
    if func["name"] in dependants:
        print(f"adding children for {name}")
        child_text = "Children\n\n" 
        for child in dependants[func["name"]]:
            child_text += f" - [{child}](#{child})\n\n"
        child_text += "\n\n"
    # print(f"\t{parent_text}")
    # print(f"\t{child_text}")

    return f"{link}{header_text}{parent_text}{child_text}"
    
    
def dag_to_jupyter(
        func_order, 
        all_funcs, 
        root_nodes, 
        global_func_list, 
        notebook_file_path, 
        dependants
    ):

    import nbformat as nbf
    nb = nbf.v4.new_notebook()
    
    cells = []
    
    cells.append(nbf.v4.new_code_cell(
        "import sys\nsys.path.insert(0, '/data/BIDS-HPC/public/software/spac_dev/src')"
    ))

    cells.append(nbf.v4.new_markdown_cell("# Input Data"))
    opening_code_text = "import pandas as pd\n" 
    #TODO document fact that you need to update user specificationto full path
    for node in root_nodes:
        if node in all_funcs and "function" in all_funcs[node]:
            opening_code_text += "\n"
            all_funcs[node]["function"].name = f"get_{node}"
            all_funcs[node]["function"].decorator_list = []
            opening_code_text += ast.unparse(all_funcs[node]["function"])
            opening_code_text += f"\n{node} = get_{node}()"
            opening_code_text += f"\n"
            
            if node in func_order:
                func_order.remove(node)
        else:
            opening_code_text += f"\n{node} = /path/to/your/data\n"
    
    cells.append(nbf.v4.new_code_cell(opening_code_text))
    cells.append(nbf.v4.new_markdown_cell("# Code"))

    python_source_file = notebook_file_path.parent / "pipeline_functions.py"
    
    with open(python_source_file, 'w') as file:
        
        for func in global_func_list:
            code_text = ast.unparse(func)
            
            file.write(f"{code_text}\n\n")

        for f in func_order:
            func = all_funcs[f]
            if "function" in func:
                md_text = get_cell_markdown_text(func, dependants)
                
                cells.append(nbf.v4.new_markdown_cell(md_text))#, metadata={"collapsed":True}))
                
                code_text = ast.unparse(func["function"])
                file.write(f"#{func['template']}\n")
                file.write(f"{code_text}\n\n")

                import_text = f"from pipeline_functions import {func['name']}"
                call_text = ast.unparse(func['func_call'])
                indent = len(func['name']) + 1
                call_text = call_text.replace(",", ",\n" + " " * indent)

                # cells.append(nbf.v4.new_code_cell(code_text))
                cells.append(nbf.v4.new_code_cell(f"{import_text}\n{call_text}"))

    
    nb["cells"] = cells
    print(f"num cells: {len(cells)/2}")
    nbf.write(nb, notebook_file_path)

def configure_function(func_dict, root_nodes):
    
    for func_name in func_dict:
        if "function" in func_dict[func_name]:
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
                    root_nodes,
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
    #this is a syntax error. it shows up between the decorators
    code = code.replace("from pyspark.sql.types import *", "") 
    tree = ast.parse(code)
    
    all_code = [node for node in tree.body]

    named_funcs = {c.name: c for c in all_code if isinstance(c, ast.FunctionDef)}
   
    manual_datasets = {
        "mcmicro_output_annotation": {
            "name": "mcmicro_output_annotation",
            "input_rids": [],
            "output_rid": "ri.foundry.main.dataset.162f602c-6d49-4f8c-a5ca-e7a91249388f",
            },
        # "rename_observations" : {
        #     "name": "rename_observations",
        #     "input_rids": [],
        #     # "input_file_path": repo_dir / "data" / "rename_observations-transform_output.pickle",
        #     "output_rid": "ri.foundry.main.dataset.628a0f9f-7dbd-4841-8f9b-84ec1562bcc3",
        # }
    }

    func_dict, global_funcs = get_func_metadata(named_funcs.values(), repo_dir, named_funcs) #returns list of functions and metadata
    with open(repo_dir / "func_dict.txt", 'w') as f:
        pprint.pprint(func_dict, stream=f)

    func_dict = {**func_dict, **manual_datasets}
   
    dependants, root_nodes = get_dependents(func_dict)
    
    
    template_mapping = get_template_version(code, named_funcs.keys(), logger)
    
    for func_name in func_dict:
        if func_name in template_mapping:
            func_dict[func_name]["template"] = template_mapping[func_name]

    notebook_file = repo_dir / "pipeline.ipynb"
    func_dict_cleaned = configure_function(func_dict, root_nodes) #edits the functions 

    func_dict_cleaned = get_function_calls(func_dict_cleaned, root_nodes, logger)
    func_dict_cleaned = spark_to_pandas_root_nodes(func_dict_cleaned, root_nodes, logger)
    func_dict_cleaned = configure_imports(func_dict_cleaned, logger)
    
    ts = TopologicalSorter(dependants)

    sorted = list(ts.static_order())
    sorted.reverse()

    data_dir = repo_dir / "data"
    data_dir.mkdir(exist_ok=True)

    dag_to_jupyter(sorted, func_dict_cleaned, root_nodes,  global_funcs, notebook_file, dependants)


if __name__ == "__main__":
    main(
        # Path("/Users/frenchth/Foundry_Migration/workbook-migration/pipeline-extractor-python/nidap-export/test_repo/SPAC-v0-9-0-SCIMAP-Workbook")
        Path(sys.argv[1])
    )