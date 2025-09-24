import ast
import json
def get_dependents(funcs, to_make_root=[]):
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
    
    root_nodes.extend(to_make_root)
    
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

        child_text = "Children\n\n" 
        for child in dependants[func["name"]]:
            child_text += f" - [{child}](#{child})\n\n"
        child_text += "\n\n"
    # print(f"\t{parent_text}")
    # print(f"\t{child_text}")
    if "template_params" in func:
        parameter_text = "Parameters\n\n"
        
        try:
            params = json.loads(func["template_params"])
            for key, value in params.items():
                parameter_text += f" - {key}: {value}\n\n"
        except:
            parameter_text = func["template_params"]
    else:
        parameter_text = ""
    return f"{link}{header_text}{parent_text}{child_text}{parameter_text}"

def format_path(path):

    if isinstance(path, list):
        return str([str(p) for p in path])
    else:
        return f"'{str(path)}'"
    
def get_foundry_data_path(node, repo_dir, logger):
    foundry_data = repo_dir / "foundry_data"
    node_path = foundry_data / node
    # if not node_path.exists():
    #     logger.warn(f"could not find expected input data file {node_path}, ")
    #     return "/path/to/your/datafile"
    if node_path.is_dir():
        csv_path = node_path / f"{node}.csv"
        if csv_path.exists():
            return format_path(csv_path)
        else:
            return format_path([ str(n) for n in node_path.glob("*.csv")])
    
    node_file_path = list(foundry_data.glob(f"{node}-*.*"))
    if len(node_file_path) == 0:
        logger.warn(f"could not find expected input data file {node_path}, ")
        return format_path("/path/to/your/datafile")
    elif len(node_file_path) > 1:
        logger.warn(f"found multiple files for expected input data file {node_path}")
        return format_path(node_file_path[0])
    else:
        return format_path(node_file_path[0])
            

def dag_to_jupyter(
        
        func_order, 
        all_funcs, 
        root_nodes, 
        global_func_list, 
        notebook_file_path, 
        dependants,
        repo_dir,
        logger,
        to_make_root=[]
    ):

    import nbformat as nbf
    nb = nbf.v4.new_notebook()
    
    cells = []
    
    cells.append(nbf.v4.new_code_cell(
        "import sys\n"\
        "sys.path.insert(0, '/data/BIDS-HPC/public/software/spac_dev/src')\n"\
        "import plotly.io as pio\n"\
        "pio.renderers.default = 'notebook'"
    ))
    cells.append(nbf.v4.new_code_cell("%load_ext autoreload\n%autoreload 2"))

    cells.append(nbf.v4.new_markdown_cell("# Input Data"))
    opening_code_text = "import pandas as pd\n" 
    #TODO document fact that you need to update user specificationto full path
    for node in root_nodes:
        if node in all_funcs and "function" in all_funcs[node]:
            opening_code_text += "\n"
            all_funcs[node]["function"].name = f"get_{node}"
            all_funcs[node]["function"].decorator_list = []
            if node in to_make_root:
                all_funcs[node]["function"].args = []
                opening_code_text += '# this function was specified to be a manual input, see the subsequent functions on how to handle this\n'
                opening_code_text += '# this is the original code\n'
                opening_code_text += "'''\n"
                opening_code_text += ast.unparse(all_funcs[node]["function"])
                opening_code_text += "'''\n"
                opening_code_text += f"{all_funcs[node]['function'].name} = {get_foundry_data_path(node, repo_dir, logger)}\n"
            else:
                opening_code_text += ast.unparse(all_funcs[node]["function"])
                opening_code_text += f"\n{node} = get_{node}()"
            
            opening_code_text += f"\n"
            
            if node in func_order:
                func_order.remove(node)
        else:
            
            opening_code_text += f"\n{node} = {get_foundry_data_path(node, repo_dir, logger)}\n"
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
                if True: #TODO certian templates
                    import_text += f"\nimport plotly.io as pio\npio.renderers.default = 'notebook'"

                # indent = len(func['name']) + 1
                indent = 4
                # call_text = ast.unparse(func['func_call'])
                call_text = f"{func['name']}(\n"
                num_args = len(func['func_call'].value.args)
                num_kwargs = len(func['func_call'].value.keywords)
                for arg in func['func_call'].value.args:
                    call_text +=f"{' ' * indent}{ast.unparse(arg)},\n"
                for kwarg in func['func_call'].value.keywords:
                    call_text +=f"{' ' * indent}{kwarg.arg}={ast.unparse(kwarg.value)},\n"

                call_text += ")"                

                # cells.append(nbf.v4.new_code_cell(code_text))
                cells.append(nbf.v4.new_code_cell(f"{import_text}\n{call_text}"))

    
    nb["cells"] = cells
    print(f"num cells: {len(cells)/2}")
    nbf.write(nb, notebook_file_path)


