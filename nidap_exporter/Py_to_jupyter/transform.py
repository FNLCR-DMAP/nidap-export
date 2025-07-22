import json
from pathlib import Path
import ast 
from collections import defaultdict 

def get_func_dependencies(funcs):


    dag_list = []
    name_output_mapping = {}

    for fun in funcs:
        if fun.decorator_list:
            transform_dec = None
            
            for dec in fun.decorator_list:
                if isinstance(dec, ast.Call):
                    if dec.func.id == "transform_pandas":
                        transform_dec = dec

            if transform_dec is None:
                print(f"Function {fun.name} does not have the transform_pandas decorator")
                continue

            input_funcs = [kw for kw in transform_dec.keywords if kw.value.func.id == "Input"]
            has_input =  transform_dec.keywords and input_funcs

            has_output = transform_dec.args and transform_dec.args[0].func.id == "Output"
            
            dag_list.append(
                {
                    "name": fun.name,
                    "input_rids": [kw.value.keywords[0].value.value for kw in input_funcs],
                    "output_rid": transform_dec.args[0].keywords[0].value.value if has_output else None,
                    "function": fun

                }
            )

            name_output_mapping[transform_dec.args[0].keywords[0].value.value] = fun.name
            
        else:
            print(f"Function {fun.name} has no decorators")
    
    return dag_list

def get_dag(funcs):
    output_to_func = {func["output_rid"]: func["name"] for func in funcs}
    dependents = defaultdict(list)

    for func in funcs:
        if func["input_rids"]:
            for in_rid in func["input_rids"]:
                producer = output_to_func.get(in_rid)
                if producer:
                    dependents[producer].append(func)
    
    def build_tree(func):
        
        downstream = dependents.get(func["name"], None)
        
        if downstream:
            return {
                "name": func["name"],
                "ast_func": func["function"],
                "children": [build_tree(child) for child in downstream]
            }
        else:
            return {
                "name": func["name"],
                "ast_func": func["function"],
                "children": []
            }

    roots = [func for func in funcs if not func["input_rids"]]
    dag = [build_tree(func) for func in roots]
    
    #TODO prune zero depth nodes? e.g. sticky notes
    return dag

def dag_to_jupyter(dag, output_file):
    import nbformat as nbf
    
    nb = nbf.v4.new_notebook()
    cells = []

    def add_cells(func, depth):
        md_text = f"{'#'*depth} {func['name']}"
        cells.append(nbf.v4.new_markdown_cell(md_text, metadata={"collapsed":True}))
        
        code_text = ast.unparse(func["ast_func"])
        cells.append(nbf.v4.new_code_cell(code_text))

        for c in func["children"]:
            add_cells(c, depth + 1)

    for root in dag:
        if not root["children"]:
            #todo throw error?
            continue
        else:
            add_cells(root, 1)
    
    nb["cells"] = cells
    print(f"num cells: {len(cells)/2}")
    nbf.write(nb, output_file)

def workbook_to_jupyter(repo_dir):

    python_file = repo_dir / "pipeline.py"
    
    with open(python_file, 'r') as f:
        code = f.read()

    #this is a syntax error apparantly. 
    code = code.replace("from pyspark.sql.types import *", "") 
    tree = ast.parse(code)
    
    all_code = [node for node in tree.body]
    named_funcs = {c.name: c for c in all_code if isinstance(c, ast.FunctionDef)}

    func_list = get_func_dependencies(named_funcs.values())
    print(f"found {len(func_list)} functions")
    dag = get_dag(func_list)

    
    notebook_file = repo_dir / "pipeline.ipynb"
    dag_to_jupyter(dag, notebook_file)


if __name__ == "__main__":
    workbook_to_jupyter(
        Path("/Users/frenchth/Foundry_Migration/workbook-migration/pipeline-extractor-python/nidap-export/test_repo/SPAC-v0-9-0-SCIMAP-Workbook")
    )