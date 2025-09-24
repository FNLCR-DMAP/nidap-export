import ast
from importlib.resources import files
import pathlib
from ast import NodeTransformer

def is_foundry_filesystem_method(node, logger):
    
    return(
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        node.value.func.attr == 'filesystem'
    )

def get_filesystem_assign(func_node, arg_list, logger):
    assigns = {}
    
    for node in ast.walk(func_node):
        if ( is_foundry_filesystem_method(node, logger) and node.value.func.value.id in arg_list): 
            assigns[node.targets[0].id] = node.value.func.value.id 
            
    return assigns

def is_transforms_get_output(node, logger):
    return(  
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        node.value and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        isinstance(node.value.func.value, ast.Name) and
        node.value.func.value.id == "Transforms" and
        node.value.func.attr == "get_output"
    )

def is_foundry_fs_operation(node, logger):
    
    if (is_transforms_get_output(node,logger) ): 
        
        return True
    elif(is_foundry_filesystem_method(node,logger)):
        
        return True
    return False

    
def is_foundry_fs_with_open(node, logger):
    #matches with output_fs.open('{outfile name}', 'wb') as f:
    
    return (
        isinstance(node, ast.With) and
        len(node.items) == 1 and
        isinstance(node.items[0].context_expr, ast.Call) and
        isinstance(node.items[0].context_expr.func, ast.Attribute) and
        isinstance(node.items[0].context_expr.func.value, ast.Name) and
        node.items[0].context_expr.func.attr == "open" 
    )

def get_output_file_info(func_node, func_name, repo_dir, logger):
    foundry_data = repo_dir / "foundry_data"
    data_dir = foundry_data / func_name
    
    if data_dir.exists() and data_dir.is_dir():
        # files = [f for f in list(data_dir.iterdir()) if f.suffix not in  [".parquet", ".log"]]
        downloaded_files = [f for f in list(data_dir.glob(f"{func_name}-*")) if f.suffix not in  [".parquet", ".log"]]
        combined_files = [f for f in list(data_dir.glob(f"{func_name}.*")) if f.suffix not in  [".parquet", ".log"]]
        
        if len(combined_files) == 1:
            output_file_name = combined_files[0].name
        elif len(combined_files) > 1:
            logger.warn(f"Multiple output files found for function {func_name} in {data_dir}, defaulting to the first")
            output_file_name = combined_files[0].name
        else:
            # print(f"func name {func_name}")
            # print(files)
            if len(downloaded_files) == 1:
                output_file_name = downloaded_files[0].name
            elif len(downloaded_files) > 1:
                logger.warn(f"Multiple output files found for function {func_name} in {data_dir}")
                output_file_name = downloaded_files[0].name

    else:
        foundry_files = list(foundry_data.glob(f"{func_name}-*"))
        if len(foundry_files) == 1:
            output_file_name = foundry_files[0].name
        elif len(foundry_files) > 1:
            logger.warn(f"Multiple foundry files found for function {func_name} in {foundry_data}")
            output_file_name = foundry_files[0].name
        else:
            output_file_name = None
    # output_file_name = None

    output_var_name = None
    for node in ast.walk(func_node):
        if is_output_file_name(node, logger):
            output_file_name = node.value.value
            output_var_name = node.targets[0].id
    
    return output_file_name, output_var_name

def get_upstream_assign(func_node, args, logger):
    
    for node in ast.walk(func_node):
        
        if ( isinstance(node, ast.Assign) and
             len(node.targets) == 1 and
             isinstance(node.targets, ast.Name) and
             node.targets[0].id == "upstream_node" and
             isinstance(node.value, ast.Name) and
             node.value.id in args
        ):
            return node.value.id
    return None

def is_load_pickle_from_dataset(node, logger):
    
    return (
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Name) and
        node.value.func.id == "load_pickle_from_dataset"
    )
    
def get_is_variable_mapping(func_node, arg_list, logger):
    mapping = {}
    for node in ast.walk(func_node):
        
        if (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            isinstance(node.value, ast.Name) and
            node.value.id in arg_list
        ):
            mapping[node.targets[0].id] = node.value.id 
    return mapping

def is_to_pandas(node, logger):
    return(
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        node.value.func.attr == "toPandas"
        )
    
def get_fun_call_args(node, logger):
    
    pos_args = []
    kw_args = []
    
    if (isinstance(node, ast.Assign) and 
        isinstance(node.value, ast.Call)
    ):
        pos_args = [arg.id for arg in node.value.args if isinstance(arg, ast.Name)]
        kw_args = [kw.value.id for kw in node.value.keywords if isinstance(kw.value, ast.Name)]
    
    return pos_args + kw_args

def is_subfunction_call(node, logger):

    return (
        (
            isinstance(node, ast.Expression) or
            isinstance(node, ast.Assign) 
        ) and
        isinstance(node.value, ast.Call)
    )      

def check_sub_function_vals(node,high_level_func_args, var_mapping, logger):
    #df = function_arg \n summarize_dataframe(df)
    sub_func_args = get_fun_call_args(node, logger)
    
    metadata = {}
    
    for arg in sub_func_args: #arg is straight up used by function call
         if arg in high_level_func_args:
            metadata[arg] = {
                "arg_type": "sub_func_call"
            }
    
    for arg in sub_func_args:
        if arg in var_mapping.keys():
            metadata[var_mapping[arg]] = {  
                "arg_type": "sub_func_call",
                "func_call_var": arg
            }
    
    return metadata

def is_get_filesystem(node, logger):
    return (
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        node.value.func.attr == "filesystem"
    )
def is_get_files(node, logger):
    return (
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        node.value.func.attr == "files"
    )
def is_output_file_name(node, logger):
    #matches csv_output_file = 'nearest_neighbor_plots.csv'
    # logger.info(f"is output file {ast.unparse(node)}")
    return (
        isinstance(node, ast.Assign) and
        isinstance(node.targets[0], ast.Name) and
        node.targets[0].id.endswith("output_file") and
        isinstance(node.value, ast.Constant)
    )

def is_arg_assign(node, arg, logger):
    return (
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.value, ast.Name) and
        node.value.id == arg        
    )
def extract_positional_args(func_node):
    args = func_node.args
    positional_args = []
    # Handle posonlyargs (Python 3.8+)
    posonlyargs = getattr(args, "posonlyargs", [])
    # Regular args (positional-or-keyword)
    regular_args = args.args
    # Combine all positional args
    all_positional_args = posonlyargs + regular_args

    # Defaults apply to the last N positional args
    num_defaults = len(args.defaults)
    num_total = len(all_positional_args)
    num_without_defaults = num_total - num_defaults

    # Extract only the ones without defaults
    for i in range(num_without_defaults):
        positional_args.append(all_positional_args[i].arg)

    return positional_args

# def get_func_args_metadata(func_node,func_metadata, logger):
#     func_args = extract_positional_args(func_node)
#     func_args_metadata = {}
#     for arg in func_args:
#         if arg not in func_metadata:
#             pass
#         out_file = func_metadata[arg]["output_file_name"]
#         if out_file:
#             ext = pathlib.Path(out_file).suffix
#             if ext == ".pickle":
#                 func_args_metadata[arg] = {
#                     "arg_type": "pickle",
#                 }
#             elif ext == ".csv":
#                 func_args_metadata[arg] = {
#                     "arg_type": "csv",
#                 }
#             else:
#                 logger.warn(f"unknown file type for arg {arg} with file {out_file}: {ext}, defaulting to generic_open")
    
#     return func_args_metadata


def get_func_args_metadata(func_node, logger):
    func_args = extract_positional_args(func_node)
    
    func_args_metadata = {}
    output_arg_metadata = {}
    upstream_node = get_upstream_assign(func_node, func_args, logger)
    var_mapping = get_is_variable_mapping(func_node, func_args, logger)
    filesystem_assign = get_filesystem_assign(func_node, func_args, logger)
    
    for node in ast.walk(func_node):
        if ( is_foundry_fs_with_open(node, logger) ):
            # matches with {}.open:
            var = node.items[0].context_expr.func.value.id
            if var in func_args:
                #matches with {FUNC ARG}.open:
                func_args_metadata[var] = {
                    "arg_type": ""
                }
                func_args.remove(var)
            elif( var in filesystem_assign.keys() ):
                #matches with {some_var} = {ARG}.filesystem()
                # ... wtih {some_var}.open
                func_args_metadata[filesystem_assign[var]] = {
                    "arg_type": "foundry_fs_pickle_load",
                    "fs_var": var
                }
                if filesystem_assign[var] in func_args:
                    func_args.remove(filesystem_assign[var])
        
        elif (is_to_pandas(node, logger) ):
            # matches blah = input.toPandas()
            if node.value.func.value.id in func_args:
                func_args_metadata[node.value.func.value.id] = {
                    "arg_type": "spark_to_pandas"
                }
                func_args.remove(node.value.func.value.id)
            elif (node.value.func.value.id in var_mapping):
                # matches blah = input.toPandas()
                func_args_metadata[var_mapping[node.value.func.value.id]] = {
                    "arg_type": "spark_to_pandas",
                    "to_pandas_var": node.value.func.value.id
                }
                func_args.remove(var_mapping[node.value.func.value.id])
        elif is_load_pickle_from_dataset(node, logger): #load_pickle_from_dataset
            func_args_metadata[node.value.args[0].id] = {
                "arg_type": "load_pickle_from_dataset"
            }
            func_args.remove(node.value.args[0].id)
        elif any(is_arg_assign(node, a, logger) for a in func_args):
            #matches with var = ARG
            func_args_metadata[node.value.id] = {
                "arg_type": "arg_assign"
            }
            func_args.remove(node.value.id)

        elif (
            is_get_filesystem(node, logger) and
            node.value.func.value.id not in func_args and 
            node.value.func.value.id not in var_mapping
        ):
            #is output_fs = output.filesystem
            func_args_metadata[node.value.func.value.id] = {
                "arg_type": "output_filesystem"
            }
    
    if len(func_args) > 0:
        # raise Exception(f"not all args accounted for {func_node.name}\nhave {func_args} unaccounted for")
        logger.warn(f"Function {func_node.name} not all args accounted for. \n\thave {func_args} unaccounted for, making generic open")
        for arg in func_args:
            func_args_metadata[arg] = {
                "arg_type": "generic_open"
            }

    return func_args_metadata

def get_import_end_index (func_node, logger):
    """
    Returns the index in the function body where imports end.
    """
    for i, node in enumerate(func_node.body):
        if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
            continue
        else:
            return i
    return 0 

# def configure_func_calls(func_metadata, arg, arg_metadata, root_nodes, logger):
def configure_func_calls(func_dict, func_name, arg, root_nodes, logger):

    func_metadata = func_dict[func_name]
    arg_metadata = func_metadata["args_metadata"][arg]
    func_node = func_metadata["function"]
    
    if arg not in root_nodes:
        
        if "func_call_var" in arg_metadata:
            func_call_var = arg_metadata["func_call_var"]
        else:
            func_call_var = arg
        
        insert_index = get_import_end_index(func_node, logger)
        
        if func_dict[arg]["output_file_name"].endswith(".pickle"):
            code = f"with open({arg}, 'rb') as f:\n\t{func_call_var} = pickle.load(f)" 
        elif func_dict[arg]["output_file_name"].endswith(".csv"):
            code = f"with open({arg}, 'r') as f:\n\t{func_call_var} = pd.read_csv(f)"
        else:
            raise Exception(f"unknown input file type, input file: {func_dict[arg]['output_file_name']}")

        func_node.body.insert(insert_index, ast.parse(code).body[0])
        
        if "func_call_var" in arg_metadata:
            new_body = []
            # 
            for node in func_node.body:
                # 
                if ( isinstance(node, ast.Assign) and
                    len(node.targets) == 1 and
                    isinstance(node.value, ast.Name) and
                    node.targets[0].id == arg_metadata["func_call_var"]
                ):
                    continue
                else:
                    new_body.append(node)
            func_node.body = new_body
    
    return func_node

def configure_default_args(func_node, logger):
    default_args = {}
    func_body = []

    default_param_end_index = 0
    import_end_index = get_import_end_index(func_node, logger)

    for index, item in enumerate(func_node.body):
        
        if index < import_end_index:
            func_body.append(item)
        else:
            if ( is_foundry_filesystem_method(item, logger) or # blank = var.filesystem() 
                 is_foundry_fs_with_open(item, logger) 
            ):
                func_body.append(item)
            elif isinstance(item, ast.Assign) and len(item.targets) == 1:
        
                target = item.targets[0]
                value = item.value
                if ( isinstance(value, ast.Constant) or
                     (
                        isinstance(value, ast.UnaryOp) and 
                        isinstance(value.op, ast.USub)
                     ) or 
                     isinstance(value, ast.List)
                ):
        

                    default_args[target.id] = value
                    continue
                else:
                    default_param_end_index = index
                    
                    break
            else:
                
                default_param_end_index = index
                break
    
    for kwarg_name, default_val in default_args.items():
        func_node.args.args.append(ast.arg(arg=kwarg_name, annotation=None))
        func_node.args.defaults.append(default_val)
    
    func_body.extend(func_node.body[default_param_end_index:])
    func_node.body = func_body

    
    return func_node, default_args

def configure_non_function_output(func_node, args_metadata, logger):
    
    for node in ast.walk(func_node):
        if is_foundry_fs_with_open(node, logger):
            # body = node.body
            if (
                len(node.body) == 1 and
                isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Call) and
                node.body[0].value.func.attr == "to_csv" and
                node.body[0].value.func.value.id not in args_metadata
            ):
                node.items[0].context_expr.func = ast.Name(id="open", ctx=ast.Load())
    
    
    return func_node


def get_function_calls(func_dict, roots, logger):
    for func in func_dict:
        if "function" not in func_dict[func]: #is a manual datastet
            continue

        node = ast.Expr(
            value=ast.Call(
                func=ast.Name(id=func_dict[func]["name"], ctx=ast.Load()),
                args=[],
                keywords=[]
            )
        )
        
        # for arg_name in func_dict[func]["input_var_rid_mapping"]:
        args = [a.arg for a in func_dict[func]["function"].args.args ]
        len_kwargs = len(func_dict[func]["function"].args.defaults)
        
        if len_kwargs > 0:
            arg_order = args[:-len_kwargs]
        else:
            arg_order = args

        for arg_name in arg_order:

            if "output_file_name" in func_dict[arg_name] and not arg_name in roots:
                node.value.args.append(ast.Constant(value=func_dict[arg_name]["output_file_name"]))

            else:
                node.value.args.append(ast.Name(id=arg_name))
        
        

        for arg_name, arg_value in zip(args[-len_kwargs:], func_dict[func]["function"].args.defaults):
            #TODO remove this when run_on_HPC is working
            if arg_name == "run_on_HPC":
                arg_value = ast.Constant(value=False)
            new_keyword = ast.keyword(arg=arg_name, value=arg_value)
            node.value.keywords.append(new_keyword)
        
        func_dict[func]["func_call"] = node
    
    return func_dict
            
def is_load_pickle_from_dataset(node, logger):
    return (
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Name) and
        node.value.func.id == "load_pickle_from_dataset"
    )

def is_save_pickle_to_output(node, logger):

    return (
        isinstance(node, ast.Expr) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Name) and
        node.value.func.id == "save_pickle_to_output"
    )

def is_return_variable(node, logger):
    return (
        isinstance(node, ast.Return) and
        isinstance(node.value, ast.Name)
    )

def is_file_path_list( node, logger):
    return(
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.targets[0], ast.Name) and
        node.targets[0].id == ("file_paths_list")
    )
def is_SparkContext(node, logger):
    return (
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        isinstance(node.value.func.value, ast.Name) and
        node.value.func.value.id == "SparkContext"
    )



def has_import_pickle(func_node):

    for node in ast.walk(func_node):
        # Match: import pickle
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'pickle':
                    return True
        # Match: from pickle import ...
        elif isinstance(node, ast.ImportFrom):
            if node.module == 'pickle':
                return True
    return False

def has_import_pandas(func_node):

    for node in ast.walk(func_node):
        
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'pandas':
                    return True
        
        elif isinstance(node, ast.ImportFrom):
            if node.module == 'pandas':
                return True
    return False
def add_import(node):

    if not has_import_pickle(node):
        node.body.insert(0, ast.parse("import pickle").body[0])
    if not has_import_pandas(node):
        node.body.insert(0, ast.parse("import pandas as pd").body[0])
    return node


    

def is_hpc_launch(node, has_upstream_node_assign, func_args, logger):
    if has_upstream_node_assign:
        kw_target = ["upstream_node"]
    else:
        kw_target = func_args
    
    return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.value, ast.Call) and
            node.value.func.id == "hpc_direct_launch" and
            any([
                k.arg == "upstream_node" and k.value in kw_target 
                for k in node.value.keywords
            ])
    )