import ast
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

# def is_output_file(node, logger):
#     #matches with *_output_file = 'nearest_neighbor_plots.csv'
#     return ( 
#         isinstance(node, ast.Assign) and
#         len(node.targets) == 1 and
#         isinstance(node.targets[0], ast.Name) and
#         node.targets[0].id.endswith("output_file")
#     )
    
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

def is_pickle_dump(node, logger):
    return(
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        isinstance(node.value.func.value, ast.Name) and
        node.value.func.value.id == "pickle" and
        node.value.func.attr == "dump"
    )
    # for node in body_node.walk():
    #     if (
    #         isinstance(node, ast.Expr) and
    #         isinstance(node.value, ast.Call) and
    #         node.value.func.id == "pickle" and
    #         node.value.func.attr == "dump" 
    #     ):
    #         return True
    # return False

def is_pickle_load(node, logger):
    #with open('file.pickle', 'rb') as f:
    #    data = pickle.load(f)
     return(
        isinstance(node, ast.Assign) and
        isinstance(node.value, ast.Call) and
        isinstance(node.value.func, ast.Attribute) and
        isinstance(node.value.func.value, ast.Name) and
        node.value.func.value.id == "pickle" and
        node.value.func.attr == "load"
    )

def get_output_file_info(func_node, logger):
    output_file_name = None
    output_var_name = None
    for node in ast.walk(func_node):
        if is_output_file_name(node, logger):
            output_file_name = node.value.value
            output_var_name = node.targets[0].id
    
    return output_file_name, output_var_name
            
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
            (
                node.targets[0].id == "df" or 
                node.targets[0].id == "dataframe" or
                node.targets[0].id == "input_dataset" or
                node.targets[0].id == "phenotypes" or
                node.targets[0].id == "NIDAP_dataset" 
            ) and
            isinstance(node.value, ast.Name) and
            node.value.id in arg_list
        ):
            mapping[node.targets[0].id] = node.value.id 
    # 
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

def handle_var(node,high_level_func_args, var_mapping, logger):
    #df = function_arg \n summarize_dataframe(df)
    func_args = get_fun_call_args(node, logger)
    
    metadata = {}

    
    for arg in func_args: #arg is straight up used by function call
         if arg in high_level_func_args:
            metadata[arg] = {
                "arg_type": "sub_func_call"
            }
    
    for arg in func_args:
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

def get_func_args_metadata(func_node, logger):
    func_args = [f.arg for f in func_node.args.args]
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
                    "arg_type": "foundry_fs_pickle_load"
                }
                func_args.remove(var)
            elif( var in filesystem_assign.keys() ):
                #matches with {some_var} = {ARG}.filesystem()
                # ... wtih {some_var}.open
                func_args_metadata[filesystem_assign[var]] = {
                    "arg_type": "foundry_fs_pickle_load",
                    "fs_var": var
                }
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
        elif is_subfunction_call(node, logger):
            metadata = handle_var(node, func_args, var_mapping, logger) 
            
            for key in metadata:
                func_args_metadata[key] = metadata[key]
                func_args.remove(key)
        elif (
            is_get_filesystem(node, logger) and
            node.value.func.value.id not in func_args and 
            node.value.func.value.id not in var_mapping
        ):
            #is output_fs = output.filesystem
            func_args_metadata[node.value.func.value.id] = {
                "arg_type": "output_filesystem"
            }

        elif(False):
            if (is_hpc_launch(node, upstream_node, func_args, logger)): 
                if upstream_node:
                    func_args_metadata.append({
                        "arg_name": upstream_node,
                        "arg_type": "hpc_launch"
                    })
                    func_args.remove(upstream_node)
                else:
                    arg_name = [k for k in node.value.keywords if k.arg == "upstream_dataset"][0]
                    func_args_metadata.append({
                        "arg_name": arg_name,
                        "arg_type": "hpc_launch"
                    })
                    func_args.remove(arg_name)
    
    if len(func_args) > 0:
        raise Exception(f"not all args accounted for {func_node.name}\nhave {func_args} unaccounted for")
    
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

def configure_func_calls(func_metadata, arg, arg_metadata, logger):
    #$1
    func_node = func_metadata["function"]
    
    if "func_call_var" in arg_metadata:
        func_call_var = arg_metadata["func_call_var"]
    else:
        func_call_var = arg

    
    code = f"with open({arg}) as f:\n\t{func_call_var} = pickle.load(f)" 
    insert_index = get_import_end_index(func_node, logger)
    func_node.body.insert(insert_index, ast.parse(code).body[0])
    #$1
    
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
                

def get_function_calls(func_dict, logger):
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
        for arg_name in func_dict[func]["input_var_rid_mapping"]:
            if "output_file_name" in func_dict[arg_name]:
                node.value.args.append(ast.Constant(value=func_dict[arg_name]["output_file_name"]))

            else:
                node.value.args.append(ast.Name(id=arg_name))
                

        
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

def remove_foundry_artifacts(func, logger):

    transformer = Remove_Foundry_Artifacts(logger)
    func = transformer.visit(func)
    ast.fix_missing_locations(func)
    
    return func

class Remove_Foundry_Artifacts(NodeTransformer):
    def __init__(self, logger):
        self.logger = logger

    def generic_visit(self, node):
        if ( is_foundry_fs_operation(node, self.logger) or
             is_get_files(node, self.logger)
        ):
            return None
        return super().generic_visit(node)
    


def configure_pickles(func_metadata, arg, arg_metadata, logger):
    
    func_node = func_metadata["function"]


    transformer = Configure_Pickles(arg, func_metadata, logger)
    func_node = transformer.visit(func_node)
    ast.fix_missing_locations(func_node)
    body = []
    # return_node = None

    for node in func_node.body:
        if is_return_variable(node, logger):
            return_var = node.value.id
            new_node = ast.parse(
                f"with open('{func_metadata['output_file_name']}', 'wb') as f:\n\tpickle.dump({return_var},f)"
            ).body[0]
            # logger.info(f"Adding pickle dump for {func_metadata['name']}")  
            body.append(new_node)
            body.append(node)
        else:
            body.append(node)
    func_node.body = body
    func_node.decorator_list = []
    return func_node


class Configure_Pickles(NodeTransformer):

    def __init__(self, arg, func_metadata, logger):
        self.logger = logger
        self.arg = arg
        self.func_metadata = func_metadata
        self.arg_metadata = func_metadata["args_metadata"][arg]


    def generic_visit(self, node):
        # self.logger.info("visit node")
        # self.logger.info(ast.dump(node, indent=4))
        
        if is_output_file_name(node, self.logger):
            node.value.value = self.func_metadata["output_file_name"]

        elif is_foundry_fs_with_open(node, self.logger) and len(node.body) == 1:
            #with {}.open(..., 'w'/'wb') as f
            #    pickle.dump() OR df.to_csv(f)
            # #with {}.open(..., 'r','rb') as f
            #    pickle.load 
            

            # if the pickle op is a dump, OR to_csv we need to check the output file path#
            #        #if the argument to open is a variable, we need to check the #
            # if the pickle op is a load, we need to configure the arg #
            #     this includes updating the open() arg to the input variable#

            # open_type = node.items[0].context_expr.func.args[1].value.
            open_type = node.items[0].context_expr.args[1].value

            if open_type.startswith("w"):
                
                if not isinstance(node.items[0].context_expr.args[0], ast.Name): # is not a variable
                    # self.logger.info(f"func metadata: {self.func_metadata}")
                    # if not node.items[0].context_expr.args[0].id == self.func_metadata["output_var_name"]:
                    #     self.logger.warn(
                    #         f"Function {self.func_metadata['name']} has a with open(variable), but the variable is not accounted for"
                    #     )
                    # else:
                    #     #we dont' need to do anything, this output variable has already been configured
                    #     pass
                # else:
                    # the arg to open is not a string, so we can assume it shoudl be the output file name
                    new = ast.parse(
                        f"open('{self.func_metadata['output_file_name']}', 'wb')"
                    ).body[0].value
                    node.items[0].context_expr = new
                else:
                    file_name, write_type = node.items[0].context_expr.args
                    new = ast.parse(
                        f"open({file_name.id}, '{write_type.value}')"
                    ).body[0].value
                    node.items[0].context_expr = new

        
            elif open_type.startswith("r"):
                #reading in a dataset
                if (
                    "fs_var" in self.arg_metadata and  
                    isinstance(node.items[0].context_expr.func.value, ast.Name) and
                    node.items[0].context_expr.func.value.id == self.arg_metadata["fs_var"] 
                ):
                    node.items[0].context_expr = ast.parse(
                        f"open({self.arg}, 'rb')"
                    ).body[0].value
               
            else:
                self.logger.warn(f"Function has with, open: {self.func_metadata['name']}")
        
        elif ( is_to_pandas(node, self.logger) and
            (
                ("to_pandas_var" in self.arg_metadata and
                node.value.func.value.id == self.arg_metadata["to_pandas_var"]) or
                node.value.func.value.id == self.arg
            )
        ):
            target = node.targets[0].id
            # self.logger.info(f"Found toPandas call ")
            # self.logger.info(ast.dump(node, indent=4))
            return ast.copy_location ( 
                ast.parse(f"with open({self.arg}, 'rb') as f:\n\t{target} = pickle.load(f)"),
                node
            )
        
        
        elif ( is_load_pickle_from_dataset(node, self.logger) and 
               node.value.args[0].id == self.arg
        ):
            var_name = node.targets[0].id
            code = f"with open({self.arg}, 'rb') as f:\n\t{var_name} = pickle.load(f)"
            return ast.copy_location(
                ast.parse(code).body[0],
                node
            )
    
        elif is_save_pickle_to_output(node, self.logger):
            code = f"with open('{self.func_metadata['output_file_name']}', 'wb') as f:"\
                   f"\n\tpickle.dump({node.value.args[1].id}, f)"
            return ast.copy_location(
                ast.parse(code).body[0],
                node
            )
        

        #TODO *.files(), along with assignment


        return super().generic_visit(node)            
        