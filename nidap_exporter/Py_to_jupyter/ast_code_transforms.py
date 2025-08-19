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

# def is_pickle_dump(node, logger):
#     return(
#         isinstance(node, ast.Assign) and
#         isinstance(node.value, ast.Call) and
#         isinstance(node.value.func, ast.Attribute) and
#         isinstance(node.value.func.value, ast.Name) and
#         node.value.func.value.id == "pickle" and
#         node.value.func.attr == "dump"
#     )


# def is_pickle_load(node, logger):
#     #with open('file.pickle', 'rb') as f:
#     #    data = pickle.load(f)
#      return(
#         isinstance(node, ast.Assign) and
#         isinstance(node.value, ast.Call) and
#         isinstance(node.value.func, ast.Attribute) and
#         isinstance(node.value.func.value, ast.Name) and
#         node.value.func.value.id == "pickle" and
#         node.value.func.attr == "load"
#     )

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


def get_func_args_metadata(func_node, logger):
    func_args = extract_positional_args(func_node)
    func_args_metadata = {}
    output_arg_metadata = {}
    upstream_node = get_upstream_assign(func_node, func_args, logger)
    var_mapping = †(func_node, func_args, logger)
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
        elif any(is_arg_assign(node, a, logger) for a in func_args):
            #matches with var = ARG
            func_args_metadata[node.value.id] = {
                "arg_type": "arg_assign"
            }
            func_args.remove(node.value.id)

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

        func_node.body.insert(insert_index, ast.parse(code).body[0])
        
        if "func_call_var" in arg_metadata:
            new_body = []
            # print(f"looking to remove {arg_metadata['func_call_var']}")
            for node in func_node.body:
                # print(ast.unparse(node))
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
    #TODO pin_colored_interactive_spatial_plot among others not working 
    # has lines stratify_by = text_to_value('None', param_name='Stratify By')
    #.          defined_color_map = text_to_value('_spac_colors', param_name='Define Label Color Mapping')
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

def remove_foundry_artifacts(func, logger):

    transformer = Remove_Foundry_Artifacts(logger)
    func = transformer.visit(func)
    ast.fix_missing_locations(func)
    
    return func

def is_file_path_list(self, node, logger):
    return(
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.targets[0], ast.Name) and
        node.targets[0].id == ("file_paths_list")
    )
class Remove_Foundry_Artifacts(NodeTransformer):
    def __init__(self, logger):
        self.logger = logger
    def is_check_if_fs_not_none(self, node, logger):
        return (
            isinstance(node, ast.If) and
            isinstance(node.test, ast.Compare ) and
            isinstance(node.test.left, ast.Name) and
            node.test.left.id == "output_fs"
        )

    def generic_visit(self, node):
        if ( is_foundry_fs_operation(node, self.logger) or
             is_get_files(node, self.logger)
        ):
            return None
        elif (
            self.is_check_if_fs_not_none(node, self.logger) and 
            self.is_foundry_fs_with_open(node.body[0], self.logger)
        ):
            node = node.body[0]

        return super().generic_visit(node)

def configure_pickles(func_metadata, arg, root_nodes, logger):
    
    
    func_node = func_metadata["function"]

    transformer = Configure_Pickles(arg, func_metadata, root_nodes, logger)
    func_node = transformer.visit(func_node)
    ast.fix_missing_locations(func_node)
    body = []
    # return_node = None

    for node in func_node.body:
        if is_return_variable(node, logger): #this is outside configure_pickles because we need to add a line, not modify
                
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

    def __init__(self, arg, func_metadata,root_nodes, logger):
        self.logger = logger
        self.arg = arg
        self.func_metadata = func_metadata
        self.arg_metadata = func_metadata["args_metadata"][arg]
        self.uses_root_node = self.arg in root_nodes

    def is_check_vector(self, node, logger):
        return(
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id == 'vector_check' 
        )

    def is_output_format_string(self, node):
        if ( isinstance(node, ast.Assign) and
             isinstance(node.value, ast.JoinedStr)
        ):
            fmt_values = [v for v in node.value.values if isinstance(v, ast.FormattedValue)]
            for v in fmt_values:
                for n in ast.walk(v):
                    if isinstance(n, ast.Name) and n.id == "Transforms":
                        return True

        return False
    
    def replace_with_pickle_load(self, node, logger):
        return (
            (
                is_to_pandas(node, self.logger) and
                (
                    (
                        "to_pandas_var" in self.arg_metadata and
                        node.value.func.value.id == self.arg_metadata["to_pandas_var"]
                    ) or
                    node.value.func.value.id == self.arg
                )
            ) or (
                is_load_pickle_from_dataset(node, self.logger) and 
                node.value.args[0].id == self.arg
            ) or (
                is_arg_assign(node, self.arg, self.logger)
            )
        )

    def generic_visit(self, node):
        # if self.func_metadata["name"] == "manual_phenotyping":
            # self.logger.info(f"visit {ast.unparse(node)}")

        if is_output_file_name(node, self.logger):
    
            node.value.value = self.func_metadata["output_file_name"]
        elif self.is_output_format_string(node):
            node.value = ast.Constant(f"{node.value.values[0].value}{self.func_metadata['name']}.png")
        elif is_foundry_fs_with_open(node, self.logger) and len(node.body) == 1:
            
            #with {}.open(..., 'w'/'wb') as f
            #    pickle.dump() OR df.to_csv(f)
            # #with {}.open(..., 'r','rb') as f
            #    pickle.load 
            # if the pickle op is a dump, OR to_csv we need to check the output file path#
            #        #if the argument to open is a variable, we need to check the #
            # if the pickle op is a load, we need to configure the arg #
            #     this includes updating the open() arg to the input variable#

            
            open_type = node.items[0].context_expr.args[1].value

            if open_type.startswith("w"):
                
                if not isinstance(node.items[0].context_expr.args[0], ast.Name): # is not a variable
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
        
        elif self.replace_with_pickle_load( node, self.logger):
            target = node.targets[0].id
            
            if self.uses_root_node:
                return ast.copy_location (
                    ast.parse(f"{target} = {self.arg}"),
                    node
                )
            else:
                return ast.copy_location ( 
                    ast.parse(f"with open({self.arg}, 'rb') as f:\n\t{target} = pickle.load(f)"),
                    node
                )
    
        elif is_save_pickle_to_output(node, self.logger):
            
            code = f"with open('{self.func_metadata['output_file_name']}', 'wb') as f:"\
                   f"\n\tpickle.dump({node.value.args[1].id}, f)"
            return ast.copy_location(
                ast.parse(code).body[0],
                node
            )

        elif self.is_check_vector(node, self.logger):
            
            # this logic already updates the nidap dataset replacement above,
            # it's easier to just set the value to True here 
            node.value = ast.Constant(value=True)
            
        
            

        return super().generic_visit(node)

def configure_load_csv_files(node,  arg, func_arg_metadata, logger):

    transformer = Configure_Load_CSV_Files(arg, func_arg_metadata, logger)
    # print(f"configuring node: {ast.unparse(node)}")
    node = transformer.visit(node)
    # print(f"configured node: {ast.unparse(node)}")
    ast.fix_missing_locations(node)
    
    return node

class Configure_Load_CSV_Files(NodeTransformer):
    def __init__(self, arg, func_arg_metadata, logger):
        
        self.logger = logger
        self.func_arg_metadata = func_arg_metadata
        self.arg = arg
    def is_select_path_collect(self, node, logger):
        return (        
            isinstance(node, ast.Assign) and
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Attribute) and
            isinstance(node.value.func.value, ast.Call) and
            isinstance(node.value.func.value.func, ast.Attribute) and
            node.value.func.value.func.attr == 'select' and
            node.value.func.attr == 'collect'
        )

    def visit_Assign(self, node):
        if (self.is_select_path_collect(node, self.logger) and
            self.func_arg_metadata["arg_type"] == "foundry_fs_pickle_load"
        ):
            
            new_node = ast.parse(f"file_paths_list = {self.arg}").body[0]

            return ast.copy_location(new_node, node)
        
        if is_file_path_list(node, self.logger):
            return None
        return self.generic_visit(node)

    def visit_With(self, node):
        if is_foundry_fs_with_open(node, self.logger):
            withitem = node.items[0]
            new_node = ast.Call(
                func=ast.Name(id='open', ctx=ast.Load()),
                args=[
                    withitem.context_expr.args[0],  # file_name
                    ast.Constant(value='r')
                ],
                keywords=[]
            )
            withitem.context_expr = new_node
        return self.generic_visit(node)
    

def spark_to_pandas_root_nodes(funcs, root_nodes, logger):
    for node in root_nodes:
        if "function" in funcs[node]:
            transformer = SparkToPandasTransformer()
            funcs[node]["function"] = transformer.visit(funcs[node]["function"])
            ast.fix_missing_locations(funcs[node]["function"])
    
    return funcs

    
class SparkToPandasTransformer(ast.NodeTransformer):
    def __init__(self):
        self.columns = []

    def visit_FunctionDef(self, node):
        new_body = []

        for stmt in node.body:
            # Detect schema assignment and extract column names
            if (
                isinstance(stmt, ast.Assign) and
                isinstance(stmt.value, ast.Call) and
                isinstance(stmt.value.func, ast.Name) and
                stmt.value.func.id == 'StructType'
            ):
                # Extract column names from StructField
                fields = stmt.value.args[0].elts
                self.columns = [
                    field.args[0].value
                    for field in fields
                    if isinstance(field, ast.Call) and field.func.id == 'StructField'
                ]
                continue  # Skip adding this line to the new body

            # Replace Spark createDataFrame return with pd.DataFrame
            elif isinstance(stmt, ast.Return):
                spark_call = stmt.value
                if (
                    isinstance(spark_call, ast.Call) and
                    isinstance(spark_call.func, ast.Attribute) and
                    spark_call.func.attr == 'createDataFrame'
                ):
                    data_arg = spark_call.args[0]
                    new_call = ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='pd', ctx=ast.Load()),
                            attr='DataFrame',
                            ctx=ast.Load()
                        ),
                        args=[],
                        keywords=[
                            ast.keyword(arg='data', value=data_arg),
                            ast.keyword(
                                arg='columns',
                                value=ast.List(
                                    elts=[ast.Constant(value=col) for col in self.columns],
                                    ctx=ast.Load()
                                )
                            )
                        ]
                    )
                    new_body.append(ast.Return(value=new_call))
                    continue  # Done with this stmt

            # Keep anything else (just in case)
            new_body.append(stmt)

        node.body = new_body
        return node
    

def configure_imports(funcs, logger):
    for func in funcs:
        if "function" in funcs[func]:
            transformer = ImportTransformer(logger)
            funcs[func]["function"] = transformer.visit(funcs[func]["function"])
            ast.fix_missing_locations(funcs[func]["function"])
    return funcs


class ImportTransformer(NodeTransformer):
    def __init__(self, logger):
        self.logger = logger

    def is_code_workbook_utils_import(self, node, logger):
        return(
            isinstance(node, ast.ImportFrom) and
            node.module == "code_workbook_utils.utils" 
        )    

    def is_pyspark_import(self, node, logger):
        return(
            isinstance(node, ast.ImportFrom) and
            node.module == "pyspark" 
        )    

    def is_hpc_connector_import(self, node, logger):
        return(
            isinstance(node, ast.ImportFrom) and
            node.module.startswith("hpc_connector_addons")
        )
    
    def generic_visit(self, node):
        if self.is_code_workbook_utils_import(node, self.logger):
            imports = [a.name for a in node.names]
            exclude = ["save_pickle_to_output","load_pickle_from_dataset"]
            for e in exclude:
                if e in imports:
                    imports.remove(e)

            node.module = "spac.templates.template_utils"
            node.names = [ast.alias(name=i) for i in imports]
        if self.is_pyspark_import(node, self.logger):
            return None
        if self.is_hpc_connector_import(node, self.logger):
            return None
        return super().generic_visit(node)
        

def configure_hpc_call(funcs, logger):
    for func in funcs:
        if "function" in funcs[func]:
            transformer = Configure_HPC_Call(funcs[func], logger)
            funcs[func]["function"] = transformer.visit(funcs[func]["function"])
            ast.fix_missing_locations(funcs[func]["function"])
    return funcs

class Configure_HPC_Call(NodeTransformer):

    def __init__(self, func_metadata, logger):
        self.logger = logger
        self.func_metadata = func_metadata
        self.upstream_node = None
        for arg_name in func_metadata["args_metadata"]:
            if func_metadata["args_metadata"][arg_name]["arg_type"] == "hpc_launch":
                self.upstream_node = arg_name

    def is_if_run_on_hpc_block(self, node):
        return (
            isinstance(node, ast.If) and
            isinstance(node.test, ast.Name) and
            node.test.id == "run_on_hpc"
        )
    
    def should_remove(self, node):
        return (
            isinstance(node, ast.Try) or (
                isinstance(node, ast.If) and
                isinstance(node.test, ast.Name) and
                node.test.id == "batch_mode" 
            ) or (

                isinstance(node, ast.If) and
                isinstance(node.test, ast.Subsctript) and
                isinstance(node.test.value, ast.Name) and
                node.test.value.id == "monitor_result" 
            ) or self.is_input_file_name(node)
        )
    
    def is_input_file_name(self, node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id.endswith("input_file_name") and
            isinstance(node.value, ast.Constant)
        )
    
    def is_upstream_node(self, node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id == "upstream_node" 
        )
    
    def generic_visit(self, node):
        
        if self.is_if_run_on_hpc_block(node):
            new_body = []
            input_file_var = ""
            for child in node.body:
                if self.should_remove(child):
                    continue
                elif self.is_upstream_node(child):
                    input_file_var = child.value.id
                else: 
                    new_body.append(child)
            
            
            new_body.append(ast.parse(f'''input_file_name={input_file_var}''').body[0])
            new_body.append(
                
            )
            node.body = new_body
                
        
        return super().generic_visit(node)
    

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

def configure_default_vals(funcs, logger):
    for func in funcs:
        if "function" in funcs[func]:
            transformer = Configure_Default_Vals(logger, funcs[func])
            funcs[func]["function"] = transformer.visit(funcs[func]["function"])
            ast.fix_missing_locations(funcs[func]["function"])
    return funcs

class Configure_Default_Vals(ast.NodeTransformer):
    def __init__(self, logger, func_metadata):
        self.logger = logger
        self.func_metadata = func_metadata

    def is_with_spark(node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            node.targets[0].id == "with_spark" and
            isinstance(node.value, ast.Constant) and
            node.value.value == True
        )

    def is_run_on_HPC(node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            node.targets[0].id == "run_on_HPC" and
            isinstance(node.value, ast.Constant) and
            node.value.value == True
        )

    def visit_Assign(self, node):
        
        if self.is_with_spark(node):
            self.logger.warn(f"{self.func_metadata['name']}: Setting with_spark to False")
            node.value = ast.Constant(value=False)
        elif self.is_run_on_HPC(node):
            self.logger.warn(f"{self.func_metadata['name']}: Setting run_on_HPC to False")
            node.value = ast.Constant(value=False)

        return self.generic_visit(node)