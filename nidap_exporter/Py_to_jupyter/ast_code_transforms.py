import ast

def _is_high_level_function(node):
    if hasattr(node, "decorator_list"):
        for dec in node.decorator_list:
            if isinstance(dec, ast.Name) and dec.id == "transform_pandas":
                return True
        return False
    return False

def is_foundry_fs_operation(node, logger):
    if isinstance(node, ast.Assign):
        if (
            len(node.targets) == 1 and
            node.value and
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Attribute) and
            isinstance(node.value.func.value, ast.Name) and
            node.value.func.value.id == "Transforms" and
            node.value.func.attr == "get_output"

        ): 
            logger.debug("deleting output = Transforms.get_output()")
            return True
        elif(
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Attribute) and
            node.value.func.attr == 'filesystem'
        ):
            logger.debug("deleting var = var.filesystem()")
            return True
    return False

def is_output_file(node, logger):
    #matches with *_output_file = 'nearest_neighbor_plots.csv'
    if ( 
        isinstance(node, ast.Assign) and
        len(node.targets) == 1 and
        isinstance(node.targets[0], ast.Name) and
        node.targets[0].id.endswith("output_file")
    ):
        logger.debug("is csv_output_file assignment")
        return True
    
    return False

def is_foundry_fs_open(node, logger):
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
        isinstance(node, ast.With) and
        isinstance(node.body[0].value, ast.Call) and
        node.body[0].value.func.value.id == "pickle" and
        node.body[0].value.func.attr == "dump"
    )

def is_pickle_load(node, logger):
    return(
        isinstance(node, ast.With) and
        isinstance(node.body[0].value, ast.Call) and
        node.body[0].value.func.value.id == "pickle" and
        node.body[0].value.func.attr == "load"
    )

def get_output_file_name(func_node, logger):
    output_file_name = ""
    
    for node in func_node.body:
        if is_output_file(node, logger):
            output_file_name = node.targets[0].id
            logger.debug(f"Found output file name: {output_file_name}")
    
    if not output_file_name:
        logger.info(f"No output file name found in function body {func_node.name}, using default name.")
        output_file_name = f"{func_node.name}.pickle"
    
    return output_file_name
            
        

def configure_output_pickles(func_metadata, dependency_list, logger):
    function_body_list = []
    func_node = func_metadata["function"]
    for node in func_node.body: 
        # >>>csv_output_file = 'nearest_neighbor_plots.csv'
        # >>>output = Transforms.get_output()
        # >>>output_fs = output.filesystem()
        # >>>with output_fs.open(csv_output_file, 'w') as f:
        # >>>    final_df.to_csv(f, index=False)
        output_file_name = ""

        if is_foundry_fs_operation(node, logger):
            continue
        elif is_output_file(node, logger):
            continue  
        elif is_pickle_load(node, logger):
            function_body_list.append(node)
        elif is_pickle_dump(node, logger):
            # node.items[0].context_expr = ast.parse(f"open({})")
            # node.items[0].context_expr.args[0].value = func_metadata["output_file_name"]
            node.items[0].context_expr = ast.parse(f"open('{func_metadata['output_file_name']}', 'wb')").body[0].value
            function_body_list.append(node)
            pass
        
            
        else:
            function_body_list.append(node)
            
            
        
        ###save_pickle_to_output(Transforms.get_output(), adata)

    func_node.body = function_body_list
    func_node.decorator_list = []
    return func_node
