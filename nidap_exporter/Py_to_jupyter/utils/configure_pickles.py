import ast

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

class Configure_Pickles(ast.NodeTransformer):

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
    
    def should_replace_with_pickle_load(self, node, logger):
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

            
            open_args = node.items[0].context_expr.args
            if len(open_args) == 1:
                open_type = "rb"
                self.logger.warn(
                    f"No open type found\n\tfunction {self.func_metadata['name']}"
                    f"is opening arg {self.arg} with no type, assigning 'rb'"
                )
            else:
                open_type = open_args[1].value

            if open_type.startswith("w"):
                open_type = "wb"
                if not isinstance(node.items[0].context_expr.args[0], ast.Name): # is not a variable
                    new = ast.parse(
                        f"open('{self.func_metadata['output_file_name']}', '{open_type}')"
                    ).body[0].value
                    node.items[0].context_expr = new
                else:
                    file_name = node.items[0].context_expr.args[0]
                    new = ast.parse(
                        f"open({file_name.id}, '{open_type}')"
                    ).body[0].value
                    node.items[0].context_expr = new

        
            elif open_type.startswith("r"):
                open_type = "rb"
                #reading in a dataset
                if (
                    "fs_var" in self.arg_metadata and  
                    isinstance(node.items[0].context_expr.func.value, ast.Name) and
                    node.items[0].context_expr.func.value.id == self.arg_metadata["fs_var"] 
                ):
                    node.items[0].context_expr = ast.parse(
                        f"open({self.arg}, '{open_type}')"
                    ).body[0].value
               

        
        elif self.should_replace_with_pickle_load( node, self.logger):
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