import ast
class remove_Foundry_isms(ast.NodeTransformer):

    def __init__(self, function_list):
        super().__init__() 
        self.function_list = function_list

    def _is_high_level_function(node):
        if hasattr(node, "decorator_list"):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name) and dec.id == "transform_pandas":
                    return True
            return False
        return False

    def visit_foundryOutputs(self, func_node):
        
        self.generic_visit(func_node)

        if self._is_high_level_function(func_node):
            #matches output = Transforms.get_output()
            function_name = func_node.name
            function_body_list = []
            
            for node in func_node.body: 
                # >>>csv_output_file = 'nearest_neighbor_plots.csv'
                # >>>output = Transforms.get_output()
                # >>>output_fs = output.filesystem()
                # >>>with output_fs.open(csv_output_file, 'w') as f:
                # >>>    final_df.to_csv(f, index=False)
                output_suffix = ""
                if isinstance(node, ast.Assign):
                    if (
                        len(node.targets) == 1 and
                        isinstance(node.targets[0], ast.Name) and
                        node.targets[0].id == "output"
                    ): # output=...
                        val = node.value
                        if (
                            isinstance(val, ast.Call) and
                            isinstance(val.func, ast.Attribute) and
                            isinstance(val.func.value, ast.Name) and
                            val.func.value.id == "Transforms" and
                            val.func.attr == "get_output"
                        ):
                            continue #delete node
                            
                        else:
                            function_body_list.append(node)
                    elif ( len(node.targets) == 1 and
                        isinstance(node.targets[0], ast.Name) and
                        node.targets[0].id == "csv_output_file"
                    ):
                        output_suffix = node.value.s
                        continue
                    else:
                        function_body_list.append(node)  
                      

                    ###save_pickle_to_output(Transforms.get_output(), adata)
                #elif isinstance(node, ast.Expr):
                #    pass
                else:
                    function_body_list.append(node)

            func_node.body = function_body_list
            return func_node
        else:
            return func_node
    
    # def visit_foundryInputs(self, node):
        #filesystem()
        #   NIDAP_dataset = nearest_neighbor_calculation.filesystem()
        # e.g. NIDAP_dataset = {function input name}.filesystem
        # self.generic_visit(node)

    # def visit_foundrySparkHPC(self, node):
    #     pass
    #Current_SparkContext = SparkContext.getOrCreate()
    #hpc_direct_launch