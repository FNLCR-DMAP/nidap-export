import ast 

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

    def is_with_spark(self, node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id == "with_spark" and
            isinstance(node.value, ast.Constant) and
            node.value.value == True
        )

    def is_run_on_HPC(self, node):
        return (
            isinstance(node, ast.Assign) and
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and 
            node.targets[0].id == "run_on_HPC" and
            isinstance(node.value, ast.Constant) and
            node.value.value == True
        )

    def visit_Assign(self, node):
        
        if self.is_with_spark(node):
            self.logger.warn(f"{self.func_metadata['name']}: Setting with_spark to False")
            node.value = ast.Constant(value=False)
        elif self.is_run_on_HPC(node):
            # self.logger.warn(f"{self.func_metadata['name']}: Setting run_on_HPC to False")
            # node.value = ast.Constant(value=False)
            pass

        return self.generic_visit(node)