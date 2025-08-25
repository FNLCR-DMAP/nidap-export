import ast
def configure_imports(funcs, logger):
    for func in funcs:
        if "function" in funcs[func]:
            transformer = ImportTransformer(logger)
            funcs[func]["function"] = transformer.visit(funcs[func]["function"])
            ast.fix_missing_locations(funcs[func]["function"])
    return funcs


class ImportTransformer(ast.NodeTransformer):
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
