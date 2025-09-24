import ast 
from utils.ast_code_transforms import (
    is_foundry_fs_operation,
    is_get_files,
    is_SparkContext,
    is_foundry_fs_with_open
)

def remove_foundry_artifacts(func, logger):

    transformer = Remove_Foundry_Artifacts(logger)
    func = transformer.visit(func)
    ast.fix_missing_locations(func)
    
    return func

class Remove_Foundry_Artifacts(ast.NodeTransformer):
    def __init__(self, logger):
        self.logger = logger
    def is_check_if_fs_not_none(self, node, logger):
        return (
            isinstance(node, ast.If) and
            isinstance(node.test, ast.Compare ) and
            isinstance(node.test.left, ast.Name) and
            node.test.left.id == "output_fs"
        )
    def is_hadoop_path(self, node):
        return (
            isinstance(node, ast.Assign) and
            isinstance(node.value, ast.Attribute) and
            node.value.attr == "hadoop_path"
        )

    def generic_visit(self, node):
        if ( is_foundry_fs_operation(node, self.logger) or
             is_get_files(node, self.logger) or
             is_SparkContext(node, self.logger) or 
             self.is_hadoop_path(node)
        ):
            return None
        elif (
            self.is_check_if_fs_not_none(node, self.logger) and 
            is_foundry_fs_with_open(node.body[0], self.logger)
        ):
            node = node.body[0]

        return super().generic_visit(node)


