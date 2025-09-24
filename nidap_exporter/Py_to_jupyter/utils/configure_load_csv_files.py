import ast 
from utils.ast_code_transforms import is_file_path_list, is_foundry_fs_with_open

def configure_load_csv_files(func_metadata, arg,  logger):
    node = func_metadata["function"]

    # func_arg_metadata = func_metadata["args_metadata"][arg]
    transformer = Configure_Load_CSV_Files(arg, func_metadata, logger)
    node = transformer.visit(node)
    ast.fix_missing_locations(node)

    for subnode in node.body:
        if (isinstance(subnode, ast.If) and
            isinstance(subnode.test, ast.UnaryOp) and
            isinstance(subnode.test.op, ast.Not) and
            isinstance(subnode.test.operand, ast.Name) and
            subnode.test.operand.id == "with_spark"
        ):
            # print(ast.unparse(subnode))
            return_var = None   
            for stmt in subnode.body:#ast.walk(subnode):
                if(isinstance(stmt, ast.Return)):
                    # print(f"Found return statement: {ast.unparse(stmt)}")
                    return_var = stmt.value.id
            if not return_var:
                logger.warn("Could not find return variable in with_spark block")
            
            new_node = ast.parse(
                f"with open('{func_metadata['output_file_name']}', 'wb') as f:\n\tpickle.dump({return_var},f)"
            ).body[0]
            subnode.body.insert(-2, new_node)
            break
    # print(f"finished configure_load_csv_file")
   # print(ast.unparse(node)[:500] )
    # print("="*100)
    return node

class Configure_Load_CSV_Files(ast.NodeTransformer):
    def __init__(self, arg, func_metadata, logger):
        
        self.logger = logger
        self.func_metadata = func_metadata
        self.func_arg_metadata = func_metadata["args_metadata"][arg]
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
    
        # file_path_list = blah
        if is_file_path_list(node, self.logger):

            if not (isinstance(node.value, ast.Name) and
                node.value.id in self.func_metadata["args_metadata"]
            ):
                
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
    