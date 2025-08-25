import ast
import shutil 
def configure_hpc_call(funcs, config_path, repo_dir, logger):
    shutil.copy("./submit_hpc_job.py", repo_dir)
    shutil.copy("./transformer_config.cfg", repo_dir)

    for func in funcs:
        if "function" in funcs[func]:
            transformer = Configure_HPC_Call(funcs[func], config_path, repo_dir, logger)
            funcs[func]["function"] = transformer.visit(funcs[func]["function"])
            ast.fix_missing_locations(funcs[func]["function"])
    return funcs

class Configure_HPC_Call(ast.NodeTransformer):

    def __init__(self, func_metadata, config_path, repo_dir, logger):
        self.logger = logger
        self.func_metadata = func_metadata
        self.upstream_node = None
        self.config_path = config_path
        self.repo_dir = repo_dir
        for arg_name in func_metadata["args_metadata"]:
            if func_metadata["args_metadata"][arg_name]["arg_type"] == "hpc_launch":
                self.upstream_node = arg_name

    def is_if_run_on_hpc_block(self, node):
        return (
            isinstance(node, ast.If) and
            isinstance(node.test, ast.Name) and
            node.test.id == "run_on_HPC"
        )
    
    def should_remove(self, node):
        return (
            isinstance(node, ast.Try) or (
                isinstance(node, ast.If) and
                isinstance(node.test, ast.Name) and
                node.test.id == "batch_mode" 
            ) or (

                isinstance(node, ast.If) and
                isinstance(node.test, ast.Subscript) and
                isinstance(node.test.value, ast.Name) and
                node.test.value.id == "monitor_result" 
            ) or self.is_input_file_name(node)
            or (
                isinstance(node, ast.Return) and
                isinstance(node.value, ast.Constant) and 
                node.value.value == None
            )
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
    def is_pickle_load_upstream_node(self, node):
        return(
            isinstance(node, ast.Module) and 
            isinstance(node.body[0], ast.With) and
            isinstance(node.body[0].body[0], ast.Assign) and
            len(node.body[0].body[0].targets) == 1 and
            node.body[0].body[0].targets[0].id == "upstream_node"
        )
    
    
    def generic_visit(self, node):
        
        if self.is_if_run_on_hpc_block(node):
            new_body = []
            new_body.append(ast.parse("from submit_hpc_job import submit_hpc_job").body[0])
            input_file_var = ""
            for child in node.body:
                if self.should_remove(child):
                    continue
                elif self.is_pickle_load_upstream_node(child):\
                    input_file_var = child.body[0].items[0].context_expr.args[0].id
                else: 
                    new_body.append(child)
            
            # input_file_var = se÷lf.func_metadata[""]
            # new_body.append(ast.parse(f'''input_file_name={input_file_var}''').body[0])
            if not input_file_var:
                raise Exception("Input file variable not found in HPC run block")
            
            new_body.append(ast.parse(
                f"submit_hpc_job("\
                f"{input_file_var}, "\
                f"template_code, hpc_mode, "\
                f"'{self.func_metadata['name']}', "\
                f"'{self.repo_dir}', "\
                f"'{self.config_path}', "\
                f"'{self.func_metadata.get('template_param_path', '')}', "\
                f"'{self.func_metadata.get('output_file_name', '')}'"\
                ")"
            ).body[0])
            node.body = new_body
                
        
        return super().generic_visit(node)
    
