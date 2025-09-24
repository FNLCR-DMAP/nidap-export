import ast
import shutil 
def configure_hpc_call(funcs, config_path, repo_dir, logger):
    shutil.copy("./submit_hpc_job.py", repo_dir)
    shutil.copy("./transformer_config.cfg", repo_dir)

    for func in funcs:
        if "function" in funcs[func]:
            transformer = Configure_HPC_Call(funcs[func], config_path, repo_dir, logger)
            transformer.get_hpc_vars(funcs[func]["function"])

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
        self.vars = {}
        for arg_name in func_metadata["args_metadata"]:
            if func_metadata["args_metadata"][arg_name]["arg_type"] == "hpc_launch":
                self.upstream_node = arg_name


    def get_hpc_vars(self, code):
        for node in ast.walk(code):
            if ( isinstance(node, ast.Assign) and 
                len(node.targets) == 1 and 
                isinstance(node.targets[0], ast.Name) 
            ):
                if node.targets[0].id in ["hpc_mode", "template_code"]:
                    self.vars[node.targets[0].id] = node.value
        
        # return self.vars
        

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
            or (
                self.is_hpc_link_generation(node)
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
    def is_hpc_link_generation(self, node):
        return (
            isinstance(node, ast.Expr) and
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Name) and  
            node.value.func.id == "HPC_link_generation"
        )
    
    def get_input_rid_arg(self, node):
        if self.is_hpc_link_generation(node):
            for kw in node.value.keywords:
                # print(f"kw: {ast.dump(kw, indent=4)}")
                if kw.arg == "inputRID":
                    if isinstance(kw.value, ast.Attribute):

                        return kw.value.value.id
                    
        return None
    
    def get_link_gen_template_code(self, node):
        if self.is_hpc_link_generation(node):
            for kw in node.value.keywords:
                if kw.arg == "template_code":
                    if isinstance(kw.value, ast.Constant):
                        return kw.value.value
        return None
    
    def is_hpc_mode(self, node):
        
        return None

    def generic_visit(self, node):
        
        if self.is_if_run_on_hpc_block(node):
            
            new_body = []
            
            is_hpc_link_generation = [ self.is_hpc_link_generation(node) for node in node.body ]
            if any(is_hpc_link_generation):
                print("HPC link generation found")
                hpc_link_gen_call = node.body[is_hpc_link_generation.index(True)]
                
                input_file_var = self.get_input_rid_arg(hpc_link_gen_call)
                template_code = self.get_link_gen_template_code(hpc_link_gen_call)
                

            else:
                new_body.append(ast.parse("from submit_hpc_job import submit_hpc_job").body[0])
                input_file_var = ""
                template_code = self.vars.get("template_code", None)

            for child in node.body:
                if self.should_remove(child):
                    continue
                elif self.is_pickle_load_upstream_node(child):
                    input_file_var = child.body[0].items[0].context_expr.args[0].id
                else: 
                    new_body.append(child)
            
                # input_file_var = self.func_metadata[""]
                # new_body.append(ast.parse(f'''input_file_name={input_file_var}''').body[0])
            if not input_file_var:
                raise Exception("Input file variable not found in HPC run block")
            
            
            #TODO look for hpc_mode assign
            if not template_code:
                raise Exception("template_code variable not found in HPC run block")
            
            if not self.vars.get("hpc_mode", None):
                self.logger.warning("WARNING hpc_mode variable not found in HPC run block, defaulting to CPU")
                hpc_mode = "CPU"
            else:
                hpc_mode = self.vars["hpc_mode"]
            
            new_body.append(ast.parse(
                f"submit_hpc_job("\
                f"{input_file_var}, "\
                f"'{template_code}', '{hpc_mode}', "\
                f"'{self.func_metadata['name']}', "\
                f"'{self.repo_dir}', "\
                f"'{self.config_path}', "\
                f"'{self.func_metadata.get('template_param_path', '')}', "\
                f"'{self.func_metadata.get('output_file_name', '')}'"\
                ")"
            ).body[0])
            node.body = new_body
                
        
        return super().generic_visit(node)
    
