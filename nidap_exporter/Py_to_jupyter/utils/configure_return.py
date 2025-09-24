# import ast


def configure_return(func_dict, logger):
    for func_name, func_data in func_dict.items():
        if "function" in func_data:
            func = func_data["function"]
            # transformer = FunctionReturnTransformer(func_metadata=func_data)
            # func = transformer.visit(func)
            # func = ast.fix_missing_locations(func)
            func_data["function"] = modify_top_level_returns(func)
    return func_dict

# class FunctionReturnTransformer(ast.NodeTransformer):
#     def __init__(self, func_metadata):
#         self.func_metadata = func_metadata


#     def visit_FunctionDef(self, node):
#         # Only modify top-level functions
#         self.inside_top_level_function = True
#         new_body = []
#         for stmt in node.body:
#             # Intercept return statements
#             if isinstance(stmt, ast.Return) and self.inside_top_level_function:
#                 ret_val = stmt.value

#                 # Build: with open(...) as f: pickle.dump(ret_val, f)
#                 dump_stmt = ast.With(
#                     items=[
#                         ast.withitem(
#                             context_expr=ast.Call(
#                                 func=ast.Name(id='open', ctx=ast.Load()),
#                                 args=[
#                                     ast.Constant(value=self.func_metadata['output_file_name']),
#                                     ast.Constant(value='wb')
#                                 ],
#                                 keywords=[]
#                             ),
#                             optional_vars=ast.Name(id='f', ctx=ast.Store())
#                         )
#                     ],
#                     body=[
#                         ast.Expr(value=ast.Call(
#                             func=ast.Attribute(
#                                 value=ast.Name(id='pickle', ctx=ast.Load()),
#                                 attr='dump',
#                                 ctx=ast.Load()
#                             ),
#                             args=[ret_val, ast.Name(id='f', ctx=ast.Load())],
#                             keywords=[]
#                         ))
#                     ]
#                 )
#                 # Append pickle.dump before the return
#                 new_body.append(dump_stmt)
#                 new_body.append(stmt)
#             else:
#                 new_body.append(stmt)

#         node.body = new_body
#         self.inside_top_level_function = False
#         return node

import ast

class ReturnRewriter(ast.NodeTransformer):
    def visit_Return(self, node):
        # Transform `return x` → with open... + pickle.dump(x, f); return x
        ret_val = node.value

        dump_stmt = ast.With(
            items=[
                ast.withitem(
                    context_expr=ast.Call(
                        func=ast.Name(id='open', ctx=ast.Load()),
                        args=[
                            ast.Constant(value='output.pkl'),
                            ast.Constant(value='wb')
                        ],
                        keywords=[]
                    ),
                    optional_vars=ast.Name(id='f', ctx=ast.Store())
                )
            ],
            body=[
                ast.Expr(
                    value=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='pickle', ctx=ast.Load()),
                            attr='dump',
                            ctx=ast.Load()
                        ),
                        args=[ret_val, ast.Name(id='f', ctx=ast.Load())],
                        keywords=[]
                    )
                )
            ]
        )

        return [dump_stmt, node]  # return a list of statements

def modify_top_level_returns(func_node: ast.FunctionDef):
    """Given a FunctionDef node, rewrite its return statements but skip inner defs."""
    class ScopedTransformer(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            # Don't recurse into nested functions
            return node

        def visit_AsyncFunctionDef(self, node):
            return node

        def visit_Lambda(self, node):
            return node

        def generic_visit(self, node):
            if isinstance(node, ast.Return):
                return ReturnRewriter().visit_Return(node)
            return super().generic_visit(node)

    transformer = ScopedTransformer()
    return transformer.visit(func_node)