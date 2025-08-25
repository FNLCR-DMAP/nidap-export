import ast


def spark_to_pandas_root_nodes(funcs, root_nodes, logger):
    for node in root_nodes:
        if "function" in funcs[node]:
            transformer = SparkToPandasTransformer()
            funcs[node]["function"] = transformer.visit(funcs[node]["function"])
            ast.fix_missing_locations(funcs[node]["function"])
    
    return funcs

    
class SparkToPandasTransformer(ast.NodeTransformer):
    def __init__(self):
        self.columns = []

    def visit_FunctionDef(self, node):
        new_body = []

        for stmt in node.body:
            # Detect schema assignment and extract column names
            if (
                isinstance(stmt, ast.Assign) and
                isinstance(stmt.value, ast.Call) and
                isinstance(stmt.value.func, ast.Name) and
                stmt.value.func.id == 'StructType'
            ):
                # Extract column names from StructField
                fields = stmt.value.args[0].elts
                self.columns = [
                    field.args[0].value
                    for field in fields
                    if isinstance(field, ast.Call) and field.func.id == 'StructField'
                ]
                continue  # Skip adding this line to the new body

            # Replace Spark createDataFrame return with pd.DataFrame
            elif isinstance(stmt, ast.Return):
                spark_call = stmt.value
                if (
                    isinstance(spark_call, ast.Call) and
                    isinstance(spark_call.func, ast.Attribute) and
                    spark_call.func.attr == 'createDataFrame'
                ):
                    data_arg = spark_call.args[0]
                    new_call = ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='pd', ctx=ast.Load()),
                            attr='DataFrame',
                            ctx=ast.Load()
                        ),
                        args=[],
                        keywords=[
                            ast.keyword(arg='data', value=data_arg),
                            ast.keyword(
                                arg='columns',
                                value=ast.List(
                                    elts=[ast.Constant(value=col) for col in self.columns],
                                    ctx=ast.Load()
                                )
                            )
                        ]
                    )
                    new_body.append(ast.Return(value=new_call))
                    continue  # Done with this stmt

            # Keep anything else (just in case)
            new_body.append(stmt)

        node.body = new_body
        return node