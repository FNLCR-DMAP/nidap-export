import ast
class RemoveSpecificAssign(ast.NodeTransformer):
    def visit_Assign(self, node):
        # First, visit children
        self.generic_visit(node)

        # Match target: output
        if (
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].id == "output"
        ):
            # Match value: Transforms.get_output()
            val = node.value
            if (
                isinstance(val, ast.Call) and
                isinstance(val.func, ast.Attribute) and
                isinstance(val.func.value, ast.Name) and
                val.func.value.id == "Transforms" and
                val.func.attr == "get_output"
            ):
                return None  # Remove this node

        return node  # Leave everything else unchanged