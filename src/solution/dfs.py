from parse_idxs import find_identifiers


class DAG:
    def __init__(self, node_to_neighbors):
        self._g = node_to_neighbors
        self._cache = dict()

    def get_deps(self, node):
        if node in self._cache:
            return self._cache[node]
        
        deps = set()
        for to in self._g.get(node, []):
            for to_dep in self.get_deps(to):
                deps.add(to_dep)

        if not self._g.get(node, []):
            deps.add(node)

        self._cache[node] = list(deps)
        return self._cache[node]


def get_leaf_to_filename(parsed_plan_content):
    node_to_filename = dict()
    
    for step in parsed_plan_content:
        if "Scan" in step["_name"]:
            filename = ""
            if "Location" not in step:
                continue
            line = step["Location"]
            last_slash = 0

            for i in range(0, len(line)):
                if line[i] == "/":
                    last_slash = i

            for i in range(last_slash, len(line)):
                if line[i] == "]":
                    filename = line[last_slash+1 : i]
                    break

            for id in find_identifiers(step["Output"]):
                node_to_filename[id] = filename

    return node_to_filename
