import webbrowser
import json
from parse_plan import parse_plan
import click

from parse_idxs import find_identifiers
from dfs import DAG, get_leaf_to_filename
from run_notebook import get_raw_plan

# Spark Explain Output Related variables
FINAL_COLUMNS_HEAD = "AdaptiveSparkPlan"
URL = "https://www.beeline-track.ml/"
raw_plan_content = []
parsed_plan_content = []

# Dependency clauses to parse
AS_CLAUSE = " AS "
FILTERING_CLAUSES = ["Filter", "BroadcastHashJoin"]

# Graph-related variables
adj_map = {}

def initialize(is_explain: bool, file_path: str):
    global parsed_plan_content, raw_plan_content
    if is_explain:
        raw_plan = []
        explain_file = open(file_path, 'r')
        for line in explain_file:
            raw_plan.append(line)
    else:
        raw_plan = get_raw_plan(file_path)
    skip = True
    for line in raw_plan:
        if len(line) <= 1:
            skip = False
        if not skip:
            raw_plan_content.append(line)
    parsed_plan_content = parse_plan(raw_plan_content)


def add_as_clause_dependency(line: str):
    for i in range(0, len(line)-3):
        if line[i:i+4] == AS_CLAUSE:
            parent_name = find_identifiers(line[i:], True)
            if parent_name is None:
                continue
            if not(line[i+4:].strip().startswith(parent_name)):
                continue
            left = i
            bracket_depth = 0
            while not(left < 1 or (bracket_depth == 0 and line[left] in [',', '['])):
                if line[left] == ')':
                    bracket_depth += 1
                elif line[left] == '(':
                    bracket_depth -= 1
                left -= 1
            child_names = find_identifiers(line[left:i])
            if parent_name not in adj_map:
                adj_map[parent_name] = []
            for child in child_names:
                adj_map[parent_name].append(child)

def add_as_clause_deps():
    global raw_plan_content
    for line in raw_plan_content:
        add_as_clause_dependency(line)

def get_filtering_deps():
    filtering_deps = []
    for step in parsed_plan_content:
        if step["_name"] == "Filter":
            filtering_deps += find_identifiers(step["Condition"])
        elif step["_name"] == "BroadcastHashJoin":
            filtering_deps += find_identifiers(step["Left keys"])
            filtering_deps += find_identifiers(step["Right keys"])
            filtering_deps += find_identifiers(step["Join condition"])
    return list(set(filtering_deps))

def get_final_ids():
    final_ids = []
    for step in parsed_plan_content:
        if step["_name"] == "AdaptiveSparkPlan":
            final_ids = find_identifiers(step["Output"])
            break
    return final_ids

@click.command()
@click.argument('file_path')
@click.option('--mode', type=click.Choice(['explain', 'script']), required=True)
def run(file_path, mode):
    initialize(mode == "explain", file_path)
    add_as_clause_deps()
    final_ids = get_final_ids()

    dag = DAG(adj_map)

    final_id_to_deps = dict()
    for id in final_ids:
        final_id_to_deps[id] = dag.get_deps(id)

    leaf_to_filename = get_leaf_to_filename(parsed_plan_content)

    common_leaf_deps = set()

    for id in get_filtering_deps():
        for leaf in dag.get_deps(id):
            common_leaf_deps.add(leaf)

    filtered_deps = set()
    filtered_source_names = set()

    for dep in common_leaf_deps:
        if dep not in leaf_to_filename:
                continue
        source_name = leaf_to_filename[dep]
        col_name = dep.split('#')[0]
        formatted_col_name = f"{source_name}.{col_name}"
        
        filtered_deps.add(formatted_col_name)
        filtered_source_names.add(source_name)

    result = {"non_trivial_cols_deps": list(filtered_deps)}
    result["non_trivial_data_sources"] = list(filtered_source_names)

    for id, deps in final_id_to_deps.items():
        col_names = set()
        source_names = set()
        for dep in set(deps):
            if dep not in leaf_to_filename:
                continue
            
            source_name = leaf_to_filename[dep]
            col_name = dep.split('#')[0]
            formatted_col_name = f"{source_name}.{col_name}"

            if dep in deps:
                col_names.add(formatted_col_name)

            source_names.add(source_name)

        result[id.split('#')[0]] = {
            "data_sources": list(source_names),
            "cols_deps": list(col_names)
        }

    pretty_json = json.dumps(result, indent=4)
    print(pretty_json)

    json_file = open("deps_graph.json", "w")
    json_file.write(pretty_json)
    json_file.close()
    
    webbrowser.open_new_tab(URL)


if __name__ == '__main__':
    run()