import pandas as pd
from contextlib import redirect_stdout

# Spark Explain Output Related variables
EXPLAIN_FILE_NAME = 'out.txt'
EXPLAIN_PLAN_NAMES = [
    "== Parsed Logical Plan ==",
    "== Analyzed Logical Plan ==",
    "== Optimized Logical Plan ==",
    "== Physical Plan =="
    ]
explain_plan_content = {}

# Dependency clauses to parse
AS_CLAUSE = " AS "
FILTERING_CLAUSES = ["+- Filter", "+- Join"]

# Graph-related variables
adj_map = {}
filtering_dependencies = []

def find_identifiers(s, first_only=False):
    identifiers = set()

    def is_left_allowed(c):
        if c.isalpha():
            return True
        
        if c.isdigit():
            return True
        
        if c in ["_"]:
            return True
        
        return False


    def is_right_allowed(c):
        if c in ["L"]:
            return True
        
        if c.isdigit():
            return True

        return False

    for i in range(len(s)):
        if s[i] != '#':
            continue

        l = i - 1
        r = i + 1

        while l >= 0 and is_left_allowed(s[l]):
            l = l - 1

        while r < len(s) and is_right_allowed(s[r]):
            r = r + 1
        
        if l != i - 1 and r != i + 1:
            if first_only:
                return s[l+1:r]
            identifiers.add(s[l+1:r])

    if first_only:
        return None

    return list(identifiers)

def initialize_explain_plans():
    # with open(EXPLAIN_FILE_NAME, 'w') as file:
    #     with redirect_stdout(file):
    #         data_frame.explain(True)
    #     file.close()
    with open(EXPLAIN_FILE_NAME, 'r') as file:
        plan_name = EXPLAIN_PLAN_NAMES[0]
        next = 1
        explain_plan_content[plan_name] = []
        for line in file:
            if EXPLAIN_PLAN_NAMES[next] in line:
                plan_name = EXPLAIN_PLAN_NAMES[next]
                if next + 1 < len(EXPLAIN_PLAN_NAMES):
                    next += 1
                explain_plan_content[plan_name] = [line]
            else:
                explain_plan_content[plan_name].append(line)
        file.close()

def add_as_clause_dependency(line: str):
    for i in range(0, len(line)-3):
        if line[i:i+4] == AS_CLAUSE:
            parent_name = find_identifiers(line[i:], True)
            if parent_name is None:
                continue
            left = i
            bracket_depth = 0
            if (parent_name == "LAST_SERIAL_NUM_MONTHLY#1384"):
                print(parent_name)
            while not(left < 1 or (bracket_depth == 0 and line[left] in [',', '['])):
                if line[left] == ')':
                    bracket_depth += 1
                elif line[left] == '(':
                    bracket_depth -= 1
                left -= 1
            if (parent_name == "LAST_SERIAL_NUM_MONTHLY#1384"):
                print(left)
                print(i)
            child_names = find_identifiers(line[left:i])
            if parent_name not in adj_map:
                adj_map[parent_name] = []
            for child in child_names:
                adj_map[parent_name].append(child)

def add_dependencies():
    for line in explain_plan_content["== Optimized Logical Plan =="]:
        add_as_clause_dependency(line)
        add_filtering_dependencies(line)

def add_filtering_dependencies(line: str):
    for filter_clause in FILTERING_CLAUSES:
        if filter_clause in line:
            identifiers = find_identifiers(line)
            for id in identifiers:
                filtering_dependencies.append(id)

initialize_explain_plans()
add_dependencies()






#### TESTS ####

def print_adj_map():
    for key in adj_map:
        print("Node: " + key)
        for child in adj_map[key]:
            print("   " + child)
        print("")

print_adj_map()
