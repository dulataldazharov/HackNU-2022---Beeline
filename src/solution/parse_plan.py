
def _process_batch(batch):
    operation_name = batch[0][batch[0].find(")")+2:].strip()
    batch.pop(0)

    result = {
        "_name": operation_name
    }
    for line in batch:
        pos_semicolon = line.find(":")
        pos_brackets = line.find("[")
        if pos_brackets == -1:
            pos_brackets = len(line)

        field_name = line[:min(pos_semicolon, pos_brackets)].strip()
        field_value = line[pos_semicolon+1:].strip()
        result[field_name] = field_value

    return result


def parse_plan(raw_plan_lines: list):
    plan_lines = raw_plan_lines.copy()
    result = []
    while plan_lines:
        while plan_lines and len(plan_lines[-1]) <= 1:
            plan_lines.pop()
        
        if not plan_lines:
            break

        batch = []
        while plan_lines and len(plan_lines[-1]) > 1:
            batch.append(plan_lines[-1])
            plan_lines.pop()
        
        batch.reverse()
        node_data = _process_batch(batch)
        result.append(node_data)
    
    return result