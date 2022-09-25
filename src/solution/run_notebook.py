import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import pathlib

PHYSICAL_PLAN_SUBSTR = "== Physical Plan =="

def get_raw_plan(notebook_filename: str):

    with open(notebook_filename, "r") as f:
        nb = nbformat.read(f, as_version=4)

    spark_init_pos = None
    sql_context_var = None
    for i in range(len(nb.cells)):
        if nb.cells[i].get('cell_type') != 'code':
            continue

        source = nb.cells[i].get('source', '')
        
        for line in source.split("\n"):
            if "SQLContext(" in line:
                spark_init_pos = i
                sql_context_var = line[:line.find("=")].strip()

    if spark_init_pos is not None and sql_context_var is not None:
        new_cell = nbformat.v4.new_code_cell(f'{sql_context_var}.setConf("spark.sql.debug.maxToStringFields", 10000)')
        nb.cells.insert(spark_init_pos + 1, new_cell)

    ###### FIX .explain to "formatted" ########
    for i in range(len(nb.cells)):
        if nb.cells[i].get('cell_type') != 'code':
            continue

        source = nb.cells[i].get('source', '')
        source += "\n"
        
        if ".explain(" in source:
            pos = source.find(".explain(")
            new_source = source[:pos] + '.explain("formatted")' + source[source.find("\n", pos):]
            new_cell = nbformat.v4.new_code_cell(new_source)
            nb.cells[i] = new_cell

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')

    ep.preprocess(nb, {'metadata': {'path': pathlib.Path(notebook_filename).parent.resolve()}})

    plan = None
    for cell in nb.cells:
        for output in cell.get('outputs', []):
            if output.get('output_type') == 'stream' and output.get('name') == 'stdout' and PHYSICAL_PLAN_SUBSTR in output.get('text', ''):
                plan = output['text'][output['text'].find(PHYSICAL_PLAN_SUBSTR):]


    raw_physical_plan = plan.split("\n")
    
    return raw_physical_plan