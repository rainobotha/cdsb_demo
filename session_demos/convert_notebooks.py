import json
import os
import copy

base = '/Users/rbotha/Documents/Cursor_code/cdsb_demo/session_demos'

def convert_notebook(input_file, output_file):
    with open(os.path.join(base, input_file)) as f:
        nb = json.load(f)
    
    new_cells = []
    for cell in nb['cells']:
        new_cell = copy.deepcopy(cell)
        
        if cell['cell_type'] == 'code':
            source = ''.join(cell['source'])
            
            if source.strip().startswith('import snowflake.connector'):
                new_cell['source'] = [
                    "import pandas as pd\n",
                    "import plotly.express as px\n",
                    "import plotly.graph_objects as go\n",
                    "from snowflake.snowpark.context import get_active_session\n",
                    "\n",
                    "session = get_active_session()\n",
                    "print('Connected to', session.get_current_database(), session.get_current_schema())\n"
                ]
                new_cells.append(new_cell)
                continue
            
            sql_only = True
            sql_stmts = []
            python_parts = []
            
            lines = source.split('\n')
            in_sql = False
            sql_buf = []
            
            for line in lines:
                stripped = line.strip()
                if stripped.startswith('%%time'):
                    continue
                if 'cur.execute("""' in stripped or "cur.execute('''" in stripped:
                    in_sql = True
                    sql_buf = []
                    continue
                if in_sql and (stripped == '"""' or stripped == '""")' or stripped == "''')" or stripped == "''')"):
                    in_sql = False
                    sql_text = '\n'.join(sql_buf)
                    sql_stmts.append(sql_text.strip())
                    continue
                if in_sql:
                    sql_buf.append(line)
                    continue
                if stripped and not stripped.startswith('#'):
                    sql_only = False
                    python_parts.append(line)
            
            if sql_only and sql_stmts and not python_parts:
                for i, stmt in enumerate(sql_stmts):
                    sql_cell = {
                        "cell_type": "raw",
                        "id": cell['id'] + f'-sql{i}',
                        "metadata": {"language": "sql"},
                        "source": [l + '\n' for l in stmt.split('\n')]
                    }
                    if sql_cell['source']:
                        sql_cell['source'][-1] = sql_cell['source'][-1].rstrip('\n')
                    new_cells.append(sql_cell)
                continue
            
            if 'pd.read_sql(' in source or 'cur.execute(' in source:
                new_source = source
                new_source = new_source.replace("pd.read_sql(\"", "session.sql(\"")
                new_source = new_source.replace("pd.read_sql('", "session.sql('")
                new_source = new_source.replace('pd.read_sql("""', 'session.sql("""')
                new_source = new_source.replace("pd.read_sql(f\"\"\"", "session.sql(f\"\"\"")
                new_source = new_source.replace(", conn)", ").to_pandas()")
                new_source = new_source.replace('%%time\n', '')
                
                import re
                new_source = re.sub(r'cur\.execute\(\s*"""', 'session.sql("""', new_source)
                new_source = re.sub(r'"""\s*\)', '""").collect()', new_source)
                
                new_source = new_source.replace("print('Model trained!', cur.fetchone())", "print('Model trained!')")
                
                new_cell['source'] = [l + '\n' for l in new_source.split('\n')]
                if new_cell['source']:
                    new_cell['source'][-1] = new_cell['source'][-1].rstrip('\n')
            
            if 'setup_sql' in source and 'for stmt in' in source:
                grants = [
                    "USE ROLE ACCOUNTADMIN;\n",
                    "\n",
                    "CREATE DATABASE ROLE IF NOT EXISTS CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT USAGE ON DATABASE CDSB_DEMO TO DATABASE ROLE CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT USAGE ON SCHEMA CDSB_DEMO.RAW TO DATABASE ROLE CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT SELECT ON ALL TABLES IN SCHEMA CDSB_DEMO.RAW TO DATABASE ROLE CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT SELECT ON FUTURE TABLES IN SCHEMA CDSB_DEMO.RAW TO DATABASE ROLE CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT CREATE TABLE ON SCHEMA CDSB_DEMO.RAW TO DATABASE ROLE CDSB_DEMO.NEO4J_ROLE;\n",
                    "GRANT DATABASE ROLE CDSB_DEMO.NEO4J_ROLE TO APPLICATION Neo4j_Graph_Analytics"
                ]
                sql_cell = {
                    "cell_type": "raw",
                    "id": cell['id'] + '-sql',
                    "metadata": {"language": "sql"},
                    "source": grants
                }
                new_cells.append(sql_cell)
                continue
            
            if 'print("""' in source and 'PRODUCTION AUTOMATION' in source:
                automation_sql = [
                    "-- PRODUCTION AUTOMATION (reference only)\n",
                    "-- Retrain model monthly, alert on new anomalies\n",
                    "-- See notebook markdown for full Task + Alert SQL\n",
                    "\n",
                    "SELECT 'See comments above for production Task + Alert automation' as NOTE"
                ]
                sql_cell = {
                    "cell_type": "raw",
                    "id": cell['id'] + '-sql',
                    "metadata": {"language": "sql"},
                    "source": automation_sql
                }
                new_cells.append(sql_cell)
                continue
        
        new_cells.append(new_cell)
    
    nb['cells'] = new_cells
    nb['metadata'] = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.11.0"}
    }
    
    with open(os.path.join(base, output_file), 'w') as f:
        json.dump(nb, f, indent=1)
    
    print(f'{output_file}: {len(new_cells)} cells written')


convert_notebook('01_anomaly_detection.ipynb', 'sf_01_anomaly_detection.ipynb')
convert_notebook('02_process_mining.ipynb', 'sf_02_process_mining.ipynb')
convert_notebook('03_neo4j_graph_analytics.ipynb', 'sf_03_neo4j_graph_analytics.ipynb')

for f in ['sf_01_anomaly_detection.ipynb', 'sf_02_process_mining.ipynb', 'sf_03_neo4j_graph_analytics.ipynb']:
    path = os.path.join(base, f)
    with open(path) as fh:
        nb = json.load(fh)
    cell_types = [c['cell_type'] for c in nb['cells']]
    print(f'  {f}: {len(nb["cells"])} cells - types: {dict((t, cell_types.count(t)) for t in set(cell_types))}')
