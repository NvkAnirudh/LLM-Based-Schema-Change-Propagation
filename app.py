import streamlit as st
import pandas as pd 
from snowflake.snowpark.context import get_active_session
import re
import time
from collections import defaultdict

# set the page layout to wide for better visualization
st.set_page_config(layout='wide')

# Database and Table Configurations
database = 'CORTEX_LLM_ETL'
schema = 'SCHEMA_EVOLUTION'
schema_baseline_table = f'{database}.{schema}.schema_baseline'
schema_change_log = f'{database}.{schema}.schema_change_log'

# Column name constants for lineage information and LLM processing
TABLE_NAME = 'TABLE_NAME'
SOURCE_OBJECT_NAME = 'SOURCE_OBJECT_NAME'
DISTANCE = 'DISTANCE'
EXISTING_DDL = 'EXISTING_DDL'
CORTEX_RESPONSE = 'CORTEX_RESPONSE'
MODEL_NAME = 'llama3.1-70b'
TARGET_OBJECT_NAME = 'TARGET_OBJECT_NAME'
TARGET_OBJECT_DOMAIN = 'TARGET_OBJECT_DOMAIN'
CHANGE_DETECTED_AT = 'CHANGE_DETECTED_AT'

# Upstream (bronze) table
bronze_table = 'bronze_fraud_transactions'

# Initialize Snowflake Session and Page Title

session = get_active_session()

# set the page title and description
st.title('LLM-Based Schema Change Propagation')
st.write('Effortlessly propagate schema changes across your data lineage with AI')

# Initialize Streamlit Session State Variables
if "current_index" not in st.session_state:
    st.session_state.current_index = 0
if "success_message_shown" not in st.session_state:
    st.session_state.success_message_shown = False
if "auto_propagate_done" not in st.session_state:
    st.session_state.auto_propagate_done = False
if "manual_propagate_active" not in st.session_state:
    st.session_state.manual_propagate_active = False
if "dfs_order" not in st.session_state:
    st.session_state.dfs_order = None
if "current_dfs_index" not in st.session_state:
    st.session_state.current_dfs_index = 0

# Helper functions
def fetch_schema_changes():
    """
    Fetches the latest schema change log from the designated Snowflake table.
    Returns the data as a pandas DataFrame.
    """
    query = f'select * from {schema_change_log} order by {CHANGE_DETECTED_AT} desc'
    # Run the query, collect the results, and convert to a pandas DataFrame
    return state.create_dataframe(session.sql(query).collect()).to_pandas()

def return_lineage_query_for_changed_tables(changed_table):
    """
    Generates the SQL query that calls Snowflake's lineage function for the given table.
    It retrieves the downstream lineage up to a depth of 10.
    """
    lineage_query = f"""
    select 
        {DISTANCE},
        {SOURCE_OBJECT_NAME},
        {TARGET_OBJECT_NAME},
        {TARGET_OBJECT_DOMAIN}
    from table (snowflake.core.get_lineage('{database}.{schema}.{changed_table}','table', 'downstream', 10))
    where source_status = 'ACTIVE' and target_status = 'ACTIVE';
    """
    return lineage_query

def visualize_lineage_path(lineage_df):
    """
    Visualizes the lineage path based on a pandas DataFrame.
    It groups rows by the source object and creates a string that shows
    the path from source to downstream targets.
    """
    # Sort the dataframe by 'DISTANCE' to ensure correct order
    lineage_df = lineage_df.sort_values(by=DISTANCE)

    # Create a dictionary to hold paths for each source object
    paths = {}
    for _, row in lineage_df.iterrows():
        source_name = row[SOURCE_OBJECT_NAME]
        target_name = row[TARGET_OBJECT_NAME]

        # Append target to the existing path or create a new branch if needed
        if source_name in paths:
            paths[source_name].append(target_name)
        else:
            paths[source_name] = [target_name]

    # Build a human-readable string of lineage paths
    lineage_paths = []
    for source, targets in paths.items():
        path_string = f"{source} --> {' --> '.join(targets)}"
        lineage_paths.append(path_string)

    # Separate each branch with a double newline for clarity
    return "\n\n".join(lineage_paths)

def build_adjacency_list(lineage_df):
    """
    Builds an adjacency list from the lineage DataFrame.
    This list maps each source table to a list of downstream target tables.
    """
    adj_list = defaultdict(list)
    for _,row in lineage_df.iterrows():
        source = row[SOURCE_OBJECT_NAME]
        target = row[TARGET_OBJECT_DOMAIN]
        adj_list[source].append({
            'table_name': target,
            'domain': row[TARGET_OBJECT_DOMAIN],
            'distance': row[DISTANCE]
        })
    return adj_list

def get_dfs_order(adj_list, start_table):
    """
    Performs a Depth-First Search (DFS) on the adjacency list starting from the given table.
    Returns a list of dictionaries describing the traversal order, including source and target table details.
    """
    visited = set()
    traversal_order = []

    def dfs(table):
        if table not in visited:
            visited.add(table)
            for target in adj_list[table]:
                traversal_order.append({
                    'source': table,
                    'target': target['table_name'],
                    'domain': target['domain']
                })
                dfs(target['table_name'])
    dfs(start_table)
    return traversal_order

def create_llm_prompt(existing_table_ddl, schema_change_log_df, upstream_table_name, target_table_name, mode='apply'):
    """
    Creates an LLM prompt for either applying schema changes or generating a preview.
    It includes the existing DDL for the target table and details from the schema change log.
    """
    # Convert the first row of the schema change log DataFrame to string for context
    schema_change_log_info = str(schema_change_log_df[0])

    if mode == 'apply':
        prompt = f"""
        This is the existing DDL for the target table `{target_table_name}`:

        {existing_table_ddl}

        Based on the schema changes detected in the upstream table `{upstream_table_name}`, shown below:

        {schema_change_log_info}

        Make the necessary modifications to the DDL for `{target_table_name}` to incorporate these changes from `{upstream_table_name}`. 
        Ensure that:
        1. The structure and formatting of the original DDL is preserved, including any WITH clauses, transformations, or filters.
        2. The newly added or modified columns are integrated into the DDL appropriately, reflecting only the specified changes.
        3. Only the final SQL query is returned as plain text—do not include explanations, comments, or extraneous characters.

        Return only the SQL query with the updated structure in plain text.
        """
    else: # 'preview' mode
        prompt = f"""
        This is the existing DDL for the target table `{target_table_name}`:

        {existing_table_ddl}

        Based on the schema changes detected in the upstream table `{upstream_table_name}`, shown below:

        {schema_change_log_info}

        Generate a SELECT query to preview the modified structure for `{target_table_name}`.
        Ensure that:
        1. The query mirrors the original DDL structure, incorporating any transformations or WITH clauses.
        2. The new columns from `{upstream_table_name}` are included only as specified in the schema change log.
        3. Only the final SQL query is returned as plain text—do not include explanations, comments, or extraneous characters.

        Return only the SQL query for the preview in plain text.
        """
    return prompt

def clean_ddl_response(ddl_response):
    """
    Cleans the LLM response by removing markdown code block formatting.
    """
    return re.sub(r'```', '', ddl_response).strip()

def automate_propagate_changes(lineage_df, upstream_table_name):
    """
    Automatically applies schema change propagation to all downstream tables.
    It first builds the lineage graph (using DFS order), then for each downstream table:
      - Fetches the current DDL.
      - Generates an LLM prompt to modify the DDL.
      - Executes the updated DDL.
    """
    # Convert the lineage DataFrame to pandas if not already
    lineage_pandas_df = session.create_dataframe(lineage_df).to_pandas() if not isinstance(lineage_df, pd.DataFrame) else lineage_df

    # Build the adjacency list and obtain the DFS order starting from the upstream table
    adj_list = build_adjacency_list(lineage_pandas_df)
    dfs_order = get_dfs_order(adj_list, upstream_table_name)

    # Process each downstream table in DFS order
    for item in dfs_order:
        target_table_name = item['target']
        target_table_domain = item['domain']
        source_table = item['source']

        # Fetch the existing DDL for the target table using Snowflake's GET_DDL function
        existing_table_ddl_query = f"select GET_DDL('{target_table_domain}','{target_table_name}') as {EXISTING_DDL}"
        existing_table_ddl_df = session.create_dataframe(session.sql(existing_table_ddl_query).collect()).to_pandas()
        existing_table_ddl = existing_table_ddl_df[EXISTING_DDL].iloc[0]

        # Generate the LLM prompt in 'apply' mode to produce the new DDL
        apply_prompt = create_llm_prompt(existing_table_ddl, schema_change_log_df, source_table, target_table_name, mode='apply')

        # Call the LLM via Snowflake function to get the suggested DDL update
        apply_response = session.sql(f"select snowflake.cortex.complete('{MODEL_NAME}', $${apply_prompt}$$)").collect()
        new_ddl_suggestion = clean_ddl_response(apply_response[0][0])

        # Try to execute the new DDL; if it fails, display an error and stop propagation
        try:
            session.sql(new_ddl_suggestion).collect()
            st.success(f"Applied DDL for {target_table_name")
            time.sleep(2) # SHort pause for message display
        except Exception as e:
            st.error(f"Error applying DDL to {target_table_name}: {e}")
            break # Stop if an error occurs

def generate_preview_table_ddl(ddl_sql, preview_table_name='PREVIEW_LLM_TABLE'):
    """
    Generates a new DDL for a preview table by replacing the original table name
    with a preview table name. This allows you to run a SELECT query against the preview.
    """
    modified_ddl = re.sub(r'(create or replace dynamic table\s+)(\w+)', f'\\1{preview_table_name}', ddl_sql, flags=re.IGNORECASE)
    return modified_ddl


# UI Section: Display Schema Change Log and Lineage Selection

# Fetch and display the schema change log from Snowflake
data = fetch_schema_changes()
st.write('Schema Change Log:')
data_container = st.empty()
data_container.dataframe(data)

# Retrieve the schema change log data as a list for further processing
schema_change_log_df = session.sql(f'select * from {schema_change_log}').collect()

# Populate a selection box with unique changed table names from the schema change log
schema_change_log = session.create_dataframe(session.sql(f'select * from {schema_change_log}').collect()).to_pandas()
changed_table_list = list(schema_change_log[TABLE_NAME].unique())
changed_table_list.insert(0, 'Select Table')
selected_source_object = st.selectbox('Which changed table do you want to review?', changed_table_list)

@st.cahce_data
def cache_lineage_df(selected_source_object):
    """
    Caches the downstream lineage information for a selected changed table.
    """
    lineage_query = return_lineage_query_for_changed_table(selected_source_object)
    lineage_df = session.sql(lineage_query).collect()
    return lineage_df

@st.cache_data
def cache_lineage_path(lineage_df):
    """
    Caches the visualization of the lineage path for the selected table.
    """
    lineage_pandas_df = session.create_dataframe(lineage_df).to_pandas()
    lineage_path = visualize_lineage_path(lineage_path_df)
    return lineage_path

# UI Section: Buttons for Lineage Visualization and Propagation

if selected_source_object != 'Select Table':
    st.write('You selected:', selected_source_object, '\n')

    # Display a message while processing the lineage
    with st.write("_Processing Table's Lineage via Snoaflake's Lineage Function..._")
        lineage_df = cache_lineage_df(selected_source_object)
    
    # Create two columns: one for showing lineage and one for auto-propagation
    lineage, auto_propagation = st.columns(2)
    
    # Button to display the complete lineage path (downstream tables)
    if lineage.button('Show Lineage Path'):
        lineage_path = cache_lineage_path(lineage_df)
        st.write('Impacted Downstream Tables:')
        st.write(lineage_path)
    
    # Button for automatic propagation of schema changes to all downstream tables
    if auto_propagation.button('Auto-Propagate Changes'):
        lineage_pandas_df = session.create_dataframe(lienage_df).to_pandas()
        auto_propagate_changes(lineage_pandas_df, selected_source_object)
        st.session_state.auto_propagate_done = True
        st.write('All tables in the lineage have been processed automatically')
        
    # Button for manual propagation, allowing review and confirmation of each change
    if st.button('Manually Propagate Changes') or st.session_state.manual_propagate_active:
        st.session_state.manual_propagate_active = True
    
        # Initialize DFS order for manual propagation if not already done
        if st.session_state.dfs_order is None:
            lineage_pandas_df = session.create_dataframe(lineage_df).to_pandas()
            adj_list = build_adjacency_list(lineage_pandas_df)
            st.session_state.dfs_order = get_dfs_order(adj_list, selected_source_object)
            st.session_state.current_dfs_index = 0
    
        # Process the next table in the DFS order if available
        if st.session_state.current_dfs_index < len(st.session_state.dfs_order):
            current_item = st.session_state.dfs_order[st.session_state.current_dfs_index]
            target_table_name = current_item['target']
            target_table_domain = current_item['domain']
            source_table = current_item['source']
    
            st.write(f'Processing: {target_table_name}')
    
            # Fetch the existing DDL for the current downstream table
            existing_table_ddl_query = f"select GET_DDL('{target_table_domain}','{target_table_name}') as {EXISTING_DDL}"
            existing_table_ddl_df = session.sql(existing_table_ddl_query).collect()
            existing_table_ddl = existing_table_ddl_df[0][EXISTING_DDL]
    
            # Generate preview and apply prompts using the LLM
            preview_prompt = create_llm(existing_table_ddl, schema_cahnge_log_df, source_table, target_table_name, mode='preview')
            apply_prompt = create_llm(existing_table_ddl, schema_cahnge_log_df, source_table, target_table_name, mode='apply')
    
            # Call the LLM to get suggested DDL updates for the downstream table
            apply_response = session.sql(f"select snowflake.cortex.complete('{MODEL_NAME}', $${apply_prompt}$$").collect()
            new_ddl_suggestion = clean_ddl_response(apply_response[0][0])
    
            # Display the existing and LLM-suggested DDL side by side for manual review
            col1, col2 = st.columns(2)
            with col1:
                st.text_area('Existing DDL', value=existing_table_ddl, height=300, key=f'existing_ddl_{target_table_name}')
            with col2:
                edited_sql = st.text_area('LLM Suggested New DDL', value=new_ddl_suggestion, height=300, key=f'suggested_ddl_{target_table_name}')
    
            # Buttons for previewing and applying the changes
            preview_button, apply_button = st.columns(2)
    
            # Preview button: generate a preview table DDL and display preview results
            if preview_button.button('Preview Changes', key=f'preview_{st.session_state.current_dfs_index}'):
                preview_table_sql = generate_preview_table_ddl(edited_sql)
                try:
                    session.sql(preview_table_sql).colelct()
                    st.write('Preview Results:')
                    st.write(session.sql('select * from PREVIEW_LLM_TABLE').collect())
                except Exception as e:
                    st.error(f'Error executing preview: {e}')
                finally:
                    session.sql('drop table if exists PREVIEW_LLM_TABLE').collect()
    
            # Apply button: apply the changes and move on to the next table
            if apply_button.button('Apply Changes', key=f'apply_{st.session_state.current_dfs_index}'):
                try:
                    session.sql(edited_sql).collect()
                    st.success(f'Applied DDL for {target_table_name}')
                    st.session_state.success_message_shown = True
                except Exception as e:
                    st.error(f'Error applying DDL: {e}')
    
            # If DDL was successfully applied, automatically move to the next downstream table
            if st.session_state.success_message_shown:
                time.sleep(2) # Pause to show the success message
                st.session_state.success_message_shown = False
                st.session_state.current_dfs_index += 1
    
                # If more tables remain, rerun; otherwise, conclude propagation
                if st.session_state.current_dfs_index < len(st.session_state.dfs_order):
                    st.rerun()
                else:
                    st.session_state.manual_propagate_active = False
                    st.session_state.dfs_order = None # Reset DFS order for future runs
                    st.write('All tables in the lineage have been processed')
    else:
        st.session_state.manual_propagate = False
        st.session_state.dfs_order = None # Reset DFS order if complete
        st.write('All tables in the lineage have been processed')
    
        
        
    

    







