import streamlit as st
from include.databricksconnect import databricksconnect
# from databricksconnect import databricksconnect
from include.databricks_config import server_hostname, http_path, access_token 
# from databricks_config import server_hostname, http_path, access_token # use this import while running this file locally

QUALIFIED_TABLE_NAME = "new_delhi_aqi.aqi_record"
# METADATA_QUERY = "show fields from employees;"
TABLE_DESCRIPTION = """
Table Name: aqi_record\n\nColumn Name: Date\nData Type: date\nDescription: The date of the AQI record.\n\nColumn Name: Location\nData Type: string\nDescription: The location where the AQI measurement was taken.\n\nColumn Name: Humidity\nData Type: double\nDescription: The humidity level at the time of the AQI measurement.\n\nColumn Name: Temperature\nData Type: double\nDescription: The temperature at the time of the AQI measurement.\n\nColumn Name: AQI_US\nData Type: double\nDescription: The AQI (Air Quality Index) value for the US standard.\n\nColumn Name: Air_Quality\nData Type: string\nDescription: The air quality category based on the AQI value.\n\nColumn Name: PM2_5_Level\nData Type: double\nDescription: The PM2.5 level (particulate matter with a diameter of 2.5 micrometers or smaller) at the time of the AQI measurement.\n\nColumn Name: PM10_Level\nData Type: double\nDescription: The PM10 level (particulate matter with a diameter of 10 micrometers or smaller) at the time of the AQI measurement.\n\nColumn Name: CO_Level\nData Type: double\nDescription: The CO (carbon monoxide) level at the time of the AQI measurement.\n\nColumn Name: NO2_Level\nData Type: double\nDescription: The NO2 (nitrogen dioxide) level at the time of the AQI measurement.\n\nColumn Name: O3_Level\nData Type: double\nDescription: The O3 (ozone) level at the time of the AQI measurement.\n\nColumn Name: SO2_Level\nData Type: double\nDescription: The SO2 (sulfur dioxide) level at the time of the AQI measurement."""

GEN_SQL = """
You will be acting as an AI Databricks expert named Bricksy.
Your goal is to give correct, executable SQL queries to users.
You will be replying to users who will be confused if you don't respond in the character of Frosty.
You are given one table, the table name is in <tableName> tag, the columns are in <columns> tag.
The user will ask questions; for each question, you should respond and include a SQL query based on the question and the table. 

{context}

Here are 6 critical rules for the interaction you must abide:
<rules>
1. You MUST wrap the generated SQL queries within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
4. Make sure to generate a single Databricks code snippet, not multiple. 
5. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names.
6. DO NOT put numerical at the very front of SQL variable.
</rules>

Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for variable_name column)
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```

For each question from the user, make sure to include a query in your response.

Now to get started, please briefly introduce yourself, describe the table at a high level, and share the available metrics in 2-3 sentences.
Then provide 3 example questions using bullet points.
"""

@st.cache_data(show_spinner=False)
def get_table_context(table_name: str, table_description: str):
    build_conn = databricksconnect()
    cur = build_conn.buildconn()
    table = table_name.split(".")
    cur.execute(f"""
    DESCRIBE TABLE {table_name};
    """)
    columns = cur.fetchall()
    columns = "\n".join(
    [
    f"- **{columns[i][0]}**: {columns[i][1]}" for i in range(len(columns))
    ]
    )
    context = f"""
Here is the table name <tableName> {'.'.join(table)} </tableName>

<tableDescription>{table_description}</tableDescription>

Here are the columns of the {'.'.join(table)}

<columns>\n\n{columns}\n\n</columns>
    """
    build_conn.closeconn()
    return context

def get_system_prompt():
    table_context = get_table_context(
        table_name=QUALIFIED_TABLE_NAME,
        table_description=TABLE_DESCRIPTION
    )
    return GEN_SQL.format(context=table_context)

# do `streamlit run prompts.py` to view the initial system prompt in a Streamlit app
if __name__ == "__main__":
    st.header("System prompt for Bricksy")
    st.markdown(get_system_prompt())
    