import streamlit as st
import openai
import re
# from include.databricksconnect import databricksconnect
from connection import DatabricksConnection
from include.system_prompt_databricks import get_system_prompt

page = st.sidebar.selectbox("Select Page",("Daily Report", "Weather Agent"))

if page == "Daily Report":
    # building connection with databricks
    conn = st.experimental_connection("databricks", type=DatabricksConnection)
    cur = conn.cursor() # building connection
    st.title("Daily Air Quality Index Report 🌦️")
    cur.execute("SELECT location from new_delhi_aqi.current_aqi_data;")
    get_loc = cur.fetchall()
    locations = [get_loc[i][0] for i in range(len(get_loc))]
    location_select = st.selectbox("Select Location", tuple(locations))
    if location_select:
        cur.execute(f"SELECT AQI_US, PM2_5, PM10, Humidity, Temperature, AQI_Level from new_delhi_aqi.current_aqi_data where location = '{location_select}';")
        output = cur.fetchall()
    st.write("Humidity")
    st.text(f"{output[0][3]}")
    st.write("Temperature")
    st.text(f"{output[0][4]}")
    st.write("AQI-US")
    if output[0][0] < 50:
        write_color = 'green'
    if output[0][0] < 100:
        write_color = 'yellow'
    if output[0][0] < 150:
        write_color = 'orange'
    else:
        write_color = 'red'
    st.markdown(f":{write_color}[{output[0][0]}]")
    st.write("PM 2.5")
    st.text(f"{output[0][1]}")
    st.write("PM 10")
    st.text(f"{output[0][2]}")

    # closing connection with databricks
    cur.close()
    conn.closeconn()

if page == "Weather Agent":
    # Initialize the chat messages history
    openai.api_key = st.secrets.OPENAI_API_KEY
    st.title("Weather Agent 🤖")
    if "messages" not in st.session_state:
        # system prompt includes table information, rules, and prompts the LLM to produce
        # a welcome message to the user.
        st.session_state.messages = [
            {
                "role": "system",
                "content": get_system_prompt()
            }
        ]

    # Prompt for user input and save
    if prompt := st.chat_input():
        st.session_state.messages.append(
            {
                "role": "user",
                "content": prompt
            }
        )

    # display the existing chat messages
    for message in st.session_state.messages:
        if message["role"] == "system":
            continue
        with st.chat_message(message["role"]):
            st.write(message["content"])
            if "results" in message:
                st.dataframe(message["results"])

    # If last message is not from assistant, we need to generate a new response
    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant"):
            response = ""
            resp_container = st.empty()
            for delta in openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
                stream=True,
            ):
                response += delta.choices[0].delta.get("content", "")
                resp_container.markdown(response)

    message = {
        "role": "assistant",
        "content": response
    }
    # Parse the response for a SQL query and execute if available
    sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
    if sql_match:
        sql = sql_match.group(1)
        # connecting to Databricks and Executing query
        build_conn = st.experimental_connection("databricks", type=DatabricksConnection)
        cur = build_conn.cursor() # building connection
        cur.execute(sql)
        output = cur.fetchall()
        data = [output[x].asDict() for x in range(len(output))]
        st.dataframe(data)
    st.session_state.messages.append(message)
