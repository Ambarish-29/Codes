import streamlit as st

st.header("Dynamic Sql Query Gen Test")

import psycopg2

db_name = "postgres"
db_user = "postgres"
db_pass = "postgres"
db_port = 5432
db_host = "localhost"

conn = psycopg2.connect(dbname=db_name,user=db_user,password=db_pass,host=db_host,port=db_port)

def query_execution(query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        tbls = cursor.fetchall()
    table_names = [tbl[0] for tbl in tbls]

    return table_names

query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
table_names = query_execution(query)

table_names.append(None)
default_index = table_names.index(None)

join_lst = ["inner"]


usr_selected_table_name = st.selectbox("Select Table Name", options=table_names, index=default_index)
usr_selected_join_table_name = st.selectbox("Select Join Table Name", options=table_names, index=default_index)
usr_selected_join_type = st.selectbox("Select Join Type", options=join_lst)
usr_selected_limit = st.text_input("Enter the limit:")

if usr_selected_table_name != None:
    upd_query = f"""SELECT column_name FROM information_schema.columns WHERE table_name = '{usr_selected_table_name}'"""
    column_names = query_execution(upd_query)
    upd_column_names = [f"{usr_selected_table_name}.{column_name}" for column_name in column_names]
    #print(upd_column_names)

    if usr_selected_join_table_name is None:

        with st.form(key='my_form'):
            selected_columns = st.multiselect("Select Column Names",options=upd_column_names) 
            submit_button = st.form_submit_button(label='Submit')

        if submit_button:
            i=0
            usr_selected_limit = 10 if usr_selected_limit == '' else usr_selected_limit
            for select_column_name in selected_columns:
                if i==0:
                    tmp = select_column_name 
                else:
                    tmp+=f',{select_column_name}'
                i+=1
            select_query = f"select {tmp} from {usr_selected_table_name} limit {usr_selected_limit}"
            print(select_query)

    if usr_selected_join_table_name is not None:
        upd_query = f"""SELECT column_name FROM information_schema.columns WHERE table_name = '{usr_selected_join_table_name}'"""
        column_names = query_execution(upd_query)
        upd_join_column_names = [f"{usr_selected_join_table_name}.{column_name}" for column_name in column_names]
        left_join_condition = st.selectbox("Join Condition on Left table", options=upd_column_names)
        right_join_condition = st.selectbox("Join Condition on Left table", options=upd_join_column_names)

        join_combined_select_lst = upd_column_names + upd_join_column_names

        with st.form(key='my_form1'):
            selected_columns = st.multiselect("Select Both Table Column Names",options=join_combined_select_lst) 
            submit_button = st.form_submit_button(label='Submit')

        if submit_button:
            i=0
            usr_selected_limit = 10 if usr_selected_limit == '' else usr_selected_limit
            for select_column_name in selected_columns:
                if i==0:
                    tmp = select_column_name 
                else:
                    tmp+=f',{select_column_name}'
                i+=1
            select_query = f"""select {tmp} from {usr_selected_table_name} \
{usr_selected_join_type} join {usr_selected_join_table_name} \
on {left_join_condition} = {right_join_condition} \
limit {usr_selected_limit}"""
            print(select_query)


        



