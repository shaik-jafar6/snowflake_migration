import os
import re
import snowflake.connector


conn = snowflake.connector.connect(
    user='*****',
    password='*****',
    account='*****',
    warehouse='*****',
    DATABASE='*****'
    )

def get_ddl_content(filepath):
    with open(filepath, "r") as f:
        sql_contents = f.read()
    res = sql_contents.find("CREATE TABLE")
    sql_contents = sql_contents[res:]
    return sql_contents
def extract_create_table(ddl):
    counter = 0
    for i in range(len(ddl)):
        if ddl[i] == '(':
            counter = counter + 1
        elif ddl[i] == ')':
            counter = counter - 1
            if counter == 0:
                end = i
                #print("valid paranthesis")
                break
            else:
                continue
    final_ddl = ddl[0:end + 1]
    return final_ddl

def convert_to_snowflake_ddl(create_ddl):
    output_string = re.sub(r"VARCHAR2\((\d+) CHAR\)", r'VARCHAR(\1)', create_ddl)
    output_string = re.sub(r"VARCHAR2\((\d+) BYTE\)", r'VARCHAR(\1)', output_string)
    output_string = re.sub(r"FLOAT\((\d+)\)", r"FLOAT(\1)", output_string)
    output_string = re.sub(r"NVARCHAR2\((\d+)\)", r"VARCHAR(\1)", output_string)
    #output_string = re.sub(r"NUMBER\((\d+), (\d+)\)", r"NUMBER(\1,\1)", output_string)
    output_string = re.sub(r"TIMESTAMP\((\d+)\)", r"TIMESTAMP_NTZ(\1)", output_string)
    output_string = output_string.replace("ENABLE", "")
    output_string = output_string.replace("*", "38")
    output_string = output_string.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
    output_string = output_string.replace("SYSDATE ", "CURRENT_DATE ")
    output_string = output_string.replace("sysdate ", "CURRENT_DATE ")
    output_string = output_string.replace("CLOB ", "VARCHAR")
    output_string = output_string.replace("LONG ", "VARCHAR")
    output_string = output_string.replace("NCLOB ", "VARCHAR")
    output_string = output_string.replace("BLOB ", "BINARY")

    #output_string = output_string.replace("CUSTOMER_FORM_#", "CUSTOMER_FORM_#")
    return output_string
def oracle_to_snowflake_ddl(file_path):
    """Reads a SQL file and returns its contents as a string."""
    oracle_table_ddl = get_ddl_content(file_path)
    create_table_ddl = extract_create_table(oracle_table_ddl)
    snowflake_ddl = convert_to_snowflake_ddl(create_table_ddl)
    return snowflake_ddl

if __name__ == "__main__":
    #schema_name = sys.argv[1]
    files = os.listdir('./CANADA_COMIT_HIST_DM')
    # Print the list of files
    conn.cursor().execute("USE ROLE DEV_SYSADMIN")
    conn.cursor().execute("USE DATABASE *****")
    conn.cursor().execute("CREATE OR REPLACE SCHEMA *****")
    #conn.cursor().execute("USE SCHEMA *****")
    for filename in files:
            ddl = oracle_to_snowflake_ddl('./*****/'+filename)
            print(filename)
            conn.cursor().execute(ddl)
