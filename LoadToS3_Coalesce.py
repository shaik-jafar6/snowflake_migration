import pandas as pd
import oracledb
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import boto3
from botocore.exceptions import ClientError
import yaml
import logging
import concurrent.futures
import sys
with open('config.yaml', 'r') as f:
    data = yaml.safe_load(f)
# Set the connection parameters
host = data['host']
port = data['port']
sid = data['sid']
dsn = data['dsn']
username = data['username']
password = data['password']
AWS_ACCESS_KEY_ID = data['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = data['AWS_SECRET_ACCESS_KEY']
Bucket_name = data['bucket_name']

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

engine = sqlalchemy.create_engine(f"oracle+cx_oracle://{username}:{password}@{host}/?service_name={sid}", arraysize=1000)

def return_strings_as_bytes(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.DB_TYPE_NUMBER:
        return cursor.var(int, arraysize=cursor.arraysize)
    else:
        return cursor.var(str, arraysize=cursor.arraysize, bypass_decode=True)

def decode_data(data):
    decoded_data = []
    for row in data:
        row = list(row)
        for i in range(len(row)):
            if isinstance(row[i], bytes):
                row[i] = row[i].decode("latin")
        decoded_data.append(row)
    return decoded_data

def convert_datetime_to_date(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_ns_dtype(df[column]):
            df[column] = df[column].dt.date
    return df
def write_data_s3(dataframe,table_name,area,schema_name):
    max_size_mb = 250
    dataframe = convert_datetime_to_date(dataframe)
    print(dataframe.info())
    # Calculate the estimated size of the DataFrame in MB
    df_size_mb = dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
    print(df_size_mb)
    if df_size_mb > max_size_mb:
        # Split the DataFrame into chunks based on a row count (you can adjust this)
        chunk_size = 500000
        num_chunks = len(dataframe) // chunk_size + 1
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            chunk_df = dataframe[start_idx:end_idx]
            csv_data = chunk_df.to_csv(index=False, encoding='utf-8', errors='ignore', sep='|')
            file_name = f"{table_name}_"+str(i)+".csv"
            s3_key = f"Dev/{sid}/{area}/{schema_name}/{table_name}/Delta/{file_name}"
            s3.put_object(Bucket=Bucket_name, Key=s3_key, Body=csv_data)
            print(f"DataFrame for {table_name} saved as CSV to S3 bucket: {Bucket_name}/{s3_key}")
    else:
        csv_data = dataframe.to_csv(index=False, encoding='utf-8', errors='ignore', sep='|')
        file_name = f"{table_name}.csv"
        s3_key = f"Dev/{sid}/{area}/{schema_name}/{table_name}/Delta/{file_name}"
        s3.put_object(Bucket=Bucket_name, Key=s3_key, Body=csv_data)
        print(f"DataFrame for {table_name} saved as CSV to S3 bucket: {Bucket_name}/{s3_key}")

def load_data_to_s3(sid,area,schema_name,table_name,query):
    orders_sql = f"""{query}""";
    print(orders_sql)
    try:
        df = pd.read_sql(orders_sql, engine)
    except Exception as e:
        print("Exception " + str(e))
        print(table_name)
        oracledb.init_oracle_client()
        with oracledb.connect(user=username, password=password,dsn=dsn) as conn:
            with conn.cursor() as cursor:
                #cursor.outputtypehandler = return_strings_as_bytes
                cursor.execute(f"{orders_sql}")
                columns = [col[0] for col in cursor.description]
                data = cursor.fetchall()
                #data = decode_data(data)
                df = pd.DataFrame(data, columns=columns)
                print(table_name, df.shape[0])
    write_data_s3(df,table_name,area,schema_name)

def get_list_tables(schema_name):
    schema_query = f"""SELECT table_name FROM all_tables WHERE owner = '{schema_name}' """
    oracledb.init_oracle_client()
    with oracledb.connect(user=username, password=password, dsn=dsn) as conn:
        with conn.cursor() as cursor:
            # cursor.outputtypehandler = return_strings_as_bytes
            cursor.execute(f"{schema_query}")
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
    return df["TABLE_NAME"]

if __name__ == "__main__":
    schema_name = str(input('Enter a schema name '))    
    table_names = get_list_tables(schema_name)  # Add more table names as needed
    num_threads = len(table_names)
    print(num_threads)
    max_workers = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for table_name in table_names:
            future = executor.submit(load_data_to_s3, sid, schema_name, table_name)
            futures.append(future)
        for future in futures:
            future.result()
