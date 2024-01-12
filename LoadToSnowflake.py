import snowflake.connector
import yaml
import boto3

s3 = boto3.client("s3")
with open("config.yaml", "r") as f:
    data = yaml.safe_load(f)

conn = snowflake.connector.connect(
    user="****",
    password="****",
    account="****",
    warehouse="DEV_COMPUTE_WH_WH",
    DATABASE="****",
)


print(conn)
host = "****"
port = 1521
sid = "****"
dsn = "****"
username = "****"
password = "****"
AWS_ACCESS_KEY_ID = data["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = data["AWS_SECRET_ACCESS_KEY"]
bucket_name = data["bucket_name"]

# USE DATABASE
conn.cursor().execute("USE ROLE DEV_SYSADMIN")
conn.cursor().execute("USE DATABASE ****")
conn.cursor().execute("USE SCHEMA ****")

prefix = "Dev/PWKDM/****/"

# List objects in the specified prefix (folder)
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

# Extract the common prefixes (folder names)
folders = [
    common_prefix.get("Prefix").split(prefix)[1].replace("/", "")
    for common_prefix in response.get("CommonPrefixes", [])
]

for table_name in folders:
    print(table_name)
    conn.cursor().execute(
        f"""
        COPY INTO {table_name} FROM s3://app-****-****/****/****/****/{table_name}/
        CREDENTIALS =(AWS_KEY_ID='****' AWS_SECRET_KEY='****/****')
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|', SKIP_HEADER = 1 
        record_delimiter = '\n'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        ESCAPE_UNENCLOSED_FIELD=NONE
        )
        PATTERN = '.*.csv'
        """
    )