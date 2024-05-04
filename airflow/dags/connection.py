import os
from airflow.models.connection import Connection


conn = Connection(
    conn_id="sample_aws_connection",
    conn_type="aws",
    login="AKIAQ3EGSYYN2PEUP6W4",  # Reference to AWS Access Key ID
    password="tCOgJ8f7HNYIK77208tIcSiydRFYrlW9d8oLK3nV",  # Reference to AWS Secret Access Key
    extra={
        # Specify extra parameters here
        #"region_name": "eu-central-1",
    },
)

# Generate Environment Variable Name and Connection URI
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
print(f"{env_key}={conn_uri}")
# AIRFLOW_CONN_SAMPLE_AWS_CONNECTION=aws://AKIAQ3EGSYYN2PEUP6W4:tCOgJ8f7HNYIK77208tIcSiydRFYrlW9d8oLK3nV@/?region_name=eu-central-1

os.environ[env_key] = conn_uri
print(conn.test_connection())  # Validate connection credentials. 