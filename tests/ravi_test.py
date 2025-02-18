import os
from cryptography.hazmat.primitives import serialization
import snowflake.connector

try:
    print("Trying to load .env")
    from dotenv import load_dotenv
    load_dotenv()
except Exception as e:
    print(f"Failed to load .env {e}")
    pass

# Read the private key file
with open("/Users/ravipotnuru/Downloads/snf_cred/SA_DP_VANNA_USER.p8", "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=os.getenv('PRIVATE_KEY_PASSPHRASE').encode() if os.getenv('PRIVATE_KEY_PASSPHRASE') else None
    )
# Get the private key in the correct format
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)    

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USERNAME'),
    private_key=pkb,
    warehouse=os.getenv('FLEET_WH'),    # Optional
    database=os.getenv('DEV_DB'),      # Optional
    schema=os.getenv('RPT_FLEET')          # Optional
)

cur = conn.cursor()
cur.execute("SELECT count(1) from prod_db.rpt_fleet.production_monthly")
print(cur.fetchone())

cur.close()
conn.close()