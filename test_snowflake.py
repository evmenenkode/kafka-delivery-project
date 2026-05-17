import snowflake.connector
from config import SNOWFLAKE_CONFIG

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

cursor.execute("SELECT CURRENT_VERSION()")
print(cursor.fetchone())

cursor.close()
conn.close()