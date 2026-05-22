from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import json

# ---------------------------
# CONFIG
# ---------------------------
default_args = {
    "owner": "denis",
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}

# ---------------------------
# HELPER 
# ---------------------------
def serialize_dataframe(df):
    """
    Timestamp → string (ISO)
    """
    for col in df.columns:
        if str(df[col].dtype).startswith("datetime"):
            df[col] = df[col].astype(str)
    return df.to_dict(orient="records")


# ---------------------------
# EXTRACT USERS (INCREMENTAL)
# ---------------------------
def extract_users(**context):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    with sf_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(user_id) FROM RAW.raw_users")
        result = cursor.fetchone()[0]

    last_id = int(result.split("_")[1]) if result else 0

    query = f"""
        SELECT *
        FROM delivery_app_info.users
        WHERE CAST(SUBSTRING(user_id, 6) AS INT) > {last_id}
    """

    df = pg_hook.get_pandas_df(query)

    data = serialize_dataframe(df)

    context["ti"].xcom_push(key="users_data", value=data)


# ---------------------------
# EXTRACT PRODUCTS (INCREMENTAL)
# ---------------------------
def extract_products(**context):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    with sf_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(product_id) FROM RAW.raw_products")
        result = cursor.fetchone()[0]

    last_id = int(result.split("_")[1]) if result else 0

    query = f"""
        SELECT *
        FROM delivery_app_info.products
        WHERE CAST(SUBSTRING(product_id, 9) AS INT) > {last_id}
    """

    df = pg_hook.get_pandas_df(query)

    data = serialize_dataframe(df)

    context["ti"].xcom_push(key="products_data", value=data)


# ---------------------------
# LOAD USERS
# ---------------------------
def load_users_to_snowflake(**context):
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    users = context["ti"].xcom_pull(key="users_data")

    if not users:
        print("No new users")
        return

    with sf_hook.get_conn() as conn:
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO RAW.raw_users (raw_data, user_id)
            SELECT PARSE_JSON(%s), %s
        """

        for user in users:
            cursor.execute(
                insert_query,
                (json.dumps(user), user["user_id"])
            )

    print(f"Inserted {len(users)} users")


# ---------------------------
# LOAD PRODUCTS
# ---------------------------
def load_products_to_snowflake(**context):
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    products = context["ti"].xcom_pull(key="products_data")

    if not products:
        print("No new products")
        return

    with sf_hook.get_conn() as conn:
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO RAW.raw_products (raw_data, product_id)
            SELECT PARSE_JSON(%s), %s
        """

        for product in products:
            cursor.execute(
                insert_query,
                (json.dumps(product), product["product_id"])
            )

    print(f"Inserted {len(products)} products")


# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="postgres_to_snowflake_raw_incremental",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_users_task = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users,
    )

    load_users_task = PythonOperator(
        task_id="load_users",
        python_callable=load_users_to_snowflake,
    )

    extract_products_task = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
    )

    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products_to_snowflake,
    )

    extract_users_task >> load_users_task
    extract_products_task >> load_products_task