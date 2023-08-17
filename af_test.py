from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'm_popov',
    'start_date': datetime(2023, 8, 17)
}


@dag(dag_id='de_test', schedule_interval='0 */12 * * *', default_args=DEFAULT_ARGS)
def my_dag():
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='',
        sql="""
            CREATE TABLE IF NOT EXISTS api_cannabis  (
            id int,
            uid text,
            strain text, 
            cannabinoid_abbreviation text,
            cannabinoid text,
            terpene text, 
            medical_use text,
            health_benefit text,
            category text,
            type text,
            buzzword text,
            brand text);
          """
    )

    def extract():
        import requests
        r = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10')
        r_list = []
        for _i in r.json():
            r_list.append((_i['id'],
                           _i['uid'],
                           _i['strain'],
                           _i['cannabinoid_abbreviation'],
                           _i['terpene'],
                           _i['medical_use'],
                           _i['health_benefit'],
                           _i['category'],
                           _i['type'],
                           _i['buzzword'],
                           _i['brand']))
        return r_list

    get_api = PythonOperator(task_id='get_api',
                             python_callable=extract)

    def insert_postgres(rows_to_insert):
        pg_hook = PostgresHook(postgres_conn_id='')
        conn = pg_hook.get_conn()
        curs = conn.cursor()
        rows_to_insert = rows_to_insert.replace('[', '', 1).replace(']', '', -1)
        curs.execute(f"""insert into api_cannabis VALUES {rows_to_insert}""")
        conn.commit()
        curs.close()
        conn.close()

    load = PythonOperator(
        task_id='load',
        python_callable=insert_postgres,
        op_kwargs=dict(rows_to_insert='{{ ti.xcom_pull(task_ids="get_api")}}')
    )

    create_table >> get_api >> load


my_dag = my_dag()
