from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    description='Pipeline TP',
    schedule='0 4 * * *', #corre 4am
    start_date= datetime(2024, 5, 2),
    catchup=False,
) as dag:

    def read_csv_from_s3(**kwargs):
        file_names=['ads_views.csv','advertiser_ids.csv','product_views.csv']

        dfs = []  # lista de dfs

        s3_conn_id = 'aws_default'
        bucket_name = 'tpudesa2'
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_conn = s3_hook.get_conn()
        print(f"Connection to S3 established: {s3_conn}")
        
        for name in file_names:
            
            key = name
            print(f"Reading CSV file from S3 bucket: {bucket_name}, key: {key}")
            csv_object = s3_hook.get_key(key, bucket_name)
            print(f"CSV object: {csv_object}")
            csv_content = csv_object.get()['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            print(df.head())
            
            dfs.append(df)
        
        ads_views, advertiser_ids, product_views=tuple(dfs)

        today_date = pd.Timestamp.now().date() - timedelta(days=1) #leo datos del dia anterior
        today_date = today_date.strftime('%Y-%m-%d')

        ads_views_today = ads_views[ads_views['date'] == today_date]
        ads_views_filtered = ads_views_today[ads_views_today['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

        print(ads_views_filtered.head())

        product_views_today = product_views[product_views['date'] == today_date]
        product_views_filtered = product_views_today[product_views_today['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

        print(product_views_filtered.head())

        #los mando a S3
        csv_buffer_ads = StringIO()
        ads_views_filtered.to_csv(csv_buffer_ads, index=False)
        s3_hook.load_string(csv_buffer_ads.getvalue(), key='ads_filtered.csv', bucket_name='resultadostpxdia', replace=True)

        csv_buffer_product = StringIO()
        product_views_filtered.to_csv(csv_buffer_product, index=False)
        s3_hook.load_string(csv_buffer_product.getvalue(), key='product_filtered.csv', bucket_name='resultadostpxdia', replace=True)

        return

    read_csv_task = PythonOperator(
        task_id='read_csv_from_s3_task',
        python_callable=read_csv_from_s3,
        dag=dag,
        )
    
    def top_product(**kwargs):
        s3_conn_id = 'aws_default'
        bucket_name = 'resultadostpxdia'
        key = 'product_filtered.csv'
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        print(f"Reading CSV file from S3 bucket: {bucket_name}, key: {key}")
        s3_conn = s3_hook.get_conn()
        print(f"Connection to S3 established: {s3_conn}")
        csv_object = s3_hook.get_key(key, bucket_name)
        print(f"CSV object: {csv_object}")
        csv_content = csv_object.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(df.head())

        #para cada advertiser id, quiero los 20 product_id más visitados (en ese dia)
        top_products = df.groupby('advertiser_id')['product_id'].value_counts().reset_index(name='count')
        top_products['ranking'] = top_products.groupby('advertiser_id')['count'].rank(method='first', ascending=False)
        top_20_products_per_advertiser = top_products[top_products['ranking'] <= 20]
        top_20_products_per_advertiser = top_20_products_per_advertiser.drop(columns='count')

        today_date = pd.Timestamp.now().date() - timedelta(days=1)
        today_date = today_date.strftime('%Y-%m-%d')

        # Assuming 'date' is a column in the original DataFrame df
        top_20_products_per_advertiser['date']=today_date
        top_20_products_per_advertiser = top_20_products_per_advertiser[['date', 'advertiser_id', 'product_id', 'ranking']]

        print(top_20_products_per_advertiser.head())

        # Connect to RDS
        rds_connection_string = "postgresql://postgres:postgres@tp-rds.cdm2asygkn2c.us-east-1.rds.amazonaws.com:5432/postgres"
        engine = create_engine(rds_connection_string)
        print(engine)

        # Save the top 20 products per advertiser to the RDS table 'top_product'
        top_20_products_per_advertiser.to_sql('top_product', con=engine, if_exists='append', index=False)

        return
    
    top_product_task = PythonOperator(
        task_id='top_product',
        python_callable=top_product,
        dag=dag,
        )

    def top_ctr(**kwargs):
        s3_conn_id = 'aws_default'
        bucket_name = 'resultadostpxdia'
        key = 'ads_filtered.csv'
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        print(f"Reading CSV file from S3 bucket: {bucket_name}, key: {key}")
        s3_conn = s3_hook.get_conn()
        print(f"Connection to S3 established: {s3_conn}")
        csv_object = s3_hook.get_key(key, bucket_name)
        print(f"CSV object: {csv_object}")
        csv_content = csv_object.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(df.head())

        #para cada advertiser id, quiero los 20 product_id más visitados (en ese dia)
        impressions_df = df[df['type'] == 'impression']
        clicks_df = df[df['type'] == 'click']

        # Group by 'advertiser_id' and 'product_id' and count the number of rows in each group
        count_impressions = impressions_df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='count_impressions')
        count_clicks = clicks_df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='count_clicks')

        # Merge count_impressions and count_clicks on 'advertiser_id' and 'product_id'
        result_df = count_impressions.merge(count_clicks, on=['advertiser_id', 'product_id'], how='outer')

        # Fill NaN values with 0 (in case there are no impressions or clicks for a specific advertiser_id and product_id combination)
        result_df.fillna(0, inplace=True)

        # Convert count columns to integers
        result_df[['count_impressions', 'count_clicks']] = result_df[['count_impressions', 'count_clicks']].astype(int)

        result_df['count_impressions'] = result_df.apply(lambda row: row['count_clicks'] if row['count_impressions'] == 0 else row['count_impressions'], axis=1)

        result_df['ctr'] = result_df['count_clicks'] / result_df['count_impressions']

        # Add a column ranking that assigns a ranking for the product ids based on best to worst ctr
        result_df['ranking'] = result_df.groupby('advertiser_id')['ctr'].rank(method='first', ascending=False)
    
        # Filter for top 20 ranking
        top_20_ranking = result_df[result_df['ranking'] <= 20]

        top_20_ranking.drop(columns=['count_impressions', 'count_clicks','ctr'], inplace=True)

        today_date = pd.Timestamp.now().date() - timedelta(days=1)
        today_date = today_date.strftime('%Y-%m-%d')

        # Assuming 'date' is a column in the original DataFrame df
        top_20_ranking['date']=today_date

        # Select only the desired columns and rearrange them
        top_20_ranking = top_20_ranking[['date', 'advertiser_id', 'product_id', 'ranking']]

        print(top_20_ranking.head())

        # Connect to RDS
        rds_connection_string = "postgresql://postgres:postgres@tp-rds.cdm2asygkn2c.us-east-1.rds.amazonaws.com:5432/postgres"
        engine = create_engine(rds_connection_string)
        print(engine)

        # Save the top 20 products per advertiser to the RDS table 'top_product'
        top_20_ranking.to_sql('top_ctr', con=engine, if_exists='append', index=False)

        return
    
    top_ctr_task = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr,
        dag=dag,
        )


    read_csv_task >> [top_product_task,top_ctr_task]

   





