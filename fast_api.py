import pandas as pd
import boto3
from io import StringIO
from fastapi import FastAPI
import psycopg2
import datetime


app = FastAPI()

# Establecemos connección a base de datos
def connect_to_rds():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="db-tp-avanzada.c7o4g4e22k4y.us-east-1.rds.amazonaws.com",
            port="5432",
            database="postgres"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

@app.get("/recommendations/{advertiser_id}/{model}")

#model=TopProduct o TopCTR

def get_recommendations(advertiser_id: int, model: str):
    # Conección a la base
    connection = connect_to_rds()
    if connection is None:
        return {"message": "Failed to connect to the database"}

    try:
        cursor = connection.cursor()

        # Defino la query en base al modelo
        if model == "TopProduct":

            query = f"SELECT product_id FROM top_product WHERE advertiser_id = '{advertiser_id}' AND date = CURRENT_DATE ORDER BY ranking LIMIT 20"
        
        elif model == "TopCTR":

            query = f"SELECT product_id FROM top_ctr WHERE advertiser_id = '{advertiser_id}' AND date = CURRENT_DATE ORDER BY ranking LIMIT 20"
        
        else:
            return {"message": "Invalid model. Please choose either 'TopProduct' or 'TopCTR'"}

        cursor.execute(query)
        records = cursor.fetchall()

        if not records:
            return {"message": f"No recommendations found for advertiser_id {advertiser_id}"}

        products = [record[0] for record in records]
        return {"advertiser_id": advertiser_id, "recommended_products": products}
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return {"message": "Error occurred while fetching data from the database"}

    finally:
        if connection:
            cursor.close()
            connection.close()



#historial de recomendaciones, 7 días, modelo top product
@app.get("/history/{advertiser_id}")
def get_history(advertiser_id: int):
    # Conección a la base
    c
    if connection is None:
        return {"message": "Failed to connect to the database"}

    try:
        cursor = connection.cursor()

        # Obtener la fecha actual
        current_date = datetime.date.today()

        # Definir la fecha límite para el historial (últimos 7 días)
        end_date = current_date
        start_date = end_date - datetime.timedelta(days=6)

        # Inicializar un diccionario para almacenar las recomendaciones para cada día
        history = {}

        for date in range((end_date - start_date).days + 1):
            # itero sobre las fechas
            date_iter = start_date + datetime.timedelta(days=date)

            # Definir la query para obtener las recomendaciones para este día
            query = f"SELECT product_id FROM top_product WHERE advertiser_id = '{advertiser_id}' AND date = '{date_iter}' ORDER BY ranking LIMIT 20"
            cursor.execute(query)
            records = cursor.fetchall()

            # Almacenar las recomendaciones para este día en el diccionario de historial
            history[str(date_iter)] = [record[0] for record in records]

        return history
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return {"message": "Error occurred while fetching data from the database"}

    finally:
        if connection:
            cursor.close()
            connection.close()


#devuelve un json con numero de advertisers, 
@app.get("/stats")
def get_stats():
    # Conección a la base
    connection = connect_to_rds()
    if connection is None:
        return {"message": "Failed to connect to the database"}

    try:
        cursor = connection.cursor()

        # Obtener la fecha actual
        current_date = datetime.date.today()

        # Cantidad de advertisers
        cursor.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_product")
        num_advertisers = cursor.fetchone()[0]

        # Advertisers que más varían sus recomendaciones por día
        cursor.execute("SELECT advertiser_id, COUNT(DISTINCT date) AS num_dates FROM top_product GROUP BY advertiser_id ORDER BY num_dates DESC LIMIT 5")
        advertisers_most_varied = cursor.fetchall()

        # Estadísticas de coincidencia entre ambos modelos para los diferentes advs.
        cursor.execute("SELECT COUNT(*) FROM (SELECT advertiser_id FROM top_product INTERSECT SELECT advertiser_id FROM top_ctr) AS intersection")
        num_shared_advertisers = cursor.fetchone()[0]

        return {
            "num_advertisers": num_advertisers,
            "advertisers_most_varied": advertisers_most_varied,
            "num_shared_advertisers": num_shared_advertisers
        }
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return {"message": "Error occurred while fetching data from the database"}

    finally:
        if connection:
            cursor.close()
            connection.close()