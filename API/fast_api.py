import pandas as pd
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

def get_recommendations(advertiser_id: str, model: str):
    # Conección a la base
    connection = connect_to_rds()
    if connection is None:
        return {"message": "Failed to connect to the database"}

    current_date = datetime.date.today()- datetime.timedelta(days=1) #voy a buscar datos del dia anterior

    try:
        cursor = connection.cursor()

        # Defino la query en base al modelo
        if model == "TopProduct":

            query = f"SELECT product_id FROM top_product WHERE advertiser_id = '{advertiser_id}' AND date = '{current_date}' ORDER BY ranking LIMIT 20"
        
        elif model == "TopCTR":

            query = f"SELECT product_id FROM top_ctr WHERE advertiser_id = '{advertiser_id}' AND date = '{current_date}' ORDER BY ranking LIMIT 20"
        
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
    connection = connect_to_rds()
    
    if connection is None:
        return {"message": "Failed to connect to the database"}

    try:
        cursor = connection.cursor()

        # fecha actual
        current_date = datetime.date.today()

        # Defino rango de fechas
        end_date = current_date- datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=7)

        # diccionario para guardar resultados
        history = {}

        for date in range((end_date - start_date).days + 1):
            # itero sobre las fechas
            date_iter = start_date + datetime.timedelta(days=date)

            # query para recomendaciones del dia
            query = f"SELECT product_id FROM top_product WHERE advertiser_id = '{advertiser_id}' AND date = '{date_iter}' ORDER BY ranking LIMIT 20"
            cursor.execute(query)
            records = cursor.fetchall()

            # Almaceno recomendaciones del dia
            if records:
                history[str(date_iter)] = [record[0] for record in records]
            else:
                history[str(date_iter)] = "no data for this date"

        return history
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return {"message": "Error occurred while fetching data from the database"}

    finally:
        if connection:
            cursor.close()
            connection.close()


#devuelve un json con numero de advertisers y advertisers que más varían sus recomendaciones
@app.get("/stats")
def get_stats():
    # Conección a la base
    connection = connect_to_rds()
    if connection is None:
        return {"message": "Failed to connect to the database"}

    try:
        cursor = connection.cursor()

        # Fecha para buscar datos
        current_date = datetime.date.today()- datetime.timedelta(days=1)

        # Cantidad de advertisers activos
        cursor.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_product WHERE date = %s", (current_date,))
        num_advertisers = cursor.fetchone()[0]

        # Advertisers que más varían sus recomendaciones por día
        cursor.execute('''SELECT advertiser_id
                            FROM top_product
                            GROUP BY advertiser_id
                            ORDER BY COUNT(DISTINCT product_id) DESC
                            LIMIT 3''')
        advertisers_most_varied = cursor.fetchall()

        return {
            "num_advertisers": num_advertisers,
            "advertisers_most_varied": advertisers_most_varied
        }
    
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return {"message": "Error occurred while fetching data from the database"}

    finally:
        if connection:
            cursor.close()
            connection.close()
