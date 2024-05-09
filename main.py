import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os
import shutil
import time

destination_folder = 'C:/Users/kevin/Desktop/Examen de Integracion/Respaldo'

def connect_to_mysql(host, username, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Conexión exitosa a la base de datos MySQL")
            return connection
    except Error as e:
        print(f"Error al conectarse a la base de datos MySQL: {e}")
        return None

def test_connection(connection):
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            db_version = cursor.fetchone()
            print(f"Versión de la base de datos MySQL: {db_version[0]}")
        except Error as e:
            print(f"Error al ejecutar la consulta: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Conexión cerrada.")


def insert_data_from_df(connection, df, local_id):
    try:
        cursor = connection.cursor()
        duplicates = 0
        for index, row in df.iterrows():
            id_transaccion = row['IdTransaccion']
            # Check if IdTransaccion already exists in the database
            cursor.execute("SELECT COUNT(*) FROM Ventas_Consolidadas WHERE IdTransaccion = %s", (id_transaccion,))
            result = cursor.fetchone()
            if result[0] == 0:  # No existe en la base de datos, se puede insertar
                # Convertir fecha al formato adecuado
                fecha = datetime.strptime(row['Fecha'], '%d/%m/%Y').strftime('%Y-%m-%d')
                # Convertir local_id a cadena
                id_local_str = str(local_id)
                sql = "INSERT INTO Ventas_Consolidadas (IdTransaccion, IdLocal, Fecha, IdCategoria, IdProducto, Cantidad, PrecioUnitario, TotalVenta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = (row['IdTransaccion'], id_local_str, fecha, row['IdCategoria'], row['IdProducto'], row['Cantidad'], row['PrecioUnitario'], row['TotalVenta'])
                cursor.execute(sql, values)
                connection.commit()
            else:
                duplicates += 1
        print(f"Se han omitido {duplicates} registros duplicados.")
    except Error as e:
        print(f"Error al insertar datos en la base de datos MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()

def move_file_to_backup_folder(source_path, destination_folder):
    try:
        # Obtener la fecha y hora actual
        now = datetime.now()
        current_date_time = now.strftime("%Y-%m-%d_%H-%M-%S")

        # Extraer el nombre del archivo de la ruta
        file_name = os.path.basename(source_path)

        # Construir la nueva ruta de destino con la fecha y hora actual en el nombre del archivo
        destination_path = os.path.join(destination_folder, f"{current_date_time}_{file_name}")

        # Mover el archivo a la carpeta de destino
        shutil.move(source_path, destination_path)

        print(f"Archivo movido exitosamente a: {destination_path}")
    except Exception as e:
        print(f"Error al mover el archivo: {e}")


# Detalles de conexión
host = "localhost"
username = "root"
password = "Mysql"
database = "reporteElectroFacil"
while True:
    # Conectarse a MySQL
    connection = connect_to_mysql(host, username, password, database)

    # Probar la conexión
    #test_connection(connection)

    # Lectura de datos CSV
    csv_files = ['Local1.csv', 'Local2.csv', 'Local3.csv', 'Local4.csv']

    for i, csv_file in enumerate(csv_files, start=1):
        print(f"Leyendo datos {csv_file}")
        try:
            # Intenta leer el archivo CSV
            df = pd.read_csv(f'C:/Users/kevin/Desktop/Examen de Integracion/Origen/{csv_file}', sep=';')
            print("El archivo CSV se ha leído correctamente:")
            # Convertir i a entero y pasar como IdLocal
            insert_data_from_df(connection, df, i)
            move_file_to_backup_folder(f'C:/Users/kevin/Desktop/Examen de Integracion/Origen/{csv_file}', destination_folder)
        except FileNotFoundError:
            # Maneja el error si el archivo no se encuentra
            print(f"¡Error! El archivo CSV del {csv_file} no se ha encontrado.")
        except pd.errors.ParserError:
            # Maneja el error si hay un problema al analizar el archivo CSV
            print(f"¡Error! No se pudo analizar el archivo CSV del {csv_file} correctamente.")
        except Exception as e:
            # Maneja cualquier otro tipo de error inesperado
            print(f"¡Error inesperado! {e}")
    
    # Esperar un minuto antes de la próxima ejecución
    time.sleep(15)

