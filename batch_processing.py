from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializar Spark Session
spark = SparkSession.builder.appName("MinCIT_BatchProcessing").getOrCreate()
# Opcional: configurar el nivel de log para que sea menos verboso
spark.sparkContext.setLogLevel("ERROR")

# Rutas
DATA_PATH = "/home/vboxuser/bigdata_proyecto/datasets/MinCIT-Registro-Activos-Informacion.csv." 
# Ruta de salida para el dataset limpio y particionado en formato Parquet
OUTPUT_PATH = "/home/vboxuser/bigdata_proyecto/datasets/MinCIT-Registro-Activos-Informacion"

try:
    # 1. Cargar el Dataset
    # Usamos delimiter='|' porque los archivos de datos.gov.co a menudo usan delimitadores distintos a la coma.
    # Si tu archivo CSV usa coma (,), cambia delimiter=","
    df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH, delimiter=";")

    print("--- 1. Datos cargados correctamente (MinCIT) ---")
    df.printSchema()

    # 2. Limpieza y Transformación
    # Seleccionamos y limpiamos las columnas clave para el análisis:
    # 'Dependencia', 'Clasificacion_Informacion', 'Tipo_Dato'
    
    # Renombrar columnas para simplificar
    df_clean = df.withColumnRenamed("Clasificaci_n_Informaci_n", "Clasificacion_Informacion") \
                 .withColumnRenamed("Tipo_Dato", "Tipo_Dato") \
                 .withColumnRenamed("Dependencia", "Dependencia")
    
    # Limpieza: Eliminar filas donde la 'Dependencia' o la 'Clasificacion' sean nulas
    # Estas son las claves para el particionamiento y el análisis EDA.
    df_clean = df_clean.na.drop(subset=["Dependencia", "Clasificacion_Informacion"])

    print("\n--- 2. Datos transformados y limpios (Primeras 5 filas) ---")
    df_clean.show(5, truncate=False)
    df_clean.printSchema()

    # 3. Almacenar los Resultados Limpios con Particionamiento (CRÍTICO para el proyecto)
    # Se guarda en formato Parquet, optimizado para Spark, y se particiona por 'Dependencia'.
    df_clean.write.mode("overwrite").partitionBy("Dependencia").parquet(OUTPUT_PATH)

    print(f"\n--- 3. Datos procesados guardados en Parquet particionado por Dependencia en: {OUTPUT_PATH} ---")
    print("¡El procesamiento Batch de Carga ha finalizado con éxito!")

except FileNotFoundError:
    print(f"ERROR: Archivo no encontrado. Asegúrate de que el archivo CSV MinCIT esté en la ruta: {DATA_PATH}")
except Exception as e:
    print(f"Ocurrió un error durante el procesamiento Batch de carga: {e}")

finally:
    spark.stop()
