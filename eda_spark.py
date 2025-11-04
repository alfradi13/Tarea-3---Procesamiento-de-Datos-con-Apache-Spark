from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, round, col, lit, sum as Fsum

# Inicializar Spark Session
spark = SparkSession.builder.appName("MinCIT_EDA").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Rutas
# El script debe leer los datos limpios y particionados generados por batch_processing.py
PROCESSED_DATA_PATH = "/home/vboxuser/bigdata_proyecto/datasets/processed_mincit_activos"
# Ruta de salida para almacenar los resultados del análisis (opcional, pero buena práctica)
OUTPUT_ANALYSIS_PATH = "/home/vboxuser/bigdata_proyecto/output/mincit_eda_results"

try:
    # 1. Cargar los Datos Limpios (desde Parquet, que es el formato de salida del Batch)
    df = spark.read.parquet(PROCESSED_DATA_PATH)

    print("--- 1. Datos limpios cargados desde Parquet (Activos MinCIT) ---")
    df.show(5, truncate=False)
    print(f"Total de registros limpios para análisis: {df.count()}")
    
    # -------------------------------------------------------------------------
    # 2. Análisis EDA para las Capturas de Pantalla
    # -------------------------------------------------------------------------

    # CAPTURA 1: Distribución de la Clasificación de la Información (Pública vs. Reservada)
    # Muestra el cumplimiento de las políticas de transparencia.
    total_registros = df.count()
    clasificacion_info = df.groupBy("Clasificacion_Informacion") \
                           .agg(count(lit(1)).alias("Total_Activos")) \
                           .withColumn("Porcentaje", round((col("Total_Activos") / total_registros) * 100, 2)) \
                           .orderBy(desc("Total_Activos"))
    
    print("\n--- CAPTURA 1: Distribución de la Clasificación de la Información (Análisis de Transparencia) ---")
    clasificacion_info.show(truncate=False)

    # CAPTURA 2: Top 5 Dependencias con mayor número de Activos
    # Identifica las áreas más intensivas en generación/manejo de información.
    top_dependencias = df.groupBy("Dependencia") \
                         .agg(count(lit(1)).alias("Total_Activos")) \
                         .orderBy(desc("Total_Activos")) \
                         .limit(5)
    
    print("\n--- CAPTURA 2: Top 5 Dependencias con mayor número de Activos ---")
    top_dependencias.show(truncate=False)

    # CAPTURA 3: Distribución de Activos por Tipo de Dato (Ej: Datos Personales, Geoespacial, etc.)
    # Muestra la naturaleza de la información manejada.
    tipo_dato_distribucion = df.groupBy("Tipo_Dato") \
                               .agg(count(lit(1)).alias("Total_Activos")) \
                               .orderBy(desc("Total_Activos")) \
                               .limit(10)
    
    print("\n--- CAPTURA 3: Top 10 Tipos de Datos manejados por el MinCIT ---")
    tipo_dato_distribucion.show(truncate=False)
    
    # CAPTURA 4: Conteo de Activos por Frecuencia de Actualización
    # Identifica la vigencia y dinamismo de los activos.
    frecuencia_actualizacion = df.groupBy("Frecuencia_Actualizacion") \
                                .agg(count(lit(1)).alias("Total_Activos")) \
                                .orderBy(desc("Total_Activos"))
    
    print("\n--- CAPTURA 4: Conteo de Activos por Frecuencia de Actualización ---")
    frecuencia_actualizacion.show(truncate=False)
    
    # CAPTURA 5: Análisis Cruzado - Top 3 Dependencias y su Clasificación
    # Muestra si las dependencias más activas manejan principalmente información pública o reservada.
    top_dependencia_clasificacion = df.groupBy("Dependencia", "Clasificacion_Informacion") \
                                      .agg(count(lit(1)).alias("Total_Activos")) \
                                      .orderBy(desc("Total_Activos")) \
                                      .limit(10) # Tomamos el top 10 de combinaciones
    
    print("\n--- CAPTURA 5: Análisis Cruzado - Dependencia vs. Clasificación de Información ---")
    top_dependencia_clasificacion.show(truncate=False)


    # Almacenar los resultados para documentación (Opcional)
    # Agrupamos todos los resultados en un solo DataFrame temporal para guardar
    # En un proyecto real, se guardaría cada tabla por separado. Aquí lo simplificamos.
    # top_dependencias.write.mode("overwrite").csv(OUTPUT_ANALYSIS_PATH + "/top_dependencias")
    
    print("\n¡El Análisis Exploratorio de Datos (EDA) ha finalizado con éxito!")

except Exception as e:
    print(f"Ocurrió un error durante el procesamiento Batch de análisis (EDA): {e}")

finally:
    spark.stop()
