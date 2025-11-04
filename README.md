# Tarea-3---Procesamiento-de-Datos-con-Apache-Spark
Diseñar e implementar soluciones de almacenamiento y procesamiento de grandes volúmenes de datos utilizando herramientas como Hadoop, Spark y Kafka, para desarrollar infraestructuras tecnológicas que soporten el análisis eficiente de datos.


Para ejecutar la solución en un entorno de Big Data (como una Máquina Virtual con Spark y Kafka):

Requisitos Previos

El archivo CSV de MinCIT debe estar descargado y guardado en: /home/vboxuser/bigdata_proyecto/datasets/MinCIT-Registro-Activos-Informacion.csv.

Los servicios de ZooKeeper y Kafka Broker deben estar inicializados y corriendo.

1. Iniciar Servicios de Infraestructura (2 Terminales)

Mantener estas terminales abiertas en PuTTY:

# Terminal 1: Iniciar ZooKeeper
/opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

# Terminal 2: Iniciar Kafka Server
/opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties


2. Fase Batch (Procesamiento y EDA)

Ejecutar en una tercera terminal solo una vez para generar el archivo Parquet limpio.

# Terminal 3: Ejecutar Carga, Limpieza y Particionamiento
spark-submit batch_processing.py

# Terminal 3: Ejecutar el Análisis Exploratorio (genera las métricas de la presentación)
spark-submit eda_spark.py


3. Fase Streaming (Análisis en Tiempo Real)

Ejecutar el productor y el consumidor en terminales separadas para la Tarea 3.

# Terminal 4: Ejecutar el Productor de Kafka (Simulación de datos)
python3 kafka_producer.py

# Terminal 5: Ejecutar el Consumidor de Spark Streaming (Análisis en tiempo real)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.
