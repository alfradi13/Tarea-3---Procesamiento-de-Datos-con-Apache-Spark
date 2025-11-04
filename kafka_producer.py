import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'traffic_violations'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Datos de muestra para la simulación
VIOLATION_TYPES = [
    "Speeding", "RedLightViolation", "ParkingViolation", 
    "UnsafeLaneChange", "DUI", "SeatbeltViolation"
]

LOCATIONS = [
    "Calle 20", "Avenida Los Alpes", "Carrera 19", 
    "Vía al Mar", "Sector Centro", "Bulevar de la 45"
]

# Función para generar un registro de infracción simulado
def generate_violation_record():
    return {
        "ViolationType": random.choice(VIOLATION_TYPES),
        "Location": random.choice(LOCATIONS),
        "Severity": random.choice(["Low", "Medium", "High"]),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "FineAmount": round(random.uniform(50.00, 500.00), 2)
    }

print(f"Iniciando productor de infracciones. Enviando al topic: {KAFKA_TOPIC}")

try:
    while True:
        record = generate_violation_record()
        
        # Enviar registro a Kafka
        producer.send(KAFKA_TOPIC, value=record)
        producer.flush() 
        
        print(f"Enviando registro: {record['ViolationType']} en {record['Location']}")
        
        # Esperar un momento antes de enviar el siguiente (simulación de flujo de datos)
        time.sleep(random.uniform(0.5, 1.5))

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
    producer.close()
except Exception as e:
    print(f"Ocurrió un error en el productor: {e}")
