from kafka import KafkaProducer, KafkaAdminClient
import time
import logging
import socket
from datetime import datetime, timezone, timedelta
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Diccionario para nombres de meses
MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

# Lista de saludos
SALUDOS = [
    "¡Hola!",
    "¡Buenos días!",
    "¡Buenas tardes!",
    "¡Buenas noches!",
    "¡Saludos!",
    "¡Que tengas un excelente día!",
    "¡Espero que estés teniendo un gran día!",
    "¡Un cordial saludo!"
]

# Zona horaria de Buenos Aires (UTC-3)
BUENOS_AIRES_OFFSET = timezone(timedelta(hours=-3))

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    logger.info("Waiting for Kafka to be ready...")
    
    max_attempts = 30
    for attempt in range(1, max_attempts + 1):
        try:
            with socket.create_connection(('kafka', 9092), timeout=5):
                logger.info("Kafka is reachable via socket")
            
            admin = KafkaAdminClient(
                bootstrap_servers="kafka:9092",
                request_timeout_ms=10000,
                retry_backoff_ms=2000
            )
            topics = admin.list_topics()
            admin.close()
            logger.info("Kafka admin connection successful!")
            return True
            
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{max_attempts}: {e}")
            if attempt >= max_attempts:
                logger.error("Failed to connect to Kafka")
                return False
            time.sleep(2)

def create_producer():
    """Create Kafka producer"""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                max_request_size=200_000_000, # 200MB
                retries=10,
                request_timeout_ms=30000,
                max_block_ms=60000,
                acks='all',
                compression_type='gzip'
            )
            logger.info("Kafka producer created")
            return producer
        except Exception as e:
            logger.warning(f"Producer attempt {attempt}/5: {e}")
            if attempt >= 5:
                raise e
            time.sleep(2)

def obtener_hora_buenos_aires():
    """Obtiene la fecha y hora actual de Buenos Aires (UTC-3)"""
    # Obtener hora actual con zona horaria de Buenos Aires
    buenos_aires_time = datetime.now(BUENOS_AIRES_OFFSET)
    return buenos_aires_time

def generar_mensaje_español():
    """Genera un mensaje en español con la fecha y hora de Buenos Aires"""
    # Obtener hora de Buenos Aires
    ahora = obtener_hora_buenos_aires()
    
    # Obtener componentes de la fecha
    dia = ahora.day
    mes_numero = ahora.month
    año = ahora.year
    hora = ahora.strftime("%H:%M")
    
    # Convertir número de mes a nombre
    mes = MESES_ES.get(mes_numero, "Desconocido")
    
    # Seleccionar saludo aleatorio
    saludo = random.choice(SALUDOS)
    
    # Generar mensaje
    mensaje = f"{saludo} Hoy es {dia} de {mes} de {año} y son las {hora}."
    
    return mensaje, ahora

def main():
    logger.info("Starting Kafka producer...")
    
    # Mostrar información de zona horaria
    ahora_bs_as = obtener_hora_buenos_aires()
    logger.info(f"Zona horaria configurada: Buenos Aires (UTC-3)")
    logger.info(f"Hora actual Buenos Aires: {ahora_bs_as.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    if not wait_for_kafka():
        logger.error("Exiting due to Kafka connection failure")
        return
    
    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Could not create producer: {e}")
        return
    
    i = 0
    try:
        while True:
            msg, hora_bs_as = generar_mensaje_español()
            
            # También mostrar la hora UTC para referencia
            hora_utc = datetime.now(timezone.utc)
            logger.info(f"Hora Buenos Aires: {hora_bs_as.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Hora UTC: {hora_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            
            future = producer.send("test-topic", msg.encode("utf-8"))
            
            try:
                metadata = future.get(timeout=10) #Espero que kafka confirme que recibio el mensaje
                logger.info(f"Mensaje {i+1}: {msg}")
                logger.info(f"Partition: {metadata.partition}, Offset: {metadata.offset}")
            except Exception as e:
                logger.warning(f"Confirmación fallida: {e}")
                logger.info(f"Enviado: {msg}")
            
            time.sleep(10)
            i += 1
            
    except KeyboardInterrupt:
        logger.info("Producer detenido por el usuario")
    except Exception as e:
        logger.error(f"Error fatal: {e}")
    finally:
        if 'producer' in locals():
            producer.flush() #Fuerza el envío de todos los mensajes pendientes en el buffer
            producer.close()
            logger.info("Producer cerrado correctamente")

if __name__ == "__main__":
    main()