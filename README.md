# 📨 Kafka - Saludos Aleatorios en Tiempo Real

Proyecto Docker que demuestra un flujo completo de **Apache Kafka**: un productor genera saludos aleatorios en español con la fecha y hora de Buenos Aires, y un consumidor los retransmite por **WebSockets** a una vista web en tiempo real. Incluye **Kafbat UI** para inspeccionar el cluster desde el navegador.

## 🛠 Tecnologías

- **Apache Kafka 4.3.1** (modo KRaft, sin ZooKeeper) — broker de mensajería
- **Python** + `kafka-python` — productor
- **Node.js** + `kafkajs` + **Express** + **Socket.IO** — consumidor y servidor web
- **Kafbat UI** — panel web para monitorear topics, particiones, offsets y consumer groups
- **Docker & Docker Compose** — orquestación de servicios

## 📦 Estructura del proyecto

```
Kafka-Saludos-Aleatorios/
├── README.md
└── Kafka/
    ├── docker-compose.yml
    ├── producer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── producer.py
    └── consumer/
        ├── Dockerfile
        ├── package.json
        ├── index.js
        ├── index.html
        └── styles.css
```

## 🚀 Cómo ejecutar

```bash
cd Kafka
docker compose up -d --build
```

Una vez levantados los servicios:

| Servicio   | URL                     | Descripción                                   |
|------------|-------------------------|-----------------------------------------------|
| App web    | http://localhost:3000   | Vista en tiempo real de los mensajes de Kafka |
| Kafbat UI  | http://localhost:8080   | Panel de inspección del cluster Kafka         |
| Kafka      | localhost:9092          | Broker (bootstrap server)                     |

Detener todo:

```bash
docker compose down
```

## ⚙️ Cómo funciona

### Flujo general

```
Producer (Python)          Kafka (broker)           Consumer (Node.js)           Navegador
─────────────────          ──────────────           ──────────────────           ─────────
genera saludo cada 10s
       │
       ▼
producer.send() ────────►  topic "test-topic" ────► eachMessage() ◄──────────── socket.io
                           (3 particiones)              │                          ▲
                                                        ▼                          │
                                                  io.emit("mensaje") ──────────────┘
                                                  + guarda backlog
```

### Producer (`producer/producer.py`)

1. Espera a que Kafka esté disponible (`wait_for_kafka`, hasta 30 intentos).
2. Crea un `KafkaProducer` con `acks='all'`, reintentos y compresión gzip.
3. En un loop infinito cada **10 segundos**:
   - Elige un saludo aleatorio de una lista fija (`"¡Hola!"`, `"¡Buenos días!"`, etc.).
   - Arma el mensaje con la fecha y hora actual de **Buenos Aires (UTC-3)**.
   - Envía el mensaje al topic `test-topic` y espera la confirmación (partition + offset).

Ejemplo de mensaje:

```
¡Buenos días! Hoy es 20 de Octubre de 2026 y son las 14:35.
```

### Consumer (`consumer/index.js`)

1. **`ensureTopic()`**: crea el topic `test-topic` con 3 particiones si no existe (KafkaJS no puede autocrearlo desde el consumer).
2. **`startConsumer()`**: se suscribe a `test-topic` con `fromBeginning: true` dentro del grupo `grupo-demo-consumidor-web`, y procesa cada mensaje.
3. **`publishMessage()`**: por cada mensaje:
   - Arma un payload con `content`, `timestamp`, `offset` y `partition`.
   - Lo guarda en un **backlog** (últimos 50 mensajes) para clientes que lleguen tarde.
   - Lo emite por WebSocket a todos los clientes: `io.emit("mensaje", payload)`.
4. Sirve los archivos estáticos y `index.html` en `/` (Express, puerto **3000**).
5. Si el consumer se cae, sale con código 1 para que Docker lo levante de nuevo; el arranque reintenta hasta 30 veces.

### WebSockets (frontend, `index.html`)

- El navegador se conecta con `io()` (Socket.IO) al puerto 3000.
- **`socket.on("mensaje")`**: recibe cada mensaje en tiempo real y lo renderiza en el DOM, con deduplicación por `partition:offset`.
- **`socket.on("backlog")`**: recibe el historial de los últimos 50 mensajes al conectarse.
- Incluye estadísticas en vivo (total de mensajes, mensajes/segundo, particiones vistas) y botones para **pausar** (los mensajes se bufferizan) y **limpiar**.

### Kafbat UI (`kafka-ui`)

Panel web en el puerto **8080** conectado a `kafka:9092`. Permite ver:

- Topics y sus configuraciones (por ejemplo `test-topic`, 3 particiones).
- Particiones, offsets y replicación.
- Consumer groups con su lag (por ejemplo `grupo-demo-consumidor-web`).
- Explorador de mensajes del topic.
