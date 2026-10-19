const { Kafka } = require("kafkajs");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const TOPIC = "test-topic";
const GROUP_ID = "grupo-demo-consumidor-web";
const MAX_BACKLOG = 50;

const app = express();
const server = http.createServer(app); //Servidor HTTP basado en Express
const io = new Server(server);

// Servir archivos estáticos (CSS, JS, etc.)
app.use(express.static(path.join(__dirname)));

// Config Kafka
const kafka = new Kafka({ 
  brokers: ["kafka:9092"],
  retry: {
    initialRetryTime: 1000,
    retries: 10
  }
});

// Últimos mensajes recibidos, para enviarlos a los clientes que se conectan después
const messageBacklog = [];

// Función para formatear la fecha
function formatTimestamp(timestamp) {
  try {
    if (!timestamp) return new Date().toLocaleTimeString();
    
    const date = new Date(parseInt(timestamp));
    if (isNaN(date.getTime())) {
      return new Date().toLocaleTimeString();
    }
    return date.toLocaleTimeString('es-ES', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      fractionalSecondDigits: 3
    });
  } catch (error) {
    return new Date().toLocaleTimeString();
  }
}

// Crea el tópico si no existe. KafkaJS no permite autocrearlo desde el consumer,
// por eso el consumer puede fallar si arranca antes que el producer.
async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const topics = await admin.listTopics();

    if (topics.includes(TOPIC)) {
      console.log(`Topic ${TOPIC} already exists`);
      return;
    }

    await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
      waitForLeaders: true,
      timeout: 15000
    });

    console.log(`Topic ${TOPIC} created`);
  } finally {
    await admin.disconnect();
  }
}

function publishMessage({ topic, partition, message }) {
  const payload = {
    content: message.value.toString(),
    timestamp: formatTimestamp(message.timestamp),
    offset: message.offset,
    partition: partition,
    rawTimestamp: message.timestamp
  };

  console.log(`[${topic}:${partition}:${message.offset}] ${payload.content}`);

  // Guardar en el backlog para nuevos clientes
  messageBacklog.push(payload);
  if (messageBacklog.length > MAX_BACKLOG) {
    messageBacklog.shift();
  }

  // Enviar mensaje a todos los clientes WebSocket
  io.emit("mensaje", payload);
}

async function startConsumer() {
  console.log("Creating Kafka consumer...");
  
  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    maxBytesPerPartition: 200000000, // 200MB máximo
    sessionTimeout: 30000, // 30 seg timeout
    heartbeatInterval: 3000 
  });

  try {
    console.log("Connecting to Kafka...");
    await consumer.connect();
    console.log("Consumer connected to Kafka");

    console.log(`Subscribing to ${TOPIC}...`);
    await consumer.subscribe({ 
      topic: TOPIC, 
      fromBeginning: true // Leer desde el primer mensaje
    });

    // Si el consumer se cae después de arrancar, reiniciar el proceso
    // para que Docker lo levante de nuevo.
    consumer.on(consumer.events.CRASH, (event) => {
      console.error("Consumer crashed:", event.payload.error);
      process.exit(1);
    });

    console.log("Listening for messages...");
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        publishMessage({ topic, partition, message });
      },
    });
  } catch (error) {
    await consumer.disconnect().catch(() => {});
    throw error;
  }
}

// Reintenta el arranque hasta que Kafka y el tópico estén listos
async function startConsumerWithRetry({ retries = 30, delayMs = 2000 } = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await ensureTopic();
      await startConsumer();
      return;
    } catch (error) {
      console.error(`Consumer start attempt ${attempt}/${retries} failed: ${error.message}`);
      if (attempt === retries) throw error;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

// Web server - servir HTML
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  // Al conectarse, enviar los últimos mensajes ya consumidos
  if (messageBacklog.length > 0) {
    socket.emit("backlog", messageBacklog);
  }
});

// Start application
async function main() {
  try {
    server.listen(3000, () => {
      console.log("Web server running on http://localhost:3000");
    });
    
    await startConsumerWithRetry();
    
  } catch (error) {
    console.error("Failed to start application:", error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log("\nShutting down gracefully...");
  process.exit(0);
});

main().catch(console.error);
