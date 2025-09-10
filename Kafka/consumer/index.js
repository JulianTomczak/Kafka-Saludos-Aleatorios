const { Kafka } = require("kafkajs");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

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

async function startConsumer() {
  console.log("Creating Kafka consumer...");
  
  const consumer = kafka.consumer({
    groupId: `grupo-demo-${Date.now()}`, // ID único del grupo
    maxBytesPerPartition: 200000000, // 200MB máximo
    sessionTimeout: 30000, // 30 seg timeout
    heartbeatInterval: 3000 
  });

  console.log("Connecting to Kafka...");
  await consumer.connect();
  console.log("Consumer connected to Kafka");

  console.log("Subscribing to test-topic...");
  await consumer.subscribe({ 
    topic: "test-topic", 
    fromBeginning: true // Leer desde el primer mensaje
  });

  console.log("Listening for messages...");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = message.value.toString();
      const timestamp = formatTimestamp(message.timestamp);
      
      console.log(`[${topic}:${partition}:${message.offset}] ${msg}`);
      
      // Enviar mensaje a todos los clientes WebSocket
      io.emit("mensaje", {
        content: msg,
        timestamp: timestamp,
        offset: message.offset,
        partition: partition,
        rawTimestamp: message.timestamp
      });
    },
  });
}

// Web server - servir HTML
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);
});

// Start application
async function main() {
  try {
    server.listen(3000, () => {
      console.log("Web server running on http://localhost:3000");
    });
    
    await startConsumer();
    
  } catch (error) {
    console.error("Failed to start application:", error);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log("\nShutting down gracefully...");
  process.exit(0);
});

main().catch(console.error);