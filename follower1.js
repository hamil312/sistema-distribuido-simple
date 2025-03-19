const express = require("express");
const axios = require("axios");
const cron = require('cron');
const app = express();
const EventEmitter = require('events');
app.use(express.json());

let data = {};
let isPartitioned = false; 
let partitionMode = "availability"; 
const followers = ["http://localhost:3001", "http://localhost:3003"];
const NODES = { 1: "http://localhost:3001", 2: "http://localhost:3002", 3: "http://localhost:3003" };
const NODE_ID = process.argv[2] || 2;

let leaderId = process.argv[2] || 1;
let electionInProgress = false;

const nodes = new Map();

app.get("/get-leader", (req, res) => {
  res.json({ leaderId });
});

app.use((req, res, next) => {
  if (isPartitioned) {
    return res.status(503).json({ error: "Nodo en partición de red" });
  }
  next();
});

app.post("/election", async (req, res) => {
  const { senderId } = req.body;
  if (senderId < NODE_ID) {
    console.log(`Nodo ${NODE_ID}: Respondiendo a elección de ${senderId}`);
    await axios.post(`${NODES[senderId]}/alive`, { senderId: NODE_ID });
    if (!electionInProgress) startElection();
  }
  res.sendStatus(200);
});

app.post("/alive", (req, res) => {
  electionInProgress = false;
  res.sendStatus(200);
});

app.post("/coordinator", (req, res) => {
  leaderId = req.body.leaderId;
  console.log(`Nodo ${NODE_ID}: Nuevo líder es ${leaderId}`);
  electionInProgress = false;
  res.sendStatus(200);
});

function startElection() {
  electionInProgress = true;
  const higherNodes = Object.keys(NODES).filter((id) => id > NODE_ID);

  if (higherNodes.length === 0) {
    declareAsLeader();
  } else {
    higherNodes.forEach(async (id) => {
      try {
        await axios.post(`${NODES[id]}/election`, { senderId: NODE_ID });
      } catch (err) {
        console.log(`Nodo ${NODE_ID}: Nodo ${id} no responde`);
      }
    });

    setTimeout(() => {
      if (electionInProgress) declareAsLeader();
    }, 5000);
  }
}

async function declareAsLeader() {
  leaderId = NODE_ID;
  console.log(`Nodo ${NODE_ID}: Soy el nuevo líder`);

  const requests = Object.values(NODES).map(async (url) => {
    if (url !== NODES[NODE_ID]) {
      try {
        await axios.post(`${url}/coordinator`, { leaderId: NODE_ID });
      } catch (error) {
        console.log(`Nodo ${NODE_ID}: Fallo al notificar a ${url}, reintentando...`);
        setTimeout(() => axios.post(`${url}/coordinator`, { leaderId: NODE_ID }).catch(() => {}), 3000);
      }
    }
  });

  await Promise.all(requests);
}

setInterval(() => {
  if (leaderId !== NODE_ID) {
    axios
      .get(`${NODES[leaderId]}/health`)
      .catch(() => {
        console.log(`Nodo ${NODE_ID}: Líder ${leaderId} caído. Reintentando verificación...`);

        setTimeout(() => {
          axios.get(`${NODES[leaderId]}/health`)
            .catch(() => {
              console.log(`Nodo ${NODE_ID}: Confirmado fallo del líder ${leaderId}. Iniciando elección...`);
              startElection();
            });
        }, 3000);
      });
  }
}, 3000);

app.get("/health", (req, res) => res.sendStatus(200));

app.post('/heartbeat', (req, res) => {
  const { nodeId } = req.body;
  nodes.set(nodeId, Date.now());
  res.send({ status: 'ACK' });
});

setInterval(() => {
    const now = Date.now();
    nodes.forEach((lastTime, nodeId) => {
      if (now - lastTime > 10000) {
        console.log(`Nodo ${nodeId} caído! ${lastTime}`);
        nodes.delete(nodeId);
      }
    });
  }, 5000);

const job = new cron.CronJob('*/3 * * * * *', async () => {
  try {
    await axios.post(`${NODES[leaderId]}/heartbeat`, { nodeId: NODE_ID });
    console.log(`Heartbeat enviado por ${NODE_ID}`);
  } catch (err) {
    console.error(`Error en heartbeat (${NODE_ID}): ${err.message}`);
  }
});

job.start();

app.post("/write", async (req, res) => {
  if(leaderId == NODE_ID){
    const { key, value } = req.body;
    const consistency = req.query.consistency || "low";

    if (isPartitioned) {
      if (partitionMode === "consistency") {
        return res.status(503).json({ error: "Sistema inconsistente por partición" });
      } else {
        data[key] = value;
        return res.json({ message: "Dato aceptado (Disponibilidad)", data: value });
      }
    }

    data[key] = value;
    if (consistency === "high") {
      try {
        await Promise.all(
          followers.map((follower) =>
            axios.post(`${follower}/replicate`, { key, value })
          )
        );
        res.json({ message: "Dato replicado (Consistencia)", data: value });
      } catch (err) {
        res.status(500).json({ error: "Error en replicación" });
      }
    } else {
      followers.forEach(async (follower) => {
        try {
          await axios.post(`${follower}/replicate`, { key, value });
        } catch (err) {
          console.log(`Error replicando a ${follower}: ${err.message}`);
        }
      });
      res.json({ message: "Dato aceptado (Baja latencia)", data: value });
    }
  } else{
    console.log("Can't write as a follower")
    res.json({ message: "Can't write as a follower"});
  }
  
});

app.post("/replicate", (req, res) => {
  const { key, value } = req.body;
  data[key] = value;
  res.json({ message: "Réplica exitosa" });
});

app.post("/set-partition-mode", (req, res) => {
  partitionMode = req.body.mode;
  res.json({ message: `Modo partición: ${partitionMode}` });
});

app.get("/data", (req, res) => res.json({ data }));

app.get("/read/:key", (req, res) => {
  const value = data[req.params.key];
  res.json({ value });
});

app.post("/toggle-partition", (req, res) => {
  isPartitioned = !isPartitioned;
  res.json({ message: `Partición: ${isPartitioned ? "ACTIVA" : "INACTIVA"}` });
});

const PORT = process.argv[3] || 3002;
app.listen(3002, () => console.log("Node2 en puerto 3002"));