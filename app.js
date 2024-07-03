let webscoketIP = process.env.WEBSOCKETIP;
let topicNm = process.env.TOPIC;
let groupNm = process.env.GROUPID;
let kafkaIP = process.env.KAFKAIP;
// let webscoketIP = "ws://155.155.4.161:9511";
// let topicNm = "tp_VHF_Packet_511";
// let groupNm = "tp_511";
// let kafkaIP = "155.155.4.161:9092";

//console.log(webscoketIP, topicNm, groupNm, kafkaIP);

const { Kafka, ConfigSource } = require("kafkajs");
const protobuf = require("protobufjs");
var WebSocket = require("ws");
// var WebSocketServer = WebSocket.Server,
//   wss = new WebSocketServer({ port: 6900 });
//9101, 9102, 9103

const kafka = new Kafka({
  clientId: "my-app",
  brokers: [kafkaIP],
});

async function decodeTestMessage(buffer) {
  const root = await protobuf.load("./proto/met.proto");
  const testMessage = root.lookupType("mda.MET_Signal");
  const err = testMessage.verify(buffer);
  if (err) {
    throw err;
  }
  const message = testMessage.decode(buffer);
  return testMessage.toObject(message);
}

const consumer = kafka.consumer({
  groupId: groupNm,
});

const ws = new WebSocket(webscoketIP);
ws.onopen = () => {
  console.log("ws opened on browser");
};

// let client = [];
// wss.on("connection", function connection(ws) {
//   console.log("connection client");

//   client.push(ws);
// });

const initKafka = async () => {
  console.log("start subscribe");
  await consumer.connect();
  await consumer.subscribe({
    topic: topicNm,
    fromBeginning: false,
  });
  //wss.on("connection", async (ws, request) => {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      //if (err) throw err;

      //console.log(message.value);
      let obj = await decodeTestMessage(message.value);
      //let obj = message.value;
      console.log(obj, new Date().toLocaleTimeString());

      let deviceId = obj.DeviceId;
      let temp = obj.Temp;
      let humidity = obj.Humidity;
      let windDirection = obj.WindDirection;
      let windSpeed = obj.WindSpeed;
      let visivility = obj.Visivility;
      let timeStamp = obj.TimeStamp;

      let sendMsg;

      if (ws.readyState === ws.OPEN) {
        //console.log(sendMsg);
        let resultObj = {};
        //resultObj["remote"] = remote;
        resultObj["deviceId"] = deviceId;
        resultObj["temp"] = temp;
        resultObj["humidity"] = humidity;
        resultObj["windDirection"] = windDirection;
        resultObj["windSpeed"] = windSpeed;
        resultObj["visivility"] = visivility;
        resultObj["timeStamp"] = timeStamp;

        sendMsg = JSON.stringify(resultObj);
        ws.send(sendMsg);
      }
    },
    //});
  });
};

//console.log("클라이언트 접속");
initKafka();
