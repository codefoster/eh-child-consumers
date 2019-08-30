import { EventHubClient, EventData } from "@azure/event-hubs";
require('dotenv').config();

async function main(): Promise<void> {
  const client = new EventHubClient(process.env.EVENT_HUB_CONNECTION_STRING_SEND as string);
  const producer = client.createProducer();

  let i = 0;
  setInterval(async () => {
    i++;
    let msg:EventData = { body: {
      msgId: i,
      createTimestamp: Date.now(),
      // createTimestamp: Date.now() - (lag * 1000),
      id: (i % 50) + 1
    } };
    producer.send(msg);
    console.log(msg);
  }, 4)
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
