import { EventHubClient, EventData, EventPosition } from "@azure/event-hubs";
import { Subject } from "rxjs";
import { filter, map } from "rxjs/operators";
import moment = require('moment');
import commandLineArgs = require('command-line-args');
import { spawn } from 'child_process';
import { writeFile, appendFile } from "fs";
require('dotenv').config();

const config = commandLineArgs([
  { name: 'count', alias: 'c' },
  { name: 'id', alias: 'i' },
  { name: 'delay', alias: 'd', defaultValue: 0 },
  { name: 'deleteOutputFiles', alias: 'o', type: Boolean, defaultValue: true }
]);

let events = new Subject();
let events$ = events.asObservable();
let messageCount = 0;
let startTime = moment();
let receiveLatencies: number[] = [];
let processLatencies: number[] = [];

main();

async function main() {

  if (config['deleteOutputFiles']) {
    //TODO: delete output files
  }

  //spawn child processes
  if (config['count']) {
    console.log(`spawning ${config['count']} consumers...`);
    range(1, config['count'])
      .map(i => spawn('node', ['consumer', '--id', i.toString()]))
      .map(proc => {
        proc.stdout.on('data', data => console.log(data.toString()));
        proc.stderr.on('data', data => console.log(data.toString()));
      });
  }

  //this is a child process
  if (config['id']) {
    const client = new EventHubClient(process.env.EVENT_HUB_CONNECTION_STRING_LISTEN as string);

    let consumerGroupId = (config["id"] ? (config["id"] % 20) + 1 : "$Default");
    console.log(`[${config['id']}] Listening on consumer group ${consumerGroupId}`);

    //enumerate the partitions
    const partitionIds = await client.getPartitionIds();
    partitionIds.map(partitionId => {
      client
        .createConsumer(consumerGroupId.toString(), partitionId, EventPosition.latest())
        .receive(
          event => {
            event.body.receiveTimestamp = Date.now();
            let effectiveDelay = (config['delay'] == 0 ? 0 : (config['delay'] - moment(event.body.timestamp).diff(moment(), "millisecond")));
            setTimeout(() => events.next(event.body), effectiveDelay);
          },
          error => { console.error(error) },
        )
    });
    writeFile(`output/${config['id']}`, `Events for id ${config['id']}...`, err => console.log);
    events$
      .pipe(
        filter((e: any) => e.id == config['id'])
      )
      .subscribe(event => {
        messageCount++;
        let processTimestamp = moment();
        let receiveLatency = moment(event.receiveTimestamp).diff(event.createTimestamp, "milliseconds");
        let processLatency = processTimestamp.diff(event.receiveTimestamp, "milliseconds");
        receiveLatencies.push(receiveLatency);
        processLatencies.push(processLatency);

        appendFile(`output/${config['id']}`, JSON.stringify({
          createTimestamp: event.createTimestamp,
          receiveTimestamp: moment(event.receiveTimestamp),
          receiveLatency: receiveLatency,
          processTimestamp: processTimestamp.toDate(),
          processLatency: processLatency,
          averageReceiveLatency: Math.round(receiveLatencies.reduce((a, c) => a + c, 0) / receiveLatencies.length),
          averageProcessLatency: Math.round(processLatencies.reduce((a, c) => a + c, 0) / processLatencies.length),
          rate: moment().diff(startTime, "milliseconds") / messageCount
        }, null, 2), err => console.log);
      });
  }
}

function range(start: number, end: number) {
  return [...Array(end - start + 1)].map((_, i) => start + i)
}