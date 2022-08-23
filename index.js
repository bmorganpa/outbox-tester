const { Kafka } = require("kafkajs")
const fetch = require("node-fetch")
const isEqual = require("lodash.isequal");

const kafka = new Kafka({
  clientId: "outbox-tester",
  brokers: ["localhost:9092"],
})

const admin = kafka.admin()

const consumer = kafka.consumer({ groupId: "outbox-tester-group" })

const returnedEventIds = []
const consumedMessageKeys = []

async function main() {
  await consumeAllMessages()

  let count = 0
  const interval = setInterval(async () => {
    if (count++ > 10) {
      clearInterval(interval);
      try {
        await retryWithCount(20, 250, () => {
          if (!isEqual(new Set(returnedEventIds), new Set(consumedMessageKeys))) {
            throw new Error("Mismatch between messages sent and consumed")
          } else if (!isEqual(returnedEventIds, consumedMessageKeys)) {
            throw new Error("Mismatch message order/duplicates")
          }
        })
      } catch (e) {
        console.error(`Failed with error: ${e.message}`)
        console.log({
          returnedEventIds,
          consumedMessageKeys
        })
      } finally {
        await consumer.disconnect()
        process.exit(0)
      }
    }
    postMessage()
  }, 100);
}
 main()

async function consumeAllMessages() {
  await admin.deleteTopics({
      topics: [ "outbox_topic" ]
  })
  await admin.createTopics({
      topics: [{
        topic: "outbox_event",
        numPartitions: 1,
        replicationFactor: 1
      }]
  })

  await consumer.connect()
  await consumer.subscribe({ topic: "outbox_topic" })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      consumedMessageKeys.push(message.key.toString())
    }
  })
}

async function postMessage() {
  const response = await fetch("http://localhost:8080/outbox", {
    method: "POST",
    body: '{"foo": "bar"}',
    headers: {
      "Content-Type": "application/json",
      "X-TEST-SEND-DELAY-MS": 0,
      "X-TEST-DELETE-DELAY-MS": 0,
      "X-TEST-DELETE-PERCENT": 50,
    }
  })
  if (response.ok) {
    returnedEventIds.push(await response.text())
  }
}

async function retryWithCount(
    count,
    interval,
    block,
    i = 0
) {
  while (i++ < count) {
    try {
      block()
    } catch(e) {
      await delay(interval)
    }
  }
  block()
}

function delay(duration) {
   return new Promise(function(resolve) {
       setTimeout(resolve, duration)
   });
}
