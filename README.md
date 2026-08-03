# backpressure-aware-pipe

Producer → Redis stream → consumer → sink, with backpressure handling.

| Service  | Port |
| -------- | ---- |
| producer | 3000 |
| sink     | 3001 |
| consumer | 3002 |
| redis    | 6379 |

## Run

```sh
docker compose up -d --build
```

## Stats

```sh
curl -s http://localhost:3002/stats | jq
```

Returns `backlog`, `in_flight`, `oldest_event_age_ms`, `produced`, `delivered`, `retried`, `dead_lettered`.

## Stop individual services

```sh
docker compose stop consumer
docker compose stop producer
docker compose stop redis
```

Kill the consumer hard (SIGKILL):

```sh
docker compose kill -s SIGKILL consumer
```

## Stop everything

```sh
docker compose down -v       # also wipes redis data
```

## sh to run test
```sh
./scripts/chaos-test.sh
```

## Design Questions

### Redis Choices

- Which Redis primitive did you pick and why? What did you deliberately not pick (e.g. Pub/Sub, Lists vs. Streams) and what would break if you had?

- Used Streams. My choice was mostly due to the requirement stating we cannot lose messages, since Streams persist messages (until deletion, not implemented but we would need to cleanup). Combined with Redis AOF persistence, in case of a Redis crash we would be able to recover the messages. Also, the retry possibility was not stated, but it would be a really nice feature to have in case we ever need to reprocess messages.
I did not pick Pub/Sub, since it is fire-and-forget and anything published while the consumer is down is lost. I also did not pick Lists, since they have no consumer groups and no redelivery of in-flight messages if a consumer dies mid-processing.

- What happens when Redis itself restarts? What is lost with RDB vs. AOF persistence? 

- When Redis restarts, both the producer and consumer reconnect automatically and resume processing. I used AOF, since RDB saves snapshots of the DB at intervals, so we would have gaps of data that could be lost. Since the hit on throughput that AOF causes was not a concern here, we went with it. It's important to notice that we still have a small gap with AOF too, since the default `everysec` fsync can lose up to a second of writes in hard crashes.

### Alternatives

- Propose at least two alternatives to Redis for this pipeline (e.g. Kafka, RabbitMQ, NATS JetStream, SQS, Postgres-as-queue) and state concretely at which scale, durability requirement, or team constraint you would switch — and why you would not switch today.

- Kafka would be a good fit. It meets most of the requirements we have, mainly the requirement of not losing messages. At the moment, switching would not be the first priority for the project, since Redis can comfortably handle the load we have. Kafka, due to its greater complexity, would be a better choice once we need much higher throughput, longer message retention, or multiple independent consumer groups — and at that point memory could also start becoming a problem, since Redis keeps everything in RAM.

- RabbitMQ is always an option too, but only if the messages were more task oriented, with more routing rules being applied. I don't see a good fit here over Redis.

### Semantics

- Where exactly can duplicates arise in your design? What would it take to get effectively-once delivery to the sink?
- We can have duplicates in a few places, for example, if the consumer crashes after sending to the sink but before acknowledging. Also if the sink stored the event but we had a connection problem and send returns false. It's hard to get effectively-once delivery in the consumer, ideally the sink should be idempotent and dedupe the messages on its side. We send the id so it can be done.

### Backpressure

- Walk through the full path of a slowdown: sink stalls → what happens, in order, at each stage of your pipeline?
- In a sink stall, the consumer would start retrying the calls, and the jittered exponential backoff would increase the gaps between each call. This would slow down the consumer, since a batch only finishes when all of its events are done, and the producer would notice since it keeps monitoring the lag (`lag + pending` of the consumer group, checked every 250ms). Once the threshold set in the producer is hit (backlog >= 6000) the producer will stop sending messages, and only resumes once the backlog drops back to 3000. If the stall lasts longer than the 5 retries, the event goes to the DLQ and gets acked, so the pipeline drains instead of blocking forever.

- Why is "just let the Redis backlog grow" not an acceptable answer?
- Redis stores in memory, so if we treat it as Kafka for example, we could start having memory issues. Once we hit memory limit we can start having errors or having dropped keys. Also we would have probably old and stale data.

### Scaling

- What is the first bottleneck at 50k events/sec? How do multiple consumer instances change your delivery guarantees and your backpressure story?
- First one is the sink rate. Also, one HTTP call per event would start being a problem with the average latency we have today, so we would need to start thinking about batching.
But if we imagine the sink rate is not a problem in this case, Redis would be the one. Right now we are using a single stream, which means a single Redis node, since Redis Cluster shards by key and one stream is one key. With that amount of events going to a single thread, CPU and memory would be a problem since it's at the limit of Redis' design. We would need to start thinking about having multiple streams, partitioned by user, or Kafka. In this case the backpressure logic we have in the producer would also have to be refactored.
About the delivery guarantees, they stay the same, at-least-once, since each entry is still handed to only one consumer of the group. What changes is that the duplicates can now happen at the same time and not only one after the other, since we have multiple instances. We could add SET NX checks, but in the end the dedupe needs to be done in the sink.