import fp from 'fastify-plugin'
import { createClient, type RedisClientType } from 'redis'
import { FastifyPluginAsync } from 'fastify'
import type { SinkEvent } from './sink-client'

declare module 'fastify' {
  interface FastifyInstance {
    redis: RedisClientType
  }
}

// A batch is processed concurrently, so COUNT doubles as the in-flight cap on sink requests. 
const READ_COUNT = 32
const READ_BLOCK_MS = 5000

const MAX_RETRIES = 5

const DEFAULT_RECLAIM_MIN_IDLE_MS = 60_000
const DEFAULT_RECLAIM_INTERVAL_MS = 30_000

const BACKOFF_BASE_MS = 100
const BACKOFF_FACTOR = 2

/**
 * Backoff is jittered by a random 1-2x. 
 */
function backoffDelay (retry: number): number {
  const base = BACKOFF_BASE_MS * Math.pow(BACKOFF_FACTOR, retry - 1)
  return base * (1 + Math.random())
}

async function sleep (ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

interface StreamEntry {
  id: string
  message: Record<string, string>
}

function toSinkEvent (message: Record<string, string>): SinkEvent {
  return {
    event_id: message.event_id,
    created_at: Number(message.created_at),
    payload: JSON.parse(message.payload) as Record<string, unknown>
  }
}

const redisConsumerPlugin: FastifyPluginAsync = async (fastify) => {
  const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'
  const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY ?? 'events'
  const REDIS_CONSUMER_GROUP = process.env.REDIS_CONSUMER_GROUP ?? 'event_consumers'
  const REDIS_CONSUMER_NAME = process.env.REDIS_CONSUMER_NAME ?? 'event-consumer'
  const REDIS_DLQ_KEY = process.env.REDIS_DLQ_KEY ?? `${REDIS_STREAM_KEY}:dlq`
  const REDIS_RETRIES_KEY = process.env.REDIS_RETRIES_KEY ?? 'retries'
  const REDIS_DELIVERED_KEY = process.env.REDIS_DELIVERED_KEY ?? 'delivered'
  const RECLAIM_MIN_IDLE_MS = Number(process.env.REDIS_RECLAIM_MIN_IDLE_MS ?? DEFAULT_RECLAIM_MIN_IDLE_MS)
  const RECLAIM_INTERVAL_MS = Number(process.env.REDIS_RECLAIM_INTERVAL_MS ?? DEFAULT_RECLAIM_INTERVAL_MS)

  let hasConnectedOnce = false
  const client: RedisClientType = createClient({
    url: REDIS_URL,
    socket: {
      reconnectStrategy: (retries) => {
        if (!hasConnectedOnce && retries > 10) {
          return new Error('Redis initial connection retries exhausted')
        }
        return Math.min(retries * 100, 2000)
      }
    }
  })

  client.on('ready', () => {
    hasConnectedOnce = true
    fastify.log.info('Redis client ready')
  })

  client.on('error', (err) => {
    fastify.log.error({ err }, 'Redis client error, reconnecting')
  })

  await client.connect()
  fastify.decorate('redis', client)

  try {
    await client.xGroupCreate(REDIS_STREAM_KEY, REDIS_CONSUMER_GROUP, '0', { MKSTREAM: true })
    fastify.log.info({ group: REDIS_CONSUMER_GROUP }, 'Created consumer group')
  } catch (err) {
    if (!(err instanceof Error && err.message.includes('BUSYGROUP'))) throw err
  }

  let running = true
  let loop: Promise<void> = Promise.resolve()

  async function count (key: string, event: SinkEvent): Promise<void> {
    try {
      await client.incr(key)
    } catch (err) {
      fastify.log.warn({ err, key, eventId: event.event_id }, 'Failed to increment counter')
    }
  }

  async function deliver (id: string, message: Record<string, string>): Promise<void> {
    let event: SinkEvent
    try {
      event = toSinkEvent(message)
    } catch (err) {
      fastify.log.error({ err, id }, 'Dropping unparseable stream entry')
      await client.xAck(REDIS_STREAM_KEY, REDIS_CONSUMER_GROUP, id)
      return
    }

    let delivered = false
    let attempts = 0
 
    let reason = 'exhausted'

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        if (!running) {
          reason = 'shutdown'
          break
        }
        await sleep(backoffDelay(attempt))
        if (!running) {
          reason = 'shutdown'
          break
        }
        await count(REDIS_RETRIES_KEY, event)
      }

      attempts++
      try {
        delivered = await fastify.sinkClient.send(event)
      } catch (err) {
        fastify.log.warn({ err, id, attempt }, 'Sink request failed')
      }
      if (delivered) {
        await count(REDIS_DELIVERED_KEY, event)
        break
      }
    }

    if (!delivered) {
      await client.xAdd(REDIS_DLQ_KEY, '*', {
        ...message,
        failed_at: String(Date.now()),
        source_id: id,
        reason
      })
      fastify.log.error(
        { id, eventId: event.event_id, dlq: REDIS_DLQ_KEY, attempts, reason },
        'Sink delivery failed, sent to DLQ'
      )
    }
    await client.xAck(REDIS_STREAM_KEY, REDIS_CONSUMER_GROUP, id)
  }

  /**
   * Claims PEL entries idle longer than RECLAIM_MIN_IDLE_MS — ours from before
   * a crash, or another instance's if it never came back — and redelivers them.
   */
  async function reclaim (): Promise<void> {
    let cursor = '0-0'
    let claimed = 0

    do {
      const res = await client.xAutoClaim(
        REDIS_STREAM_KEY,
        REDIS_CONSUMER_GROUP,
        REDIS_CONSUMER_NAME,
        RECLAIM_MIN_IDLE_MS,
        cursor,
        { COUNT: READ_COUNT }
      )
      cursor = res.nextId.toString()

      const entries = res.messages.filter((m): m is StreamEntry => m !== null)
      claimed += entries.length

      await Promise.all(
        entries.map(async (entry) => {
          await deliver(entry.id.toString(), entry.message)
        })
      )
    } while (cursor !== '0-0' && running)

    if (claimed > 0) {
      fastify.log.warn({ claimed, minIdleMs: RECLAIM_MIN_IDLE_MS }, 'Reclaimed stale pending entries')
    }
  }

  async function readBatch (): Promise<void> {
    const streams = await client.xReadGroup(
      REDIS_CONSUMER_GROUP,
      REDIS_CONSUMER_NAME,
      { key: REDIS_STREAM_KEY, id: '>' },
      { COUNT: READ_COUNT, BLOCK: READ_BLOCK_MS }
    )

    if (streams === null) return

    for (const stream of streams) {
      // Did not have the time to implement but we should use a workek pool to limit the number instead of wait on all, what is punishng the throughput of the consumer.
      await Promise.all(
        stream.messages.map(async (entry: StreamEntry) => {
          await deliver(entry.id.toString(), entry.message)
        })
      )
    }
  }

  async function run (): Promise<void> {
    // reclaim on start to pick up any entries left in the PEL by a previous crash, then loop
    let nextReclaimAt = 0

    while (running) {
      try {
        if (Date.now() >= nextReclaimAt) {
          await reclaim()
          nextReclaimAt = Date.now() + RECLAIM_INTERVAL_MS
        }
        await readBatch()
      } catch (err) {
        if (!running) return
        fastify.log.error({ err }, 'Consumer read loop error, retrying')
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    }
  }

  fastify.addHook('onReady', async () => {
    loop = run()
    fastify.log.info({
      stream: REDIS_STREAM_KEY,
      group: REDIS_CONSUMER_GROUP,
      consumer: REDIS_CONSUMER_NAME
    }, 'Redis stream consumer started')
  })

  fastify.addHook('onClose', async () => {
    running = false
    await loop
    client.destroy()
  })
}

export default fp(redisConsumerPlugin)
