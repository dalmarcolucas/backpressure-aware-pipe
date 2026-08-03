import fp from 'fastify-plugin'
import { createClient, type RedisClientType } from 'redis'
import { FastifyPluginAsync } from 'fastify'

declare module 'fastify' {
  interface FastifyInstance {
    redis: RedisClientType
  }
}

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'
const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY ?? 'events'
const REDIS_COUNTER_KEY = `${REDIS_STREAM_KEY}:event_id_counter`
const REDIS_CONSUMER_GROUP = process.env.REDIS_CONSUMER_GROUP ?? 'event_consumers'

const EVENTS_PER_SEC = 2000
const BURST_DURATION_MS = 5000
const IDLE_DURATION_MS = 1000

// burst rate is approximated as 100 events fired every 50ms tick.
const PRODUCE_TICK_MS = 50
const EVENTS_PER_TICK = 100

const BACKLOG_HIGH_LIMIT = 6000
const BACKLOG_LOW_LIMIT = 3000
const BACKPRESSURE_CHECK_MS = 250

const USERS = Array.from({ length: 50 }, (_, i) => `u${i + 1}`)
const ACTIONS = ['click', 'view', 'purchase']

function randomPayload (): { user: string, action: string } {
  return {
    user: USERS[Math.floor(Math.random() * USERS.length)],
    action: ACTIONS[Math.floor(Math.random() * ACTIONS.length)]
  }
}

const redisProducerPlugin: FastifyPluginAsync = async (fastify) => {
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

  async function produceEvent (): Promise<void> {
    try {
      // Sequence lives in Redis (INCR), not process memory, so a restart
      // continues the id sequence instead of resetting to evt_000001.
      const seq = await client.incr(REDIS_COUNTER_KEY)
      const eventId = `evt_${String(seq).padStart(6, '0')}`

      await client.xAdd(REDIS_STREAM_KEY, '*', {
        event_id: eventId,
        created_at: String(Date.now()),
        payload: JSON.stringify(randomPayload())
      })
    } catch (err) {
      fastify.log.error({ err }, 'Failed to produce event to Redis stream')
    }
  }

  let throttled = false

  async function checkBackpressure (): Promise<void> {
    try {
      const [xlen, groups] = await Promise.all([
        client.xLen(REDIS_STREAM_KEY),
        client.xInfoGroups(REDIS_STREAM_KEY)
      ])

      let backlog = xlen

      // If a consumer group exists, use its lag + pending count as the backlog instead of the raw stream length.
      const group = groups.find((g) => g.name === REDIS_CONSUMER_GROUP)
      if (group !== undefined) {
        backlog = group.lag + group.pending
      }

      if (!throttled &&backlog >= BACKLOG_HIGH_LIMIT) {
        throttled = true
        fastify.log.warn({ backlog }, 'Backpressure: pausing production')
      } else if (throttled && backlog <= BACKLOG_LOW_LIMIT) {
        throttled = false
        fastify.log.info({ backlog }, 'Backpressure: resuming production')
      }
    } catch (err) {
      // Stream key does not exist yet: nothing has been produced, no backlog.
      if (err instanceof Error && err.message.includes('no such key')) {
        throttled = false
        return
      }
      fastify.log.error({ err }, 'Failed to read backpressure signal')
    }
  }

  let burstActive = false

  const cycleInterval = setInterval(async () => {
    burstActive = true
    fastify.log.info({ eventsPerSec: EVENTS_PER_SEC, durationMs: BURST_DURATION_MS }, 'Burst started')
    await new Promise((resolve) => setTimeout(resolve, BURST_DURATION_MS))
    burstActive = false
    fastify.log.info({ durationMs: IDLE_DURATION_MS }, 'Idle started')
  }, BURST_DURATION_MS + IDLE_DURATION_MS)

  const backpressureInterval = setInterval(() => {
    void checkBackpressure()
  }, BACKPRESSURE_CHECK_MS)

  const produceInterval = setInterval(() => {
    if (!burstActive || throttled) return
    for (let i = 0; i < EVENTS_PER_TICK; i++) {
      void produceEvent()
    }
  }, PRODUCE_TICK_MS)

  fastify.log.info({
    stream: REDIS_STREAM_KEY,
    eventsPerSec: EVENTS_PER_SEC,
    burstDurationMs: BURST_DURATION_MS,
    idleDurationMs: IDLE_DURATION_MS
  }, 'Redis stream producer started')

  fastify.addHook('onClose', async () => {
    clearInterval(cycleInterval)
    clearInterval(produceInterval)
    clearInterval(backpressureInterval)
    await client.close()
  })
}

export default fp(redisProducerPlugin)
