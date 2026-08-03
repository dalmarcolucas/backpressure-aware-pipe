import { type FastifyPluginAsync } from 'fastify'

const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY ?? 'events'
const REDIS_CONSUMER_GROUP = process.env.REDIS_CONSUMER_GROUP ?? 'event_consumers'
const REDIS_DLQ_KEY = process.env.REDIS_DLQ_KEY ?? `${REDIS_STREAM_KEY}:dlq`
const REDIS_DELIVERED_KEY = process.env.REDIS_DELIVERED_KEY ?? 'delivered'
const REDIS_RETRIES_KEY = process.env.REDIS_RETRIES_KEY ?? 'retries'

export interface Stats {
  produced: number
  backlog: number
  in_flight: number
  delivered: number
  retried: number
  dead_lettered: number
  oldest_event_age_ms: number
}

/**
 * Stream ids are `<ms>-<seq>`, so the id alone dates the entry: no need to read
 * the entry body to age it. Returns 0 for an id we cannot parse.
 */
function ageMs (id: string | null | undefined, now: number): number {
  if (id === null || id === undefined) return 0
  const ms = Number(id.split('-')[0])
  if (!Number.isFinite(ms)) return 0
  return Math.max(0, now - ms)
}

function toCount (value: string | null): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

/**
 * A missing stream or consumer group is the normal cold-start state (nothing
 * produced, or the consumer has not booted yet), not an error: report zeros.
 */
function isMissingKeyOrGroup (err: unknown): boolean {
  if (!(err instanceof Error)) return false
  return err.message.includes('no such key') || err.message.includes('NOGROUP')
}

const stats: FastifyPluginAsync = async (fastify): Promise<void> => {
  const { redis } = fastify

  /**
   * Age of the oldest event the sink has not accepted yet, whichever is older
   * of: the oldest entry a consumer holds unacked, and the oldest entry no
   * consumer has read at all.
   */
  async function oldestEventAgeMs (lastDeliveredId: string, oldestPendingId: string | null): Promise<number> {
    const now = Date.now()

    // Exclusive range start, so this is the first entry past what the group has
    // handed out: the oldest event still waiting to be read.
    const undelivered = await redis.xRange(REDIS_STREAM_KEY, `(${lastDeliveredId}`, '+', { COUNT: 1 })
    const undeliveredAge = ageMs(undelivered[0]?.id, now)

    return Math.max(undeliveredAge, ageMs(oldestPendingId, now))
  }

  async function readStreamStats (): Promise<Pick<Stats, 'backlog' | 'in_flight' | 'oldest_event_age_ms'>> {
    const groups = await redis.xInfoGroups(REDIS_STREAM_KEY)
    const group = groups.find((g) => g.name === REDIS_CONSUMER_GROUP)

    if (group === undefined) {
      // No consumer group yet: every entry in the stream is still waiting.
      const backlog = await redis.xLen(REDIS_STREAM_KEY)
      const oldest = await redis.xRange(REDIS_STREAM_KEY, '-', '+', { COUNT: 1 })
      return { backlog, in_flight: 0, oldest_event_age_ms: ageMs(oldest[0]?.id, Date.now()) }
    }

    // lag counts entries never handed to a consumer; pending counts entries a
    // consumer read but has not acked, which is exactly what is in flight to
    // the sink right now.
    const inFlight = group.pending
    const pendingSummary = inFlight > 0 ? await redis.xPending(REDIS_STREAM_KEY, REDIS_CONSUMER_GROUP) : null

    return {
      backlog: group.lag ?? 0,
      in_flight: inFlight,
      oldest_event_age_ms: await oldestEventAgeMs(
        String(group['last-delivered-id']),
        pendingSummary?.firstId ?? null
      )
    }
  }

  fastify.get('/stats', async function (): Promise<Stats> {
    let streamStats: Pick<Stats, 'backlog' | 'in_flight' | 'oldest_event_age_ms'> = {
      backlog: 0,
      in_flight: 0,
      oldest_event_age_ms: 0
    }

    try {
      streamStats = await readStreamStats()
    } catch (err) {
      if (!isMissingKeyOrGroup(err)) throw err
    }

    const [produced, delivered, retried, deadLettered] = await Promise.all([
      // Everything ever added to the stream. XLEN on a missing key is 0, which
      // is the right answer before the producer has written anything.
      redis.xLen(REDIS_STREAM_KEY),
      redis.get(REDIS_DELIVERED_KEY),
      redis.get(REDIS_RETRIES_KEY),
      redis.exists(REDIS_DLQ_KEY).then(async (exists) =>
        exists === 1 ? await redis.xLen(REDIS_DLQ_KEY) : 0
      )
    ])

    return {
      ...streamStats,
      produced,
      delivered: toCount(delivered),
      retried: toCount(retried),
      dead_lettered: deadLettered
    }
  })
}

export default stats
