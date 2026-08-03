import { test } from 'node:test'
import * as assert from 'node:assert'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { randomUUID } from 'node:crypto'
import Fastify from 'fastify'
import { createClient, type RedisClientType } from 'redis'
import sinkClient from '../../src/plugins/sink-client'
import redisConsumer from '../../src/plugins/redis-consumer'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'

interface Received {
  event_id: string
  created_at: number
  payload: Record<string, unknown>
}

function restoreEnv (key: string, value: string | undefined): void {
  if (value === undefined) delete process.env[key]
  else process.env[key] = value
}

/**
 * Runs the consumer against a real Redis stream and a stub sink that records
 * every event it receives. `failFirst` makes the stub reject the first N calls
 * with a 500, mirroring the real sink's random failures; `failAlways` makes it
 * reject every call, so retries are always exhausted.
 */
async function buildRig (
  t: { after: (fn: () => void | Promise<void>) => void },
  { failFirst = 0, failAlways = false, reclaimMinIdleMs, reclaimIntervalMs }: {
    failFirst?: number
    failAlways?: boolean
    reclaimMinIdleMs?: number
    reclaimIntervalMs?: number
  } = {}
) {
  const received: Received[] = []
  const callTimes: number[] = []
  let calls = 0

  const sink = createServer((req, res) => {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', () => {
      calls++
      callTimes.push(Date.now())
      if (failAlways || calls <= failFirst) {
        res.writeHead(500)
        res.end()
        return
      }
      received.push(JSON.parse(body))
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ success: true }))
    })
  })
  await new Promise<void>((resolve) => sink.listen(0, '127.0.0.1', resolve))
  const { port } = sink.address() as AddressInfo

  const stream = `test:events:${randomUUID()}`
  const group = `test_group_${randomUUID().slice(0, 8)}`
  const dlq = `${stream}:dlq`

  const seeder: RedisClientType = createClient({ url: REDIS_URL })
  await seeder.connect()

  const previous = { ...process.env }
  process.env.SINK_URL = `http://127.0.0.1:${port}`
  process.env.REDIS_STREAM_KEY = stream
  process.env.REDIS_CONSUMER_GROUP = group
  process.env.REDIS_DLQ_KEY = dlq
  if (reclaimMinIdleMs !== undefined) process.env.REDIS_RECLAIM_MIN_IDLE_MS = String(reclaimMinIdleMs)
  if (reclaimIntervalMs !== undefined) process.env.REDIS_RECLAIM_INTERVAL_MS = String(reclaimIntervalMs)

  const app = Fastify()
  await app.register(sinkClient)
  await app.register(redisConsumer)

  process.env.SINK_URL = previous.SINK_URL
  process.env.REDIS_STREAM_KEY = previous.REDIS_STREAM_KEY
  process.env.REDIS_CONSUMER_GROUP = previous.REDIS_CONSUMER_GROUP
  process.env.REDIS_DLQ_KEY = previous.REDIS_DLQ_KEY
  // Assigning `undefined` would leave the literal string "undefined" behind,
  // which the plugin would read as NaN in the tests that pass no override.
  restoreEnv('REDIS_RECLAIM_MIN_IDLE_MS', previous.REDIS_RECLAIM_MIN_IDLE_MS)
  restoreEnv('REDIS_RECLAIM_INTERVAL_MS', previous.REDIS_RECLAIM_INTERVAL_MS)

  t.after(async () => {
    await app.close()
    await seeder.del([stream, dlq])
    await seeder.close()
    await new Promise<void>((resolve) => sink.close(() => resolve()))
  })

  return { app, seeder, stream, group, dlq, received, callTimes, sinkCalls: () => calls }
}

/** Polls until `predicate` holds or the deadline passes. */
async function waitFor (predicate: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('timed out waiting for condition')
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

test('reads stream entries and forwards the payload to the sink', async (t) => {
  const { app, seeder, stream, group, received } = await buildRig(t)

  for (let i = 1; i <= 3; i++) {
    await seeder.xAdd(stream, '*', {
      event_id: `evt_00000${i}`,
      created_at: String(1767185000000 + i),
      payload: JSON.stringify({ user: `u${i}`, action: 'click' })
    })
  }

  await app.ready()
  await waitFor(() => received.length === 3)

  assert.deepStrictEqual(received.map((e) => e.event_id), ['evt_000001', 'evt_000002', 'evt_000003'])
  // created_at comes back as a number and payload as a parsed object, not the
  // strings Redis Streams actually store.
  assert.strictEqual(received[0].created_at, 1767185000001)
  assert.deepStrictEqual(received[0].payload, { user: 'u1', action: 'click' })

  // Everything delivered was acked, so nothing is left pending.
  await waitFor(async () => (await seeder.xPending(stream, group)).pending === 0)
})

test('retries a transient sink failure and still delivers the event', async (t) => {
  const { app, seeder, stream, group, dlq, received } = await buildRig(t, { failFirst: 2 })

  await seeder.xAdd(stream, '*', {
    event_id: 'evt_000009',
    created_at: String(1767185000009),
    payload: JSON.stringify({ user: 'u9', action: 'view' })
  })

  await app.ready()
  await waitFor(() => received.length === 1)

  assert.strictEqual(received[0].event_id, 'evt_000009')
  // Delivered on retry, so it is acked and never reaches the DLQ.
  await waitFor(async () => (await seeder.xPending(stream, group)).pending === 0)
  assert.strictEqual(await seeder.exists(dlq), 0)
})

test('sends the event to the DLQ once retries are exhausted', async (t) => {
  const { app, seeder, stream, group, dlq, sinkCalls, callTimes } = await buildRig(t, { failAlways: true })

  await seeder.xAdd(stream, '*', {
    event_id: 'evt_000011',
    created_at: String(1767185000011),
    payload: JSON.stringify({ user: 'u11', action: 'purchase' })
  })

  await app.ready()
  await waitFor(async () => await seeder.xLen(dlq) === 1, 15000)

  const [entry] = await seeder.xRange(dlq, '-', '+')
  assert.strictEqual(entry.message.event_id, 'evt_000011')
  assert.deepStrictEqual(JSON.parse(entry.message.payload), { user: 'u11', action: 'purchase' })
  // The original stream id and a failure timestamp are kept for replay.
  assert.ok(entry.message.source_id.length > 0)
  assert.ok(Number(entry.message.failed_at) > 0)

  assert.strictEqual(entry.message.reason, 'exhausted')

  // One initial attempt plus five retries.
  assert.strictEqual(sinkCalls(), 6)

  // Gaps grow 100, 200, 400, 800, 1600ms, each jittered by a random 1-2x.
  // Only the lower bound is asserted: jitter can only push a gap higher, and a
  // strict upper bound would make this flaky on a loaded machine.
  for (let i = 1; i < callTimes.length; i++) {
    const gap = callTimes[i] - callTimes[i - 1]
    const expected = 100 * Math.pow(2, i - 1)
    assert.ok(gap >= expected * 0.8, `gap ${i} was ${gap}ms, expected at least ~${expected}ms`)
  }

  // DLQ'd entries are acked so they are not redelivered.
  await waitFor(async () => (await seeder.xPending(stream, group)).pending === 0)
})

test('stops retrying on shutdown and sends the event straight to the DLQ', async (t) => {
  const { app, seeder, stream, dlq, sinkCalls } = await buildRig(t, { failAlways: true })

  await seeder.xAdd(stream, '*', {
    event_id: 'evt_000012',
    created_at: String(1767185000012),
    payload: JSON.stringify({ user: 'u12', action: 'signup' })
  })

  await app.ready()
  // Close mid-backoff, well before the six attempts could run.
  await waitFor(() => sinkCalls() >= 1)
  await app.close()

  assert.ok(sinkCalls() < 6, `expected retries to be cut short, saw ${sinkCalls()} calls`)

  const [entry] = await seeder.xRange(dlq, '-', '+')
  assert.strictEqual(entry.message.event_id, 'evt_000012')
  assert.strictEqual(entry.message.reason, 'shutdown')
})

test('reclaims entries a dead consumer left unacked in the PEL', async (t) => {
  const { app, seeder, stream, group, received } = await buildRig(t, {
    reclaimMinIdleMs: 100,
    reclaimIntervalMs: 100
  })

  await seeder.xAdd(stream, '*', {
    event_id: 'evt_000021',
    created_at: String(1767185000021),
    payload: JSON.stringify({ user: 'u21', action: 'refund' })
  })

  // Stand in for an instance killed with -9 mid-delivery: it reads the entry
  // into the PEL under its own name and never acks.
  const dead = await seeder.xReadGroup(group, 'dead-consumer', { key: stream, id: '>' }, { COUNT: 10 })
  assert.strictEqual(dead?.[0].messages.length, 1)
  assert.strictEqual((await seeder.xPending(stream, group)).pending, 1)

  // The sink has seen nothing: '>' will never serve this entry again, so
  // without XAUTOCLAIM the event is simply lost.
  await app.ready()
  await waitFor(() => received.length === 1, 10000)

  assert.strictEqual(received[0].event_id, 'evt_000021')
  await waitFor(async () => (await seeder.xPending(stream, group)).pending === 0)
})

test('leaves entries that are still being worked on alone', async (t) => {
  const { app, seeder, stream, group, received } = await buildRig(t, {
    reclaimMinIdleMs: 60_000,
    reclaimIntervalMs: 100
  })

  await seeder.xAdd(stream, '*', {
    event_id: 'evt_000022',
    created_at: String(1767185000022),
    payload: JSON.stringify({ user: 'u22', action: 'view' })
  })
  await seeder.xReadGroup(group, 'busy-consumer', { key: stream, id: '>' }, { COUNT: 10 })

  await app.ready()
  await new Promise((resolve) => setTimeout(resolve, 1000))

  // Idle time is far below the threshold, so the entry stays with its owner
  // rather than being stolen and delivered twice.
  assert.strictEqual(received.length, 0)
  assert.strictEqual((await seeder.xPending(stream, group)).pending, 1)

  // Cleanup: the app would otherwise never ack this entry.
  await seeder.xAck(stream, group, (await seeder.xPending(stream, group)).firstId as string)
})

test('creates the consumer group when the stream does not exist yet', async (t) => {
  const { app, seeder, stream, group } = await buildRig(t)

  await app.ready()

  const groups = await seeder.xInfoGroups(stream)
  assert.ok(groups.some((g) => g.name === group), 'expected the consumer group to exist')
})
