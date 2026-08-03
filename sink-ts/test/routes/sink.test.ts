import { test } from 'node:test'
import assert from 'node:assert'

import Fastify, { FastifyInstance, InjectOptions } from 'fastify'
import Sink from '../../src/routes/sink'
import RateLimit from '../../src/plugins/rate-limit'


const MIN_DELAY = 50
const MAX_DELAY = 300
// setTimeout/event-loop scheduling is never early but can fire a bit late,
// especially under CI load, so only the upper bound needs slack.
const JITTER_TOLERANCE = 75

const validEvent = {
  event_id: 'evt_000123',
  created_at: 1767185000123,
  payload: { user: 'u42', action: 'click' }
}

function buildApp (): FastifyInstance {
  const fastify = Fastify()
  fastify.register(Sink)
  return fastify
}

function post (fastify: FastifyInstance, payload: InjectOptions['payload']) {
  return fastify.inject({ method: 'POST', url: '/sink', payload })
}

function countByStatus (responses: Array<{ statusCode: number }>) {
  const counts: Record<number, number> = {}
  for (const res of responses) counts[res.statusCode] = (counts[res.statusCode] || 0) + 1
  return counts
}

test('POST /sink accepts a well-formed event, allows extra fields, and delays the response between 50-300ms', async (t) => {
  const fastify = buildApp()
  t.after(() => fastify.close())

  for (let i = 0; i < 8; i++) {
    const start = Date.now()
    const res = await post(fastify, { ...validEvent, extra: 'unexpected' })
    const elapsed = Date.now() - start

    assert.equal(res.statusCode, 200)
    assert.ok(elapsed >= MIN_DELAY, `expected delay >= ${MIN_DELAY}ms, got ${elapsed}ms`)
    assert.ok(
      elapsed <= MAX_DELAY + JITTER_TOLERANCE,
      `expected delay <= ${MAX_DELAY}ms (+${JITTER_TOLERANCE}ms jitter), got ${elapsed}ms`
    )
  }
})

test('POST /sink rejects a payload missing a required field', async (t) => {
  const fastify = buildApp()
  t.after(() => fastify.close())

  const missingBody = await fastify.inject({ method: 'POST', url: '/sink' })
  assert.equal(missingBody.statusCode, 400)

  for (const field of ['event_id', 'created_at', 'payload'] as const) {
    const { [field]: _omitted, ...incomplete } = validEvent
    const res = await post(fastify, incomplete)
    assert.equal(res.statusCode, 400, `expected 400 when "${field}" is missing`)
  }
})

test('POST /sink rejects a payload with malformed field types', async (t) => {
  const fastify = buildApp()
  t.after(() => fastify.close())

  const badCreatedAt = await post(fastify, { ...validEvent, created_at: 'not-a-timestamp' })
  assert.equal(badCreatedAt.statusCode, 400)

  const badPayload = await post(fastify, { ...validEvent, payload: 'not-an-object' })
  assert.equal(badPayload.statusCode, 400)
})

test('POST /sink is rate limited past 200 requests/second, shared across all clients', async (t) => {
  const fastify = Fastify()
  fastify.register(RateLimit)
  fastify.register(Sink)
  t.after(() => fastify.close())

  // Spread requests across many different IPs to prove the quota is global,
  // not bucketed per client.
  const inject = (ip: string) => fastify.inject({ method: 'POST', url: '/sink', payload: validEvent, remoteAddress: ip })

  // Exhaust the limit in one burst, then await it fully before firing more:
  // the in-memory store only settles the count once this batch resolves, so
  // splitting into two awaited batches keeps the outcome deterministic.
  const withinLimit = await Promise.all(
    Array.from({ length: 200 }, (_, i) => inject(`10.0.0.${i % 255}`))
  )
  assert.deepStrictEqual(countByStatus(withinLimit), { 200: 200 })

  // A client IP that has never made a request before should still be blocked,
  // since the quota is shared rather than per-IP.
  const freshClient = await inject('192.168.1.1')
  assert.equal(freshClient.statusCode, 429)
  assert.ok(freshClient.headers['retry-after'])
})

