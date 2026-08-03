import { test } from 'node:test'
import * as assert from 'node:assert'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import Fastify from 'fastify'
import sinkClient, { type SinkEvent } from '../../src/plugins/sink-client'

type Handler = (req: IncomingMessage, res: ServerResponse) => void

const event: SinkEvent = {
  event_id: 'evt_000001',
  created_at: 1767185000123,
  payload: { user: 'u42', action: 'click' }
}

/**
 * Boots a stub sink on an ephemeral port, points SINK_URL at it, and builds an
 * app with only the sink-client plugin registered.
 */
async function buildWithStub (t: { after: (fn: () => void | Promise<void>) => void }, handler: Handler) {
  const server = createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo

  const previousUrl = process.env.SINK_URL
  process.env.SINK_URL = `http://127.0.0.1:${port}`

  const app = Fastify()
  await app.register(sinkClient)

  process.env.SINK_URL = previousUrl

  t.after(async () => {
    await app.close()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  })

  return app
}

test('posts the event to /sink and returns true on 2xx', async (t) => {
  let seen: { url?: string, method?: string, body: string } = { body: '' }

  const app = await buildWithStub(t, (req, res) => {
    let body = ''
    req.on('data', (chunk) => { body += chunk })
    req.on('end', () => {
      seen = { url: req.url, method: req.method, body }
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ success: true }))
    })
  })

  assert.strictEqual(await app.sinkClient.send(event), true)
  assert.strictEqual(seen.url, '/sink')
  assert.strictEqual(seen.method, 'POST')
  assert.deepStrictEqual(JSON.parse(seen.body), event)
})

// Retrying is the consumer's job now, so every failure mode below is a single
// call that returns false. The retry/backoff behaviour is covered in
// test/plugins/redis-consumer.test.ts.

test('returns false on a 500 without retrying', async (t) => {
  let calls = 0

  const app = await buildWithStub(t, (_req, res) => {
    calls++
    res.writeHead(500, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ error: 'Internal Server Error' }))
  })

  assert.strictEqual(await app.sinkClient.send(event), false)
  assert.strictEqual(calls, 1)
})

test('returns false on a 4xx the sink will never accept', async (t) => {
  let calls = 0

  const app = await buildWithStub(t, (_req, res) => {
    calls++
    res.writeHead(400, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ error: 'Bad Request' }))
  })

  assert.strictEqual(await app.sinkClient.send(event), false)
  assert.strictEqual(calls, 1)
})

test('returns false on a 429 rate-limit response', async (t) => {
  let calls = 0

  const app = await buildWithStub(t, (_req, res) => {
    calls++
    res.writeHead(429)
    res.end()
  })

  assert.strictEqual(await app.sinkClient.send(event), false)
  assert.strictEqual(calls, 1)
})

test('returns false when the sink is unreachable', async (t) => {
  const app = await buildWithStub(t, (_req, res) => {
    res.destroy()
  })

  assert.strictEqual(await app.sinkClient.send(event), false)
})
