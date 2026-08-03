import { test } from 'node:test'
import assert from 'node:assert'

import Fastify from 'fastify'
import FailRate from '../../src/plugins/fail-rate'

function buildApp () {
  const fastify = Fastify()
  fastify.register(FailRate)
  fastify.get('/anything', async () => ({ ok: true }))
  fastify.post('/anything', async () => ({ ok: true }))
  return fastify
}

test('fail-rate hook returns a 500 on the unlucky draw, for any route', async (t) => {
  const fastify = buildApp()
  t.after(() => fastify.close())

  t.mock.method(Math, 'random', () => 0.05)
  const res = await fastify.inject({ method: 'GET', url: '/anything' })

  assert.equal(res.statusCode, 500)
  assert.deepStrictEqual(res.json(), { error: 'Internal Server Error' })
})

test('fail-rate hook lets requests through on the lucky draw', async (t) => {
  const fastify = buildApp()
  t.after(() => fastify.close())

  t.mock.method(Math, 'random', () => 0.5)
  const res = await fastify.inject({ method: 'POST', url: '/anything' })

  assert.equal(res.statusCode, 200)
  assert.deepStrictEqual(res.json(), { ok: true })
})

test('fail-rate hook applies to every route, not just /sink', async (t) => {
  const fastify = buildApp()
  fastify.get('/other', async () => ({ ok: true }))
  t.after(() => fastify.close())

  t.mock.method(Math, 'random', () => 0.0)
  const res = await fastify.inject({ method: 'GET', url: '/other' })

  assert.equal(res.statusCode, 500)
})
