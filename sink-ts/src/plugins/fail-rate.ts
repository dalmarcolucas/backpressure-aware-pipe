import fp from 'fastify-plugin'
import { FastifyPluginAsync } from 'fastify'

const FAIL_RATE = 0.1

const failRatePlugin: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('onRequest', async (_request, reply) => {
    if (Math.random() < FAIL_RATE) {
      reply.code(500).send({ error: 'Internal Server Error' })
    }
  })
}

export default fp(failRatePlugin)
