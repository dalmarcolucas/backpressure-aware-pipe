import fp from 'fastify-plugin'
import rateLimit from '@fastify/rate-limit'
import { FastifyPluginAsync } from 'fastify'

const rateLimitPlugin: FastifyPluginAsync = async (fastify) => {
  void fastify.register(rateLimit, {
    max: 200,
    timeWindow: '1 second',
    keyGenerator: () => 'global'
  })
}

export default fp(rateLimitPlugin)
