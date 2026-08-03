import fp from 'fastify-plugin'
import { FastifyPluginAsync } from 'fastify'

declare module 'fastify' {
  interface FastifyInstance {
    someSupport(): string
  }
}

// the use of fastify-plugin is required to be able
// to export the decorators to the outer scope
const supportPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.decorate('someSupport', function () {
    return 'hugs'
  })
}

export default fp(supportPlugin)
