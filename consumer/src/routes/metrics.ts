import { type FastifyPluginAsync } from 'fastify'

const metrics: FastifyPluginAsync = async (fastify, opts): Promise<void> => {
  fastify.get('/metrics', async function (request, reply) {
    return { metrics: true }
  })
}

export default metrics
