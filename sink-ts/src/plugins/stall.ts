import fp from 'fastify-plugin'
import { FastifyPluginAsync } from 'fastify'

const stallPlugin: FastifyPluginAsync = async (fastify) => {
  let shouldStall = false

  const interval = setInterval(async () => {
    shouldStall = true
    await new Promise((resolve) => setTimeout(resolve, 3000))
    shouldStall = false
  }, 30000)

  fastify.addHook('onRequest', async (_request, _) => {
    if (shouldStall) {
      await new Promise((resolve) => setTimeout(resolve, 3000))
    }
  })

  fastify.addHook('onClose', async () => {
    clearInterval(interval)
  })
}

export default fp(stallPlugin)
