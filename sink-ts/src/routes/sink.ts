import { FastifyPluginAsync } from 'fastify'

const eventSchema = {
  type: 'object',
  required: ['event_id', 'created_at', 'payload'],
  properties: {
    event_id: { type: 'string', minLength: 1 },
    created_at: { type: 'integer', minimum: 0 },
    payload: { type: 'object' }
  }
} as const

interface SinkBody {
  event_id: string
  created_at: number
  payload: Record<string, unknown>
}

const MIN_DELAY = 50
const MAX_DELAY = 300

const sink: FastifyPluginAsync = async (fastify) => {
  fastify.post<{ Body: SinkBody }>('/sink', {
    schema: {
      body: eventSchema
    }
  }, async (_request, _reply) => {
    const delay = Math.random() * (MAX_DELAY - MIN_DELAY) + MIN_DELAY
    await new Promise((resolve) => setTimeout(resolve, delay))

    return { success: true }
  })
}

export default sink
