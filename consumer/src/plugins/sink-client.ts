import fp from 'fastify-plugin'
import { FastifyPluginAsync } from 'fastify'

export interface SinkEvent {
  event_id: string
  created_at: number
  payload: Record<string, unknown>
}

export interface SinkClient {
  /**
   * POSTs one event to the sink, once. Resolves true if the sink accepted it,
   * false on a rejection or network failure. Retrying is the caller's job.
   */
  send: (event: SinkEvent) => Promise<boolean>
}

declare module 'fastify' {
  interface FastifyInstance {
    sinkClient: SinkClient
  }
}

const sinkClientPlugin: FastifyPluginAsync = async (fastify) => {
  const SINK_URL = process.env.SINK_URL ?? 'http://localhost:3001'

  async function send (event: SinkEvent): Promise<boolean> {
    try {
      const res = await fetch(`${SINK_URL}/sink`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(event)
      })

      if (res.ok) return true

      fastify.log.warn({ eventId: event.event_id, status: res.status }, 'Sink call failed')
    } catch (err) {
      // Network-level failure (sink down, connection reset): treat as transient.
      fastify.log.warn({ err, eventId: event.event_id }, 'Sink call errored')
    }
    return false
  }

  fastify.decorate('sinkClient', { send })
}

export default fp(sinkClientPlugin)
