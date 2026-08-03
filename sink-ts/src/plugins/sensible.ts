import fp from 'fastify-plugin'
import sensible from '@fastify/sensible'
import { FastifyPluginAsync } from 'fastify'

/**
 * This plugin adds some utilities to handle http errors
 *
 * @see https://github.com/fastify/fastify-sensible
 */
const sensiblePlugin: FastifyPluginAsync = async (fastify) => {
  void fastify.register(sensible)
}

export default fp(sensiblePlugin)
