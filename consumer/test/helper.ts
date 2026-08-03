import * as path from 'node:path'
import * as test from 'node:test'
const helper = require('fastify-cli/helper.js')

export type TestContext = {
  after: typeof test.after
}

const AppPath = path.join(__dirname, '..', 'src', 'app.ts')

function config () {
  return {
    skipOverride: true
  }
}

async function build (t: TestContext) {
  const argv = [AppPath]

  const app = await helper.build(argv, config())

  t.after(() => void app.close())

  return app
}

export {
  config,
  build
}
