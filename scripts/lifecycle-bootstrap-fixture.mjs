// Loaded only by isolated tests through Node's import hook, never production.
import { registerHooks } from 'node:module'
const mode = process.env.QWD_BOOTSTRAP_TEST
registerHooks({
  load (url, context, nextLoad) {
    if (url.endsWith('/manager-host/ManagerGateway.js')) {
      const start = mode === 'listen' ? "throw new Error('fixture listen failure')" : mode === 'listen-hang' ? 'await new Promise(()=>{})' : ''
      return { format: 'module', shortCircuit: true, source: `export class ManagerGateway { constructor(){ ${mode === 'constructor' ? "throw new Error('fixture constructor failure')" : ''} } async start(){${start}} async stop(){} }` }
    }
    if (url.endsWith('/manager-host/ManagerRuntime.js')) {
      if (mode === 'import') throw new Error('fixture import failure')
      return { format: 'module', shortCircuit: true, source: `export const createManagerRuntime=()=>{ ${mode === 'runtime' ? "throw new Error('fixture runtime failure');" : ''} return {pending:{close(){}},reservations:{stop(){}},moves:{async stop(){}}} }` }
    }
    return nextLoad(url, context)
  }
})
