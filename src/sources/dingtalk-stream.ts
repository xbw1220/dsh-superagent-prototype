/** DingTalk background loops: the Stream-mode robot-message consumer and its restart loop. */
import { threadKey, type Trigger } from '../core/index.ts'
import type { DingtalkBot } from '../deployment.ts'
import { dingtalkLink } from './dingtalk-channel.ts'
import { dingtalkTrigger, SOURCE } from './dingtalk-trigger.ts'
import { health, type SourceContext } from './host.ts'
import { KV } from '../kv.ts'
import { consumerRetry, pause } from './poll.ts'
import type { DingtalkConfig } from './dingtalk.ts'

const GATEWAY_URL = 'https://api.dingtalk.com/v1.0/gateway/connections/open'
/** The Stream topic whose callbacks carry the robot's received messages. */
export const TOPIC_ROBOT = '/v1.0/im/bot/messages/get'
/** Closes and reconnects a stream that carried no frame (not even the gateway's pings) for this long. */
const SILENT_MS = 180_000
interface StreamFrame { type?: string; headers?: { topic?: string; messageId?: string }; data?: string }

const enabled = (ctx: SourceContext): DingtalkConfig | undefined => ctx.config<DingtalkConfig>(SOURCE)
const bot = (ctx: SourceContext): DingtalkBot => {
  const resolved = ctx.deployment.dingtalk
  if (!resolved) throw new Error('The DingTalk Source has no working app credentials')
  return resolved
}
async function receive(ctx: SourceContext, trigger: Trigger): Promise<void> {
  await ctx.receive(trigger)
  const conversationId = String(trigger.meta.conversationId ?? '')
  if (!conversationId) return
  const title = typeof trigger.meta.conversationTitle === 'string' ? trigger.meta.conversationTitle : ''
  const labels = { ...(ctx.state.get(KV.sources.labels(SOURCE)) ?? {}) }
  if (title && labels[conversationId] !== title) { labels[conversationId] = title; ctx.state.set(KV.sources.labels(SOURCE), labels) }
  const name = labels[conversationId] ?? conversationId
  const conversation = trigger.meta.direct === true ? { value: { zh: '私聊', en: 'Direct message' } } : { value: name }
  const url = dingtalkLink(trigger.channel?.address ?? {}, {}).url
  for (const ref of trigger.threads) ctx.threads.describe(threadKey(ref), { ...(url ? { url } : {}), facts: { conversation } })
}
function available(ctx: SourceContext): boolean {
  if (ctx.configured() && ctx.deployment.dingtalk) return true
  const waiting = health('paused', ctx.configured() ? 'dingtalkCredsUnset' : 'setupPending')
  ctx.report('dingtalk', ctx.problem ? { state: 'paused', detail: ctx.problem.detail } : waiting)
  return false
}

/** Runs the Stream consumer only while the Source is enabled; disabling it stops the consumer within five seconds. */
export async function runDingtalk(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the app credentials; until then nothing runs.
  if (!available(ctx)) return
  let retry: number | undefined
  while (!signal.aborted) {
    if (!enabled(ctx)) {
      ctx.report('dingtalk', health('paused', 'sourceOff'))
      await pause(5000, signal)
      continue
    }
    const running = new AbortController()
    const check = setInterval(() => { if (!enabled(ctx)) running.abort() }, 5000)
    const stop = () => running.abort()
    signal.addEventListener('abort', stop, { once: true })
    const began = Date.now()
    try { await consume(ctx, running.signal) }
    catch (error) { if (!running.signal.aborted) ctx.report('dingtalk', { state: 'error', detail: String(error) }) }
    finally { clearInterval(check); signal.removeEventListener('abort', stop) }
    if (running.signal.aborted) continue
    retry = consumerRetry(retry, Date.now() - began)
    await pause(retry, signal)
  }
}

/** Asks the gateway for a Stream endpoint and opens the WebSocket the robot messages arrive on. */
export async function openStream(bot: Pick<DingtalkBot, 'clientId' | 'clientSecret'>, fetcher: typeof fetch = fetch): Promise<WebSocket> {
  const response = await fetcher(GATEWAY_URL, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ clientId: bot.clientId, clientSecret: bot.clientSecret, ua: 'dsh-superagent',
      subscriptions: [{ type: 'EVENT', topic: '*' }, { type: 'CALLBACK', topic: TOPIC_ROBOT }] }), signal: AbortSignal.timeout(30_000) })
  const text = await response.text()
  let data: { endpoint?: string; ticket?: string; message?: string } = {}
  try { data = JSON.parse(text) as typeof data } catch { /* Non-JSON diagnostics are kept below. */ }
  if (!response.ok || !data.endpoint || !data.ticket) {
    throw new Error(data.message || `The DingTalk Stream gateway refused the connection (HTTP ${response.status})`)
  }
  return new WebSocket(`${data.endpoint}?ticket=${data.ticket}`)
}

/** One Stream session: acknowledges and takes in robot messages until the socket closes or the Source stops. */
export async function consume(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  const ws = await openStream(bot(ctx))
  await new Promise<void>((resolve, reject) => {
    let settled = false
    let chain = Promise.resolve()
    let last = Date.now()
    const settle = (error?: Error): void => {
      if (settled) return
      settled = true
      clearInterval(watchdog)
      signal.removeEventListener('abort', stop)
      if (error) reject(error); else resolve()
    }
    const stop = (): void => { ws.close() }
    const watchdog = setInterval(() => {
      if (Date.now() - last > SILENT_MS) { ws.close(); settle(new Error('The DingTalk stream went silent')) }
    }, 30_000)
    signal.addEventListener('abort', stop, { once: true })
    ws.addEventListener('message', event => {
      last = Date.now()
      chain = chain.then(() => handle(ctx, ws, String(event.data)))
        .catch((error: unknown) => { ctx.report('dingtalk', { state: 'error', detail: String(error) }) })
    })
    ws.addEventListener('close', () => {
      chain.then(() => settle(signal.aborted ? undefined : new Error('The DingTalk stream closed')))
    })
    ws.addEventListener('error', () => { settle(new Error('The DingTalk stream failed')) })
    // A frame the gateway sends before the open event still counts as traffic; the close event settles the session.
    ws.addEventListener('open', () => { last = Date.now() })
  })
}

/** Handles one downstream frame: answers gateway pings, and acknowledges then takes in one robot message. */
async function handle(ctx: SourceContext, ws: WebSocket, raw: string): Promise<void> {
  const frame = JSON.parse(raw) as StreamFrame
  if (frame.type === 'SYSTEM') {
    const topic = frame.headers?.topic
    if (topic === 'ping') ws.send(JSON.stringify({ code: 200, headers: frame.headers, message: 'OK', data: frame.data }))
    else if (topic === 'REGISTERED') ctx.report('dingtalk', { state: 'ready', detail: 'Listening for bot messages', at: new Date().toISOString() })
    else if (topic === 'disconnect') ws.close()
    return
  }
  if (frame.type !== 'CALLBACK' || frame.headers?.topic !== TOPIC_ROBOT || !frame.headers.messageId) return
  // Acknowledge at once: DingTalk redelivers an unanswered message within about a minute, and the Trigger ID deduplicates it.
  ws.send(JSON.stringify({ code: 200, headers: { contentType: 'application/json', messageId: frame.headers.messageId }, message: 'OK',
    data: JSON.stringify({ response: { success: true } }) }))
  const trigger = frame.data ? dingtalkTrigger(JSON.parse(frame.data)) : undefined
  // Events already read when the Source is disabled are dropped; the consumer stops shortly after.
  if (!trigger || !enabled(ctx)) return
  await receive(ctx, trigger)
}
