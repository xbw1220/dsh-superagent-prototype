import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Engine, key, type Assessor, type Executor, type Trigger } from '../src/core/index.ts'
import { clearDingtalkTokens, resolveDingtalkBot } from '../src/deployment.ts'
import { apply as applyDingtalk, dingtalkKind, dingtalkLink, dingtalkMention, dingtalkText, dingtalkThreadContext, dingtalkTime,
  dingtalkTitle, dingtalkTrigger, dingtalkTriggerId, sendDingtalk } from '../src/sources/dingtalk.ts'
import { SourceRegistry } from '../src/sources/registry.ts'
import { Store } from '../src/app/store.ts'
import { Commands } from '../src/infra/commands.ts'
import { HealthBoard } from '../src/infra/health.ts'
import { testDeployment } from './fixtures/deployment.ts'
import { SettingsSchema, settingsPolicy } from '../src/policy.ts'
import { sourceContextOf, type SourceContextDeps } from '../src/app/source-context.ts'
import { SetupError } from '../src/setup.ts'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const ALICE = 'alice'
const BOT = 'dingbot'
const credentials = { clientId: BOT, clientSecret: 's3cret', robotCode: BOT }
function host(overrides: Partial<SourceContextDeps> = {}) {
  const store = new Store(join(mkdtempSync(join(tmpdir(), 'dingtalk-')), 'db.sqlite'))
  store.saveSettings(SettingsSchema.parse({
    policy: { sources: { dingtalk: { allowed: [{ id: ALICE, name: 'Alice' }] }, github: { allowed: [{ id: 'alice' }] } } },
    eval: { mode: 'auto' },
    sources: { dingtalk: { enabled: true, config: { chats: [{ conversationId: 'cidteam', listen: 'mentions' }, { conversationId: 'cidobs', listen: 'all' }],
      directMessages: true } } } }))
  const board = new HealthBoard()
  const sources = new SourceRegistry(() => store.settings(), board)
  sources.register(dingtalkKind)
  const deployment = testDeployment({ dingtalk: credentials })
  const assessor: Assessor = { request: vi.fn(async () => {}) }
  const executor: Executor = { start: vi.fn(async () => {}), deliver: vi.fn(async () => {}), stop: vi.fn(async () => {}) }
  const engine = new Engine({ repo: store, scope: sources, policy: () => settingsPolicy(store.settings(), sources), assessor, executor,
    outbox: { send: vi.fn(), status: () => undefined, withdraw: vi.fn() } })
  const commands = new Commands(() => deployment, {}, new AbortController().signal)
  const receive = vi.fn(async (trigger: Trigger) => { const result = engine.receive(trigger); engine.evaluateDue(); return result })
  const deps: SourceContextDeps = { store, engine, outbox: { send: message => { store.receipts.add(message) } }, commands, sources,
    deployment, ready: Promise.resolve(), configured: () => true, receive, ...overrides }
  const value = { ...deps, context: (kind: { kind: string; health: readonly string[] }) => sourceContextOf(deps, kind) }
  return { ...value, store, engine, commands, receive, health: board.all, ctx: sourceContextOf(deps, dingtalkKind) }
}
/** One robot message as the Stream consumer delivers it. */
const message = (fields: Record<string, unknown> = {}) => ({ msgId: 'msg2', conversationId: 'cidteam', conversationType: '2',
  senderId: '$:LWCP_v1:$opaque', senderStaffId: ALICE, senderNick: 'Alice', createAt: '1757376000000', chatbotUserId: 'chatbot',
  robotCode: BOT, msgtype: 'text', text: { content: 'hello' }, atUsers: [], ...fields })
/** Stubs `fetch` for the token endpoint and the send APIs; returns the bodies it was asked to post. */
function api(answers: { token?: unknown; send?: unknown; status?: number } = {}) {
  const bodies: { url: string; body: unknown }[] = []
  vi.stubGlobal('fetch', vi.fn(async (url: string, init?: RequestInit) => {
    const body = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined
    bodies.push({ url, body })
    const isToken = url.endsWith('/accessToken')
    const data = isToken ? (answers.token ?? { accessToken: 't0ken', expireIn: 7200 }) : (answers.send ?? { processQueryKey: 'q1' })
    const status = isToken ? 200 : (answers.status ?? 200)
    return { ok: status < 400, status, text: async () => JSON.stringify(data) } as Response
  }))
  return bodies
}
beforeEach(() => { clearDingtalkTokens(); vi.unstubAllGlobals() })

describe('dingtalk source', () => {
  it('registers its kind without credentials, and names on its health what setup still needs', async () => {
    const run = async (overrides: Partial<SourceContextDeps>, settings = SettingsSchema.parse({})) => {
      // The fixture's registry already holds the kind; this one starts empty so the plugin's registration shows.
      const board = new HealthBoard()
      const empty = new SourceRegistry(() => settings, board)
      const value = host({ sources: empty, ...overrides })
      const disposers: (() => unknown)[] = []
      applyDingtalk({ orchestrator: value, effect: (effect: () => () => unknown) => { disposers.push(effect()) } } as never)
      await vi.waitFor(() => expect(board.all.dingtalk).toMatchObject({ state: 'paused' }))
      const registered = empty.kind('dingtalk')
      // Snapshot health before disposal clears the registered entries.
      const health = { ...board.all }
      for (const dispose of disposers) await dispose()
      return { registered, health }
    }
    const unset = await run({ deployment: testDeployment() })
    expect(unset.registered).toBe(dingtalkKind)
    expect(unset.health.dingtalk).toMatchObject({ state: 'paused', code: 'dingtalkCredsUnset' })
    const problem = new SetupError('dingtalkCredentialsRefused', { output: 'HTTP 400' }).problem
    const refused = await run({ deployment: testDeployment(), dingtalkProblem: problem })
    // A failed credential check waits for the operator's fix and a check; it does not retry by itself.
    expect(refused.health.dingtalk).toEqual({ state: 'paused', detail: problem.detail })
    expect((await run({ configured: () => false })).health.dingtalk).toEqual({ state: 'paused', detail: 'Waiting for the deployment check', code: 'setupPending' })
    expect((await run({ deployment: testDeployment({ dingtalk: credentials }) }, SettingsSchema.parse({ sources: { dingtalk: { enabled: false } } }))).health.dingtalk)
      .toMatchObject({ state: 'paused', code: 'sourceOff' })
  })

  it('translates robot messages into triggers keyed by the message, and drops the bot\'s own', () => {
    const trigger = dingtalkTrigger(message({ isReplyMsg: true, originalMsgId: 'msg0',
      repliedMsg: { msgId: 'msg1', senderStaffId: 'bob', createAt: 1757375990000, text: { content: 'earlier' } }, isInAtList: true }))!
    expect(trigger).toMatchObject({ id: key('dingtalk', 'msg2'), source: 'dingtalk', threads: [{ source: 'dingtalk', id: 'cidteam:msg0' }],
      actor: { source: 'dingtalk', id: ALICE }, human: true, at: '2025-09-09T00:00:00.000Z', text: 'hello',
      channel: { source: 'dingtalk', thread: 'dingtalk:cidteam', address: { conversationId: 'cidteam', conversationType: '2',
        messageId: 'msg2', senderId: ALICE } },
      meta: { conversationId: 'cidteam', direct: false, mentioned: true, messageId: 'msg2', rootId: 'msg0', parentId: 'msg1' } })
    expect(trigger.addressed).toBe('mention')
    expect(trigger.meta.parent).toMatchObject({ id: 'msg1', from: 'bob', text: 'earlier' })
    // Without a reply chain the thread is the message itself, and a direct message addresses the agent.
    expect(dingtalkTrigger(message({ conversationType: '1', msgId: 'msg3' }))!.threads[0]!.id).toBe(`cidteam:msg3`)
    expect(dingtalkTrigger(message({ conversationType: '1', msgId: 'msg3' }))!.addressed).toBe('direct')
    expect(dingtalkTrigger(message({ conversationType: '1', msgId: 'msg3' }))!.channel!.address.direct).toBe('true')
    // A group message with no mention addresses no one; the mention list may name the bot instead of the flag.
    expect(dingtalkTrigger(message({ msgId: 'msg4' }))).not.toHaveProperty('addressed')
    expect(dingtalkTrigger(message({ msgId: 'msg5', atUsers: [{ dingtalkId: BOT }] }))!.addressed).toBe('mention')
    // The bot's own messages produce no Trigger, and a message that names no id produces none.
    expect(dingtalkTrigger(message({ senderStaffId: 'chatbot', msgId: 'msg6' }))).toBeUndefined()
    expect(dingtalkTrigger(message({ msgId: 'msg7', senderId: BOT, senderStaffId: undefined }))).toBeUndefined()
    expect(dingtalkTrigger(message({ msgId: undefined, messageId: undefined, conversationId: 'cidteam' }))).toBeUndefined()
  })

  it('reads a message\'s text, timestamps and non-text placeholders', () => {
    expect(dingtalkText({ msgtype: 'text', text: { content: 'hi' } })).toBe('hi')
    expect(dingtalkText({ msgtype: 'richText', richText: [{ text: 'a' }, { text: 'b' }] })).toBe('ab')
    expect(dingtalkText({ msgtype: 'picture' })).toBe('[picture]')
    expect(dingtalkTime(1757376000000)).toBe('2025-09-09T00:00:00.000Z')
    expect(dingtalkTime('2025-09-09 08:00')).toContain('2025-09-09')
    expect(() => dingtalkTime('yesterday')).toThrow('Unreadable DingTalk message time')
    expect(dingtalkTitle('## Fix the margin\nmore')).toBe('Fix the margin')
    expect(dingtalkMention(ALICE, 'done')).toBe('<@alice> done')
    expect(dingtalkMention('$:LWCP_v1:$x', 'done')).toBe('done')
  })

  it('decides what it receives from the conversation alone', () => {
    const scope = (meta: Record<string, unknown>) => dingtalkKind.scope!({ meta }, dingtalkKind.schema.parse({ chats: [
      { conversationId: 'cidteam', listen: 'mentions' }, { conversationId: 'cidobs', listen: 'all' }], directMessages: true }))
    expect(scope({ conversationId: 'cidteam', direct: false })).toEqual({ listened: true, observed: false, row: 'cidteam' })
    expect(scope({ conversationId: 'cidobs', direct: false })).toEqual({ listened: true, observed: true, row: 'cidobs' })
    expect(scope({ conversationId: 'cidelse', direct: false })).toEqual({ listened: false, reason: 'chat_not_allowed' })
    expect(scope({ conversationId: 'cidelse', direct: true })).toEqual({ listened: true, observed: false, row: 'direct' })
    const closed = (meta: Record<string, unknown>) => dingtalkKind.scope!({ meta }, dingtalkKind.schema.parse({ directMessages: false }))
    expect(closed({ conversationId: 'cid1', direct: true })).toEqual({ listened: false, reason: 'direct_messages_not_allowed' })
    // A reply goes only to a conversation the Source still receives.
    const config = dingtalkKind.schema.parse({ chats: [{ conversationId: 'cidteam', listen: 'mentions' }] })
    expect(dingtalkKind.channel!.allowed!({ conversationId: 'cidteam', direct: 'false' }, config)).toBe(true)
    expect(dingtalkKind.channel!.allowed!({ conversationId: 'cidelse', direct: 'false' }, config)).toBe(false)
  })

  it('sends a group reply with a mention, and a direct reply to the user', async () => {
    const h = host()
    const bodies = api()
    const reply = (address: Record<string, string>) => sendDingtalk({ commands: h.commands },
      { id: 'm1', kind: 'reply', text: 'all done', channel: { source: 'dingtalk', thread: 'dingtalk:cidteam', address } })
    expect(await reply({ conversationId: 'cidteam', conversationType: '2', messageId: 'msg2', senderId: ALICE })).toBe('sent')
    expect(await reply({ conversationId: 'cidteam', conversationType: '1', messageId: 'msg2', senderId: ALICE, direct: 'true' })).toBe('sent')
    const [token, group, direct] = bodies
    expect(token!.body).toEqual({ appKey: BOT, appSecret: 's3cret' })
    expect(token!.url).toContain('/oauth2/accessToken')
    expect(group!.url).toContain('/v1.0/robot/groupMessages/send')
    expect(group!.body).toMatchObject({ robotCode: BOT, openConversationId: 'cidteam', msgKey: 'sampleMarkdown' })
    expect(JSON.parse((group!.body as { msgParam: string }).msgParam)).toEqual({ title: 'all done', text: '<@alice> all done' })
    expect(direct!.url).toContain('/v1.0/robot/oToMessages/batchSend')
    expect(direct!.body).toMatchObject({ robotCode: BOT, userIds: [ALICE] })
    expect(JSON.parse((direct!.body as { msgParam: string }).msgParam)).toEqual({ title: 'all done', text: 'all done' })
    // The access token is cached between sends.
    expect(bodies).toHaveLength(3)
    h.store.close()
  })

  it('settles a reply as gone only when DingTalk can never deliver it', async () => {
    const h = host()
    const reply = () => sendDingtalk({ commands: h.commands },
      { id: 'm1', kind: 'reply', text: 'all done', channel: { source: 'dingtalk', thread: 'dingtalk:cidteam',
        address: { conversationId: 'cidteam', conversationType: '1', messageId: 'msg2', senderId: ALICE, direct: 'true' } } })
    api({ send: { invalidStaffIdList: [ALICE] } })
    expect(await reply()).toBe('gone')
    clearDingtalkTokens()
    api({ send: { code: 'InvalidParameter.None', message: 'the conversation does not exist' }, status: 404 })
    expect(await reply()).toBe('gone')
    clearDingtalkTokens()
    // A throttled or failed send is retried rather than dropped.
    api({ status: 500 })
    await expect(reply()).rejects.toThrow()
    clearDingtalkTokens()
    api({ send: { code: 'RateLimited', message: 'flow control' }, status: 429 })
    await expect(reply()).rejects.toThrow('flow control')
    clearDingtalkTokens()
    api({ send: { invalidStaffIdList: [] } })
    expect(await reply()).toBe('sent')
    h.store.close()
  })

  it('gives assessments the message and the message it replies to', async () => {
    const trigger = dingtalkTrigger(message({ isReplyMsg: true,
      repliedMsg: { msgId: 'msg1', senderStaffId: 'bob', createAt: 1757375990000, text: { content: 'earlier' } } }))!
    const config = dingtalkKind.schema.parse({})
    expect(dingtalkThreadContext.messageId(trigger)).toBe('msg2')
    const thread = await dingtalkThreadContext.read(new Commands(() => testDeployment(), {}, new AbortController().signal),
      trigger, 'thread', undefined, config)
    expect(thread.messages).toEqual([{ id: 'msg2', from: ALICE, at: '2025-09-09T00:00:00.000Z', text: 'hello' }])
    const parent = await dingtalkThreadContext.read(new Commands(() => testDeployment(), {}, new AbortController().signal),
      trigger, 'parent', undefined, config)
    expect(parent.messages).toEqual([{ id: 'msg1', from: 'bob', at: expect.any(String), text: 'earlier' }])
  })

  it('offers its admission rows and unnamed sender labels to the Policy', () => {
    const policy = dingtalkKind.policy!
    const part = policy.schema.parse({ allowed: [{ id: ALICE, name: 'Alice' }] })
    expect(part.allowed).toEqual([{ id: ALICE, name: 'Alice' }])
    expect(() => policy.schema.parse({ allowed: [{ id: 'alice' }, { id: 'alice' }] })).toThrow('Duplicate user IDs')
    const rows = policy.rows!(dingtalkKind.schema.parse({ chats: [{ conversationId: 'cidteam', name: 'Team', listen: 'mentions' },
      { conversationId: 'cidobs', listen: 'all' }], directMessages: true, otherChats: 'mentions' }))
    expect(rows.map(row => row.id)).toEqual(['cidteam', 'cidobs', 'other', 'direct'])
    expect(rows[0]).toMatchObject({ label: 'Team', cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false } })
    expect(rows[1]!.cells).toEqual({ listedUnaddressed: true, unlistedAddressed: true, unlistedUnaddressed: true })
    expect(rows[2]!.cells).toEqual({ listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false })
    expect(rows[3]!.cells).toEqual({ listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false })
    expect(policy.unnamed!.person.zh).toContain('钉钉')
  })

  it('takes an allowed sender\'s mention as an explicit request, and filters what it does not receive', async () => {
    const h = host()
    expect(await h.receive(dingtalkTrigger(message({ msgId: 'msg2', isInAtList: true }))!)).toBe('stored')
    expect(h.engine.candidate(key('dingtalk', 'msg2'))?.strength).toBe('explicit')
    expect(await h.receive(dingtalkTrigger(message({ msgId: 'msg2' }))!)).toBe('duplicate')
    const outside = dingtalkTrigger(message({ msgId: 'msg3', conversationId: 'cidelse' }))!
    expect(await h.receive(outside)).toBe('filtered')
    expect(h.store.triggers.disposition(key('dingtalk', 'msg3'))?.filtered).toEqual({ code: 'chat_not_allowed' })
    h.store.close()
  })

  it('verifies the app credentials by fetching an access token, or names what is missing', async () => {
    expect(await resolveDingtalkBot({})).toBeUndefined()
    await expect(resolveDingtalkBot({ clientId: BOT })).rejects.toEqual(expect.objectContaining({ place: 'dingtalk', code: 'dingtalkCredentialsIncomplete' }))
    const refused = vi.fn(async () => ({ ok: false, status: 400, text: async () => JSON.stringify({ message: 'bad secret' }) }) as Response)
    await expect(resolveDingtalkBot({ clientId: BOT, clientSecret: 'wrong' }, refused))
      .rejects.toEqual(expect.objectContaining({ place: 'dingtalk', code: 'dingtalkCredentialsRefused', values: expect.objectContaining({ output: 'bad secret' }) }))
    const fetcher = vi.fn(async () => ({ ok: true, status: 200, text: async () => JSON.stringify({ accessToken: 't0ken', expireIn: 7200 }) }) as Response)
    expect(await resolveDingtalkBot({ clientId: BOT, clientSecret: 's3cret' }, fetcher)).toEqual({ clientId: BOT, clientSecret: 's3cret', robotCode: BOT })
    expect(await resolveDingtalkBot({ clientId: BOT, clientSecret: 's3cret', robotCode: 'other' }, fetcher)).toMatchObject({ robotCode: 'other' })
  })

  it('links a group conversation only when the callback carried the AppLink ids', () => {
    expect(dingtalkLink({ conversationId: 'cid1', corpId: 'dingcorp', chatId: 'ch1' }, {})).toEqual({
      url: 'https://applink.dingtalk.com/page/conversation?corpId=dingcorp&chatId=ch1', label: { zh: '钉钉会话', en: 'DingTalk conversation' } })
    expect(dingtalkLink({ conversationId: 'cid1', direct: 'true', corpId: 'dingcorp', chatId: 'ch1' }, {}).url).toBe('')
    expect(dingtalkLink({ conversationId: 'cid1' }, {}).url).toBe('')
    expect(dingtalkTriggerId('msg9')).toBe(key('dingtalk', 'msg9'))
  })
})
