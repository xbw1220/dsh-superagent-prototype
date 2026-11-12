/** DingTalk source kind definition and Cordis plugin entry. */
import type { Context } from '@deepseek-ai/cordis'
import { z } from 'zod'
import type { Trigger } from '../core/index.ts'
import { DingtalkBotSchema } from '../deployment.ts'
import { dingtalkLink, sendDingtalk } from './dingtalk-channel.ts'
import { runDingtalk } from './dingtalk-stream.ts'
import { dingtalkTriggerId, SOURCE } from './dingtalk-trigger.ts'
import { sourceContext } from './host.ts'
import type { Localized, SourceKind, ThreadContext } from './kind.ts'

export { dingtalkChannel, dingtalkText, dingtalkThreadId, dingtalkTime, dingtalkTrigger, dingtalkTriggerId, dingtalkSenderId }
  from './dingtalk-trigger.ts'
export type { DingtalkEvent } from './dingtalk-trigger.ts'
export { sendDingtalk, dingtalkLink, dingtalkTitle, dingtalkMention } from './dingtalk-channel.ts'
export { consume, openStream, runDingtalk, TOPIC_ROBOT } from './dingtalk-stream.ts'

const DingtalkConfigSchema = DingtalkBotSchema.extend({
  chats: z.array(z.object({ conversationId: z.string().regex(/^cid[A-Za-z0-9+/=_-]+$/), name: z.string().trim().max(60).optional(),
    listen: z.enum(['mentions', 'all']) }).strict())
    .refine(chats => new Set(chats.map(chat => chat.conversationId)).size === chats.length, 'Duplicate conversation IDs').default([]),
  directMessages: z.boolean().default(true),
  otherChats: z.enum(['none', 'mentions']).default('none'),
}).strict()
export type DingtalkConfig = z.infer<typeof DingtalkConfigSchema>

/**
 * Assessments read the message itself and the message it replies to, which the callback carries; DingTalk offers no
 * history API to reread a conversation around it.
 */
export const dingtalkThreadContext: ThreadContext<DingtalkConfig> = {
  messageId: trigger => String(trigger.meta.messageId ?? ''),
  read: async (_commands, trigger: Trigger, part) => {
    const own = { id: String(trigger.meta.messageId ?? ''), from: trigger.actor.id, at: trigger.at, text: trigger.text }
    const parent = trigger.meta.parent as { id?: string; from?: string; at?: string; text?: string } | undefined
    const messages = part === 'parent' && parent?.id
      ? [{ id: parent.id, from: parent.from ?? '', at: parent.at ?? '', text: parent.text ?? '' }]
      : [own]
    return { messages, body: { messages } }
  },
}

export const dingtalkKind: SourceKind<DingtalkConfig> = {
  kind: SOURCE,
  title: { zh: '钉钉', en: 'DingTalk' },
  description: { zh: '通过 Stream 模式接收发给机器人的群聊和私聊消息。', en: 'Receives group and direct messages to the bot through Stream mode.' },
  schema: DingtalkConfigSchema,
  fields: [
    { key: 'clientId', type: 'text', label: { zh: '应用 Client ID', en: 'App Client ID' },
      hint: { zh: '钉钉开发者后台“凭证与基础信息”里的 Client ID（原 AppKey）。保存时验证凭证，然后重启 dsh-superagent。', en: 'The Client ID (AppKey) on the app\'s Credentials page in the DingTalk developer console. Saving checks the credentials, then restarts dsh-superagent.' } },
    { key: 'clientSecret', type: 'text', label: { zh: '应用 Client Secret', en: 'App Client Secret' },
      hint: { zh: '同一页的 Client Secret（原 AppSecret）；机器人能力需选择 Stream 模式并发布。', en: 'The Client Secret (AppSecret) on the same page; the bot capability must be published in Stream mode.' } },
    { key: 'robotCode', type: 'text', advanced: true, label: { zh: '机器人代码 robotCode', en: 'Robot code' },
      hint: { zh: '发消息接口使用的机器人代码；留空时使用 Client ID。收到的消息里的 robotCode 字段就是它。', en: 'The robot code the send APIs use; empty falls back to the Client ID. Received messages name it in their robotCode field.' } },
    { key: 'chats', type: 'rows', label: { zh: '接收的群', en: 'Chats' },
      hint: { zh: '“仅 @”只接收 @ 机器人的消息和已有工单的线程；“全部消息”也接收其余消息，评估可以按规则从中推断工作。会话 ID 来自机器人收到消息里的 conversationId。', en: '"@ only" receives messages that mention the bot and Threads that have Tickets; "All" also receives the rest, from which Eval may infer work under the rules. A conversation ID is the conversationId of a message the bot receives.' },
      columns: [
        { key: 'conversationId', type: 'text', label: { zh: '会话', en: 'Conversation' }, pattern: 'cid[A-Za-z0-9+/=_-]+', choose: { zh: '选择机器人收到过消息的会话', en: 'Choose a conversation the bot has received messages in' } },
        { key: 'listen', type: 'select', label: { zh: '接收', en: 'Receive' }, options: [{ value: 'mentions', label: { zh: '仅 @', en: '@ only' } }, { value: 'all', label: { zh: '全部消息', en: 'All' } }] },
      ] },
    { key: 'otherChats', type: 'select', label: { zh: '其他群', en: 'Other chats' }, options: [{ value: 'none', label: { zh: '不接收', en: 'Not received' } }, { value: 'mentions', label: { zh: '仅 @', en: '@ only' } }],
      hint: { zh: '机器人已加入、但没列在上面的群。“仅 @”接收那里 @ 机器人的消息；谁的消息能发起工作看策略里的准入。', en: 'Chats the bot has joined that are not listed above. "@ only" receives messages there that mention the bot; whose messages may originate work is the Policy\'s admission.' } },
    { key: 'directMessages', type: 'switch', label: { zh: '接收私聊', en: 'Direct messages' } },
  ],
  health: ['dingtalk'],
  scope({ meta }, config) {
    if (meta.direct === true) return config.directMessages ? { listened: true, observed: false, row: 'direct' } : { listened: false, reason: 'direct_messages_not_allowed' }
    const chat = config.chats.find(item => item.conversationId === meta.conversationId)
    if (chat) return { listened: true, observed: chat.listen === 'all', row: chat.conversationId }
    return config.otherChats === 'mentions' ? { listened: true, observed: false, row: 'other' } : { listened: false, reason: 'chat_not_allowed' }
  },
  // A reply goes only to a conversation the Source still receives.
  channel: {
    allowed: (address, config) =>
      dingtalkKind.scope!({ meta: { conversationId: address.conversationId ?? '', direct: address.direct === 'true' } }, config).listened,
    send: sendDingtalk, link: dingtalkLink,
  },
  threadContext: dingtalkThreadContext,
  find: query => ({ threads: [], terms: [...new Set(query.match(/(?:msg|cid)[A-Za-z0-9+/=_-]{4,}/g) ?? [])] }),
  // A message's Trigger is named after the message, so the one it answers is named after its parent.
  repliesTo: trigger => typeof trigger.meta.parentId === 'string' ? dingtalkTriggerId(trigger.meta.parentId) : undefined,
  policy: {
    unnamed: { person: { zh: '钉钉用户', en: 'DingTalk user' }, bot: { zh: '钉钉机器人', en: 'DingTalk bot' } },
    schema: z.object({
      allowed: z.array(z.object({ id: z.string().min(1).max(100).regex(/^\S+$/), name: z.string().trim().max(100).optional() }).strict())
        .refine(users => new Set(users.map(user => user.id)).size === users.length, 'Duplicate user IDs').default([]),
    }).strict(),
    fields: [
      { key: 'allowed', type: 'rows', label: { zh: '能发起工作的钉钉用户', en: 'DingTalk users who may originate work' },
        hint: { zh: '名单里的 userId 是钉钉的员工 userId，即机器人收到消息里的 senderStaffId。', en: 'A listed userId is the sender\'s staff userId on DingTalk, the senderStaffId of a message the bot receives.' },
        columns: [{ key: 'id', type: 'text', label: { zh: 'userId', en: 'userId' }, pattern: '\\S+' }, { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] },
    ],
    explicit: { zh: '名单里的人在群里 @ 机器人或私聊机器人，总是明确的请求，不需要配置。', en: 'Someone on the list who mentions the bot or messages it directly always makes an explicit request; that needs no setting.' },
    rows(stored) {
      const config = DingtalkConfigSchema.parse(stored)
      const mentions = { zh: '仅 @', en: '@ only' }
      return [
        ...config.chats.map(chat => ({ id: chat.conversationId, label: chat.name || chat.conversationId,
          note: chat.listen === 'all' ? { zh: '全部消息', en: 'All' } : mentions,
          cells: { listedUnaddressed: chat.listen === 'all', unlistedAddressed: true, unlistedUnaddressed: chat.listen === 'all' } })),
        { id: 'other', label: { zh: '其他群', en: 'Other chats' }, note: config.otherChats === 'mentions' ? mentions : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: config.otherChats === 'mentions', unlistedUnaddressed: false } },
        { id: 'direct', label: { zh: '私聊', en: 'Direct messages' }, note: config.directMessages ? { zh: '接收', en: 'Received' } : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: config.directMessages, unlistedUnaddressed: false } },
      ]
    },
  },
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_DINGTALK_BOT', description: { zh: '钉钉机器人代码；发消息接口以它行事', en: 'The DingTalk robot code; the send APIs act as it' } },
      { key: 'DSH_SUPERAGENT_DINGTALK_CHAT', description: { zh: '接下工单的钉钉会话 ID；工单不是从钉钉来的时为空', en: 'ID of the DingTalk conversation that started the ticket; empty when the ticket did not start on DingTalk' } },
    ],
    values: ({ deployment, trigger }) => ({
      DSH_SUPERAGENT_DINGTALK_BOT: deployment.dingtalk ? deployment.dingtalk.robotCode : '',
      DSH_SUPERAGENT_DINGTALK_CHAT: trigger?.source === SOURCE ? String(trigger.meta.conversationId ?? '') : '' }),
  },
  facts: (deployment): Record<string, Localized | string> => deployment?.dingtalk
    ? { clientId: { zh: `当前机器人 ${deployment.dingtalk.robotCode}`, en: `Current bot ${deployment.dingtalk.robotCode}` } } : {},
}

export const name = 'dsh-superagent-dingtalk'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(dingtalkKind), 'dingtalk: source kind')
  ctx.effect(() => {
    const controller = new AbortController()
    const running = runDingtalk(sourceContext(ctx.orchestrator, dingtalkKind), controller.signal)
    return async () => { controller.abort(); await running }
  })
}
