/** Normalizes DingTalk robot callbacks into domain Triggers. */
import { z } from 'zod'
import { key, type ChannelRef, type Trigger } from '../core/index.ts'

export const SOURCE = 'dingtalk'
/** Constructs a DingTalk reply channel address; `conversationType` `1` is a direct chat, `2` a group chat. */
export const dingtalkChannel = (conversationId: string, conversationType: string, messageId: string, senderId: string,
  extra: Record<string, string> = {}): ChannelRef =>
  ({ source: SOURCE, thread: `dingtalk:${conversationId}`,
    address: { conversationId, conversationType, messageId, senderId, ...conversationType === '1' ? { direct: 'true' } : {}, ...extra } })
/** A message's Thread: the conversation and the message that started it (its reply chain's root, or itself). */
export const dingtalkThreadId = (conversationId: string, rootId: string): string => `${conversationId}:${rootId}`
/** Deterministic trigger ID for a DingTalk message; every read path for this message produces the same ID. */
export const dingtalkTriggerId = (messageId: string): string => key('dingtalk', messageId)

/** Parses a DingTalk message timestamp (epoch milliseconds, or a local timestamp string) into an ISO-8601 string. */
export function dingtalkTime(value: string | number): string {
  const time = typeof value === 'number' ? value : /^\d+$/.test(value) ? Number(value) : Date.parse(value)
  if (!Number.isFinite(time)) throw new Error(`Unreadable DingTalk message time: ${value}`)
  return new Date(time).toISOString()
}

const TextSchema = z.looseObject({ content: z.string().optional() })
const RichTextElementSchema = z.looseObject({ type: z.string().optional(), text: z.string().optional() })
const AtUserSchema = z.looseObject({ dingtalkId: z.string().optional(), staffId: z.string().optional(), nick: z.string().optional() })
/** The message a DingTalk message replies to; its payload carries the parent so no network read is needed. */
export const RepliedMessageSchema = z.looseObject({
  msgId: z.string().optional(), messageId: z.string().optional(),
  senderStaffId: z.string().optional(), senderId: z.string().optional(), senderNick: z.string().optional(),
  createAt: z.union([z.string(), z.number()]).optional(), msgtype: z.string().optional(),
  text: TextSchema.optional(), richText: z.array(RichTextElementSchema).optional(),
})
export type RepliedMessage = z.infer<typeof RepliedMessageSchema>
/** One robot message as the Stream consumer delivers it (`/v1.0/im/bot/messages/get`). */
export const DingtalkEventSchema = z.looseObject({
  msgId: z.string().optional(), messageId: z.string().optional(),
  conversationId: z.string().min(1), conversationTitle: z.string().optional(),
  conversationType: z.union([z.string(), z.number()]).transform(value => String(value)),
  senderId: z.string().default(''), senderStaffId: z.string().optional(), senderNick: z.string().optional(),
  createAt: z.union([z.string(), z.number()]),
  chatbotUserId: z.string().optional(), chatbotCorpId: z.string().optional(), robotCode: z.string().optional(),
  msgtype: z.string().default('text'), text: TextSchema.optional(), content: z.unknown().optional(),
  richText: z.array(RichTextElementSchema).optional(),
  atUsers: z.array(AtUserSchema).default([]), isInAtList: z.boolean().optional(),
  isReplyMsg: z.boolean().optional(), repliedMsg: RepliedMessageSchema.optional(), originalMsgId: z.string().optional(),
})
export type DingtalkEvent = z.infer<typeof DingtalkEventSchema>

/** The message's text: plain text, the text of its rich-text elements, or a placeholder for a non-text message. */
export function dingtalkText(event: Pick<DingtalkEvent, 'msgtype' | 'text' | 'richText' | 'content'>): string {
  if (event.msgtype === 'text') return event.text?.content ?? ''
  const elements = event.richText ?? (Array.isArray(event.content) ? event.content as { text?: string }[] : [])
  const text = elements.map(element => element?.text ?? '').join('')
  return text || `[${event.msgtype}]`
}
const textOf = (message: RepliedMessage): string =>
  dingtalkText({ msgtype: message.msgtype ?? 'text', text: message.text, richText: message.richText })
/** The identity a message names for its sender: the readable staff ID where DingTalk gives one, else the opaque sender ID. */
export const dingtalkSenderId = (event: { senderStaffId?: string; senderId?: string }): string =>
  event.senderStaffId || event.senderId || ''
/**
 * Converts a DingTalk robot message into a Trigger, returning undefined for messages sent by the bot itself.
 * A reply keeps its parent's copy as `meta.parent` because DingTalk offers no history API to reread it later.
 */
export function dingtalkTrigger(input: unknown): Trigger | undefined {
  const event = DingtalkEventSchema.parse(input)
  const messageId = event.msgId ?? event.messageId
  if (!messageId) return undefined
  const sender = dingtalkSenderId(event)
  // Never produce a Trigger for the agent's own message, should the stream echo the bot's sends.
  if (sender && (sender === event.chatbotUserId || sender === event.robotCode)) return undefined
  const direct = event.conversationType === '1'
  const mentioned = event.isInAtList === true || event.atUsers.some(user =>
    [event.chatbotUserId, event.robotCode].includes(user.dingtalkId) || [event.chatbotUserId, event.robotCode].includes(user.staffId))
  const parent = event.repliedMsg
  const parentId = parent ? parent.msgId ?? parent.messageId : undefined
  const rootId = event.originalMsgId ?? parentId ?? messageId
  const thread = dingtalkThreadId(event.conversationId, rootId)
  return { id: dingtalkTriggerId(messageId), source: SOURCE, threads: [{ source: SOURCE, id: thread }],
    actor: { source: SOURCE, id: sender }, human: true,
    ...(direct ? { addressed: 'direct' as const } : mentioned ? { addressed: 'mention' as const } : {}),
    at: dingtalkTime(event.createAt), text: dingtalkText(event),
    channel: dingtalkChannel(event.conversationId, event.conversationType, messageId, sender,
      { ...(event.chatbotCorpId ? { corpId: event.chatbotCorpId } : {}), ...(event.conversationTitle ? { title: event.conversationTitle } : {}) }),
    meta: { conversationId: event.conversationId, conversationType: event.conversationType, direct, mentioned, messageId,
      ...(event.conversationTitle ? { conversationTitle: event.conversationTitle } : {}), ...(event.senderNick ? { senderNick: event.senderNick } : {}),
      msgtype: event.msgtype, rootId, ...(parentId ? { parentId } : {}),
      ...(parent ? { parent: { id: parentId, from: dingtalkSenderId(parent), at: parent.createAt !== undefined ? dingtalkTime(parent.createAt) : '',
        text: textOf(parent).slice(0, 2000) } } : {}) } }
}

