/** DingTalk outbound channel: robot markdown replies and conversation deep links. */
import type { OutboundMessage } from '../core/index.ts'
import { dingtalkToken, type DingtalkBot } from '../deployment.ts'
import { CliError } from '../infra/commands.ts'
import { checkedImText } from '../infra/text.ts'
import type { SourceContext } from './host.ts'
import type { ChannelLink, ChannelLinks } from './kind.ts'

export const DINGTALK_API = 'https://api.dingtalk.com'
/** First line of the reply as the title of DingTalk's markdown message template (at most 20 characters). */
export const dingtalkTitle = (text: string): string => {
  const line = text.split('\n')[0] ?? ''
  const cleaned = line.replace(/^[#*\s\-><@]+/, '').slice(0, 20)
  return cleaned || 'dsh-superagent'
}
/** Prefixes a group reply with the mention of the person it answers; an opaque external ID cannot be mentioned. */
export const dingtalkMention = (senderId: string, text: string): string =>
  senderId && !/[:$]/.test(senderId) ? `<@${senderId}> ${text}` : text
/** True when DingTalk permanently rejects a reply because the target user or conversation can never receive it. */
const gone = (error: unknown): boolean => error instanceof CliError
  && (error.code === 404 || /not exist|no longer|not in (the )?(chat|conversation)|invalid (conversation|user)|invalidStaffId/i.test(error.message))

interface DingtalkSendResult { processQueryKey?: string; invalidStaffIdList?: unknown[]; flowControlledStaffIdList?: unknown[];
  code?: string | number; message?: string; errcode?: number; errmsg?: string }
/**
 * Posts one JSON request to the DingTalk OpenAPI with the app's access token.
 * @throws CliError with DingTalk's error code or the HTTP status on failure.
 */
export async function dingtalkApi<T>(bot: Pick<DingtalkBot, 'clientId' | 'clientSecret'>, path: string, body: unknown,
  fetcher: typeof fetch = fetch): Promise<T> {
  const token = await dingtalkToken(bot, fetcher)
  const response = await fetcher(`${DINGTALK_API}${path}`, { method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-acs-dingtalk-access-token': token }, body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000) })
  const text = await response.text()
  let data: Partial<DingtalkSendResult> = {}
  try { data = JSON.parse(text) as Partial<DingtalkSendResult> } catch { /* Non-JSON diagnostics are kept below. */ }
  if (!response.ok || data.errcode || (data.code !== undefined && data.code !== 'OK' && data.code !== 0)) {
    const message = data.message ?? data.errmsg ?? text.slice(0, 500) ?? 'DingTalk request failed'
    const code = typeof data.errcode === 'number' ? data.errcode : typeof data.code === 'number' ? data.code : response.status
    throw new CliError(`${message}${data.code && typeof data.code === 'string' ? ` (${data.code})` : ''}`, code, response.status)
  }
  return data as T
}
/**
 * Replies to one DingTalk message through the robot APIs: a group reply goes to the conversation and mentions the
 * person it answers, a direct reply to that user. DingTalk takes no idempotency key, so a retry after a lost
 * acknowledgment may send the reply twice.
 */
export async function sendDingtalk(host: Pick<SourceContext, 'commands'>, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const bot = host.commands.deployment.dingtalk
  if (!bot) throw new Error('The DingTalk Source has no working app credentials')
  const { conversationId, conversationType, senderId, direct } = message.channel.address
  if (!conversationId) throw new Error('Missing DingTalk reply target')
  const oneToOne = direct === 'true' || conversationType === '1'
  if (oneToOne && !senderId) throw new Error('Missing DingTalk reply user')
  const text = checkedImText(message.text)
  const title = dingtalkTitle(text)
  const msgParam = JSON.stringify({ title, text: oneToOne ? text : dingtalkMention(senderId ?? '', text) })
  try {
    const result = await dingtalkApi<DingtalkSendResult>(bot, oneToOne ? '/v1.0/robot/oToMessages/batchSend' : '/v1.0/robot/groupMessages/send',
      oneToOne
        ? { robotCode: bot.robotCode, userIds: [senderId], msgKey: 'sampleMarkdown', msgParam }
        : { robotCode: bot.robotCode, openConversationId: conversationId, msgKey: 'sampleMarkdown', msgParam })
    // A user DingTalk cannot deliver to settles the message as gone; a throttled one is retried.
    if (oneToOne && result.invalidStaffIdList?.length) return 'gone'
    return 'sent'
  } catch (error) {
    if (gone(error)) return 'gone'
    throw error
  }
}
/** Conversation deep link (groups only, when the callback carried the IDs AppLink needs); DingTalk messages have no stable link. */
export const dingtalkLink = (address: Record<string, string>, _links: ChannelLinks): ChannelLink => {
  const { corpId, chatId, direct } = address
  const url = direct === 'true' || !corpId || !chatId ? ''
    : `https://applink.dingtalk.com/page/conversation?corpId=${encodeURIComponent(corpId)}&chatId=${encodeURIComponent(chatId)}`
  return { url, label: { zh: '钉钉会话', en: 'DingTalk conversation' } }
}
