// Cloudflare Worker锛歍elegram 鍙屽悜鏈哄櫒浜?v5.3

// --- 閰嶇疆甯搁噺 ---
const CONFIG = {
    VERIFY_ID_LENGTH: 12,
    VERIFY_EXPIRE_SECONDS: 300,         // 5鍒嗛挓
    VERIFIED_EXPIRE_SECONDS: 2592000,   // 30澶?    MEDIA_GROUP_EXPIRE_SECONDS: 60,
    MEDIA_GROUP_DELAY_MS: 3000,         // 3绉掞紙浠?绉掑鍔狅級
    PENDING_MAX_MESSAGES: 10,           // 楠岃瘉鏈熼棿鏈€澶氭殏瀛樼殑娑堟伅鏁?    ADMIN_CACHE_TTL_SECONDS: 300,       // 绠＄悊鍛樻潈闄愮紦瀛?5 鍒嗛挓
    NEEDS_REVERIFY_TTL_SECONDS: 600,    // 鏍囪闇€閲嶆柊楠岃瘉鐨?TTL锛堢敤浜庡苟鍙戝厹搴曪級
    RATE_LIMIT_MESSAGE: 45,
    RATE_LIMIT_VERIFY: 3,
    RATE_LIMIT_WINDOW: 60,
    BUTTON_COLUMNS: 2,
    MAX_TITLE_LENGTH: 128,
    MAX_NAME_LENGTH: 30,
    API_TIMEOUT_MS: 10000,
    CLEANUP_BATCH_SIZE: 10,
    MAX_CLEANUP_DISPLAY: 20,
    CLEANUP_LOCK_TTL_SECONDS: 1800,     // /cleanup 闃插苟鍙戦攣 30 鍒嗛挓
    MAX_RETRY_ATTEMPTS: 3,
    THREAD_HEALTH_TTL_MS: 60000,
    ADMIN_MESSAGE_MAP_TTL_SECONDS: 2592000
};

// 绾跨▼鍋ュ悍妫€鏌ョ紦瀛橈紝鍑忓皯棰戠箒鎺㈡祴璇锋眰
const threadHealthCache = new Map();
// 鍚屼竴瀹炰緥鍐呯殑骞跺彂淇濇姢锛氶伩鍏嶅悓涓€鐢ㄦ埛鐭椂闂村唴閲嶅鍒涘缓璇濋
const topicCreateInFlight = new Map();
// 绠＄悊鍛樻潈闄愮紦瀛橈紙瀹炰緥鍐咃級
const adminStatusCache = new Map();

// --- 鏈湴棰樺簱 (15鏉? ---
const LOCAL_QUESTIONS = [
    {"question": "What is 1 + 1?", "correct_answer": "2", "incorrect_answers": ["1", "3", "4"]},
    {"question": "What color is the sky on a clear day?", "correct_answer": "Blue", "incorrect_answers": ["Green", "Red", "Black"]},
    {"question": "How many days are there in a week?", "correct_answer": "7", "incorrect_answers": ["5", "6", "8"]},
    {"question": "What is 10 - 3?", "correct_answer": "7", "incorrect_answers": ["6", "8", "9"]},
    {"question": "Which one is a fruit?", "correct_answer": "Banana", "incorrect_answers": ["Potato", "Carrot", "Onion"]}
];

// --- 杈呭姪宸ュ叿鍑芥暟 ---

const Logger = {
    /**
     * 璁板綍淇℃伅绾у埆鏃ュ織
     * @param {string} action - 鎿嶄綔鍚嶇О
     * @param {object} data - 闄勫姞鏁版嵁
     */
    info(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'INFO',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    },

    /**
     * 璁板綍璀﹀憡绾у埆鏃ュ織
     * @param {string} action - 鎿嶄綔鍚嶇О
     * @param {object} data - 闄勫姞鏁版嵁
     */
    warn(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'WARN',
            action,
            ...data
        };
        console.warn(JSON.stringify(log));
    },

    /**
     * 璁板綍閿欒绾у埆鏃ュ織
     * @param {string} action - 鎿嶄綔鍚嶇О
     * @param {Error|string} error - 閿欒瀵硅薄鎴栨秷鎭?     * @param {object} data - 闄勫姞鏁版嵁
     */
    error(action, error, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'ERROR',
            action,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
            ...data
        };
        console.error(JSON.stringify(log));
    },

    /**
     * 璁板綍璋冭瘯绾у埆鏃ュ織
     * @param {string} action - 鎿嶄綔鍚嶇О
     * @param {object} data - 闄勫姞鏁版嵁
     */
    debug(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'DEBUG',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    }
};

// 鍔犲瘑瀹夊叏鐨勯殢鏈烘暟鐢熸垚
function secureRandomInt(min, max) {
    const range = max - min;
    const bytes = new Uint32Array(1);
    crypto.getRandomValues(bytes);
    return min + (bytes[0] % range);
}

function secureRandomId(length = 12) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    const bytes = new Uint8Array(length);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(b => chars[b % chars.length]).join('');
}

// 瀹夊叏鐨?JSON 鑾峰彇
async function safeGetJSON(env, key, defaultValue = null) {
    try {
        const data = await env.TOPIC_MAP.get(key, { type: "json" });
        if (data === null || data === undefined) {
            return defaultValue;
        }
        if (typeof data !== 'object') {
            Logger.warn('kv_invalid_type', { key, type: typeof data });
            return defaultValue;
        }
        return data;
    } catch (e) {
        Logger.error('kv_parse_failed', e, { key });
        return defaultValue;
    }
}

function normalizeTgDescription(description) {
    return (description || "").toString().toLowerCase();
}

function isTopicMissingOrDeleted(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("thread not found") ||
           desc.includes("topic not found") ||
           desc.includes("message thread not found") ||
           desc.includes("topic deleted") ||
           desc.includes("thread deleted") ||
           desc.includes("forum topic not found") ||
           desc.includes("topic closed permanently");
}

function isTestMessageInvalid(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("message text is empty") ||
           desc.includes("bad request: message text is empty");
}

async function getOrCreateUserTopicRec(from, key, env, userId) {
    const existing = await safeGetJSON(env, key, null);
    if (existing && existing.thread_id) return existing;

    const inflight = topicCreateInFlight.get(String(userId));
    if (inflight) return await inflight;

    const p = (async () => {
        // 骞跺彂涓嬩簩娆＄‘璁わ紝閬垮厤宸茶鍏朵粬璇锋眰鍒涘缓鍗磋鍒版棫鍊?        const again = await safeGetJSON(env, key, null);
        if (again && again.thread_id) return again;
        return await createTopic(from, key, env, userId);
    })();

    topicCreateInFlight.set(String(userId), p);
    try {
        return await p;
    } finally {
        if (topicCreateInFlight.get(String(userId)) === p) {
            topicCreateInFlight.delete(String(userId));
        }
    }
}

function withMessageThreadId(body, threadId) {
    if (threadId === undefined || threadId === null) return body;
    return { ...body, message_thread_id: threadId };
}

async function probeForumThread(env, expectedThreadId, { userId, reason, doubleCheckOnMissingThreadId = true } = {}) {
    const attemptOnce = async () => {
        const res = await tgCall(env, "sendMessage", {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: expectedThreadId,
            text: "馃攷"
        });

        const actualThreadId = res.result?.message_thread_id;
        const probeMessageId = res.result?.message_id;

        // 灏藉彲鑳芥竻鐞嗘帰娴嬫秷鎭紙鏃犺钀藉埌鍝釜璇濋/General锛?        if (res.ok && probeMessageId) {
            try {
                await tgCall(env, "deleteMessage", {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: probeMessageId
                });
            } catch (e) {
                // 鍒犻櫎澶辫触涓嶅奖鍝嶄富娴佺▼
            }
        }

        if (!res.ok) {
            if (isTopicMissingOrDeleted(res.description)) {
                return { status: "missing", description: res.description };
            }
            if (isTestMessageInvalid(res.description)) {
                return { status: "probe_invalid", description: res.description };
            }
            return { status: "unknown_error", description: res.description };
        }

        // 鍏抽敭锛氭湁浜涙儏鍐典笅 Telegram 浼氳繑鍥?ok 浣嗕笉甯?message_thread_id锛堝父瑙佷簬 General锛?
        if (actualThreadId === undefined || actualThreadId === null) {
            return { status: "missing_thread_id" };
        }

        if (Number(actualThreadId) !== Number(expectedThreadId)) {
            return { status: "redirected", actualThreadId };
        }

        return { status: "ok" };
    };

    const first = await attemptOnce();
    if (first.status !== "missing_thread_id" || !doubleCheckOnMissingThreadId) return first;

    // 浜屾鎺㈡祴锛氶伩鍏嶅伓鍙戝瓧娈电己澶卞鑷磋鍒ゅ苟瑙﹀彂閲嶅缓
    const second = await attemptOnce();
    if (second.status === "missing_thread_id") {
        Logger.warn('thread_probe_missing_thread_id', { userId, expectedThreadId, reason });
    }
    return second;
}

async function resetUserVerificationAndRequireReverify(env, { userId, userKey, oldThreadId, pendingMsgId, reason }) {
    // 娓呯悊鏃ф槧灏勪笌楠岃瘉鐘舵€侊細鐢ㄦ埛闇€瑕侀噸鏂板仛浜烘満楠岃瘉
    await env.TOPIC_MAP.delete(`verified:${userId}`);
    await env.TOPIC_MAP.put(`needs_verify:${userId}`, "1", { expirationTtl: CONFIG.NEEDS_REVERIFY_TTL_SECONDS });
    await env.TOPIC_MAP.delete(`retry:${userId}`);

    if (userKey) {
        await env.TOPIC_MAP.delete(userKey);
    }

    if (oldThreadId !== undefined && oldThreadId !== null) {
        await env.TOPIC_MAP.delete(`thread:${oldThreadId}`);
        await env.TOPIC_MAP.delete(`thread_ok:${oldThreadId}`);
        threadHealthCache.delete(oldThreadId);
    }

    Logger.info('verification_reset_due_to_topic_loss', {
        userId,
        oldThreadId,
        pendingMsgId,
        reason
    });

    await sendVerificationChallenge(userId, env, pendingMsgId || null);
}

function parseAdminIdAllowlist(env) {
    const raw = (env.ADMIN_IDS || "").toString().trim();
    if (!raw) return null;
    const ids = raw.split(/[,;\s]+/g).map(s => s.trim()).filter(Boolean);
    const set = new Set();
    for (const id of ids) {
        const n = Number(id);
        if (!Number.isFinite(n)) continue;
        set.add(String(n));
    }
    return set.size > 0 ? set : null;
}

async function isAdminUser(env, userId) {
    const allowlist = parseAdminIdAllowlist(env);
    if (allowlist && allowlist.has(String(userId))) return true;
    return String(userId) === String(env.ADMIN_UID);
}

function parseTargetUserIdFromCommand(text) {
    const parts = (text || "").trim().split(/\s+/);
    if (parts.length < 2) return null;
    const n = Number(parts[1]);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
}

async function resolveAdminTargetUserId(msg, env) {
    const cmdTarget = parseTargetUserIdFromCommand(msg.text || "");
    if (cmdTarget) return cmdTarget;

    const replyId = msg.reply_to_message?.message_id;
    if (!replyId) return null;

    const mapped = await env.TOPIC_MAP.get(`admin_msg:${replyId}`);
    if (!mapped) return null;

    const n = Number(mapped);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
}

// 鑾峰彇鎵€鏈?KV keys锛堝鐞嗗垎椤碉級
async function getAllKeys(env, prefix) {
    const allKeys = [];
    let cursor = undefined;

    do {
        const result = await env.TOPIC_MAP.list({ prefix, cursor });
        allKeys.push(...result.keys);
        cursor = result.list_complete ? undefined : result.cursor;
    } while (cursor);

    return allKeys;
}

// Fisher-Yates 娲楃墝绠楁硶
function shuffleArray(arr) {
    const array = [...arr];
    for (let i = array.length - 1; i > 0; i--) {
        const j = secureRandomInt(0, i + 1);
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

// 閫熺巼闄愬埗妫€鏌?async function checkRateLimit(userId, env, action = 'message', limit = 20, window = 60) {
    const key = `ratelimit:${action}:${userId}`;
    const countStr = await env.TOPIC_MAP.get(key);
    const count = parseInt(countStr || "0");

    if (count >= limit) {
        return { allowed: false, remaining: 0 };
    }

    await env.TOPIC_MAP.put(key, String(count + 1), { expirationTtl: window });
    return { allowed: true, remaining: limit - count - 1 };
}

export default {
  async fetch(request, env, ctx) {
    // 鐜鑷
    if (!env.TOPIC_MAP) return new Response("Error: KV 'TOPIC_MAP' not bound.");
    if (!env.BOT_TOKEN) return new Response("Error: BOT_TOKEN not set.");
    if (!env.ADMIN_UID) return new Response("Error: ADMIN_UID not set.");

    // 銆愪慨澶?#7銆戣鑼冨寲鐜鍙橀噺锛岀粺涓€涓哄瓧绗︿覆绫诲瀷
    const normalizedEnv = {
        ...env,
        ADMIN_UID: String(env.ADMIN_UID),
        BOT_TOKEN: String(env.BOT_TOKEN)
    };

    // 楠岃瘉 SUPERGROUP_ID 鏍煎紡
    if (request.method !== "POST") return new Response("OK");

    // 楠岃瘉 Content-Type
    const contentType = request.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
        Logger.warn('invalid_content_type', { contentType });
        return new Response("OK");
    }

    let update;
    try {
      update = await request.json();

      // 楠岃瘉鍩烘湰缁撴瀯
      if (!update || typeof update !== 'object') {
          Logger.warn('invalid_json_structure', { update: typeof update });
          return new Response("OK");
      }
    } catch (e) {
      Logger.error('json_parse_failed', e);
      return new Response("OK");
    }

    if (update.callback_query) {
      await handleCallbackQuery(update.callback_query, normalizedEnv, ctx);
      return new Response("OK");
    }

    const msg = update.message;
    if (!msg) return new Response("OK");

    ctx.waitUntil(flushExpiredMediaGroups(normalizedEnv, Date.now()));

    if (msg.chat && msg.chat.type === "private") {
      try {
        if (String(msg.chat.id) === normalizedEnv.ADMIN_UID) {
            await handleAdminReply(msg, normalizedEnv, ctx);
        } else {
            await handlePrivateMessage(msg, normalizedEnv, ctx);
        }
      } catch (e) {
        const errText = "系统繁忙，请稍后再试。";
        await tgCall(normalizedEnv, "sendMessage", { chat_id: msg.chat.id, text: errText });
        Logger.error('private_message_failed', e, { userId: msg.chat.id });
      }
      return new Response("OK");
    }

    // 銆愪慨澶?#7銆戜娇鐢ㄥ瓧绗︿覆姣旇緝
    return new Response("OK");
  },
};

// ---------------- 鏍稿績涓氬姟閫昏緫 ----------------

async function handlePrivateMessage(msg, env, ctx) {
  const userId = msg.chat.id;
  const key = `user:${userId}`;

  // 閫熺巼闄愬埗妫€鏌?  const rateLimit = await checkRateLimit(userId, env, 'message', CONFIG.RATE_LIMIT_MESSAGE, CONFIG.RATE_LIMIT_WINDOW);
  if (!rateLimit.allowed) {
      await tgCall(env, "sendMessage", {
          chat_id: userId,
          text: "鈿狅笍 鍙戦€佽繃浜庨绻侊紝璇风◢鍚庡啀璇曘€?
      });
      return;
  }

  // 鎷︽埅鏅€氱敤鎴峰彂閫佺殑鎸囦护
  if (msg.text && msg.text.startsWith("/") && msg.text.trim() !== "/start") {
      return;
  }

  const isBanned = await env.TOPIC_MAP.get(`banned:${userId}`);
  if (isBanned) return;

  const verified = await env.TOPIC_MAP.get(`verified:${userId}`);

  if (!verified) {
    const isStart = msg.text && msg.text.trim() === "/start";
    const pendingMsgId = isStart ? null : msg.message_id;
    await sendVerificationChallenge(userId, env, pendingMsgId);
    return;
  }

  await forwardToTopic(msg, userId, key, env, ctx);
}

async function forwardToTopic(msg, userId, key, env, ctx) {
    const needsVerify = await env.TOPIC_MAP.get(`needs_verify:${userId}`);
    if (needsVerify) {
        await sendVerificationChallenge(userId, env, msg.message_id || null);
        return;
    }

    let rec = await safeGetJSON(env, key, null);
    if (!rec) rec = {};

    if (rec.closed) {
        await tgCall(env, "sendMessage", { chat_id: userId, text: "当前对话已被管理员关闭。" });
        return;
    }

    if (!rec.title) {
        rec.title = buildTopicTitle(msg.from || {});
        await env.TOPIC_MAP.put(key, JSON.stringify(rec));
    }

    const res = await tgCall(env, "forwardMessage", {
        chat_id: env.ADMIN_UID,
        from_chat_id: userId,
        message_id: msg.message_id
    });

    if (!res.ok) {
        throw new Error(`forward failed: ${res.description || "unknown"}`);
    }

    const adminMessageId = res.result?.message_id;
    if (adminMessageId !== undefined && adminMessageId !== null) {
        await env.TOPIC_MAP.put(`admin_msg:${adminMessageId}`, String(userId), {
            expirationTtl: CONFIG.ADMIN_MESSAGE_MAP_TTL_SECONDS
        });
    }
}

async function handleAdminReply(msg, env, ctx) {
  const senderId = msg.from?.id;
  if (!senderId || !(await isAdminUser(env, senderId))) return;

  const text = (msg.text || "").trim();
  const userId = await resolveAdminTargetUserId(msg, env);

  if (text === "/cleanup") {
      await tgCall(env, "sendMessage", {
          chat_id: env.ADMIN_UID,
          text: "私聊模式无需 /cleanup。"
      });
      return;
  }

  if (!userId) {
      if (text.startsWith("/")) {
          await tgCall(env, "sendMessage", {
              chat_id: env.ADMIN_UID,
              text: "请回复一条用户消息再执行命令，或使用 /命令 UID。"
          });
      }
      return;
  }

  const key = `user:${userId}`;
  let rec = await safeGetJSON(env, key, null);
  if (!rec) rec = {};

  if (text.startsWith("/close")) {
      rec.closed = true;
      await env.TOPIC_MAP.put(key, JSON.stringify(rec));
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已关闭用户 ${userId} 的会话。` });
      return;
  }

  if (text.startsWith("/open")) {
      rec.closed = false;
      await env.TOPIC_MAP.put(key, JSON.stringify(rec));
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已恢复用户 ${userId} 的会话。` });
      return;
  }

  if (text.startsWith("/reset")) {
      await env.TOPIC_MAP.delete(`verified:${userId}`);
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已重置用户 ${userId} 的验证状态。` });
      return;
  }

  if (text.startsWith("/trust")) {
      await env.TOPIC_MAP.put(`verified:${userId}`, "trusted");
      await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已将用户 ${userId} 设为永久信任。` });
      return;
  }

  if (text.startsWith("/ban")) {
      await env.TOPIC_MAP.put(`banned:${userId}`, "1");
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已封禁用户 ${userId}。` });
      return;
  }

  if (text.startsWith("/unban")) {
      await env.TOPIC_MAP.delete(`banned:${userId}`);
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: `已解封用户 ${userId}。` });
      return;
  }

  if (text.startsWith("/info")) {
      const verifyStatus = await env.TOPIC_MAP.get(`verified:${userId}`);
      const banStatus = await env.TOPIC_MAP.get(`banned:${userId}`);
      const closed = rec.closed ? "是" : "否";
      const info = `UID: ${userId}\n已验证: ${verifyStatus ? (verifyStatus === "trusted" ? "永久信任" : "是") : "否"}\n已封禁: ${banStatus ? "是" : "否"}\n已关闭会话: ${closed}`;
      await tgCall(env, "sendMessage", { chat_id: env.ADMIN_UID, text: info });
      return;
  }

  if (msg.media_group_id) {
    await handleMediaGroup(msg, env, ctx, { direction: "t2p", targetChat: userId, threadId: undefined });
    return;
  }
  await tgCall(env, "copyMessage", { chat_id: userId, from_chat_id: env.ADMIN_UID, message_id: msg.message_id });
}

// ---------------- 楠岃瘉妯″潡 (绾湰鍦? ----------------

async function sendVerificationChallenge(userId, env, pendingMsgId) {
    // 銆愪慨澶?#1銆戞鏌ユ槸鍚﹀凡鏈夎繘琛屼腑鐨勯獙璇?    const existingChallenge = await env.TOPIC_MAP.get(`user_challenge:${userId}`);
    if (existingChallenge) {
        // 鏈夋鍦ㄨ繘琛岀殑楠岃瘉锛氫粎灏嗘柊娑堟伅鍔犲叆寰呭彂閫侀槦鍒楋紝閬垮厤閲嶅涓嬪彂棰樼洰/瑙﹀彂楠岃瘉闄愰€?        const chalKey = `chal:${existingChallenge}`;
        const state = await safeGetJSON(env, chalKey, null);

        // KV 鍙兘瀛樺湪涓嶄竴鑷?杩囨湡锛氳嚜鎰堟竻鐞嗗悗閲嶆柊涓嬪彂
        if (!state || state.userId !== userId) {
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);
        } else {
            if (pendingMsgId) {
                let pendingIds = [];
                if (Array.isArray(state.pending_ids)) {
                    pendingIds = state.pending_ids.slice();
                } else if (state.pending) {
                    pendingIds = [state.pending];
                }

                if (!pendingIds.includes(pendingMsgId)) {
                    pendingIds.push(pendingMsgId);
                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }
                    state.pending_ids = pendingIds;
                    delete state.pending;
                    await env.TOPIC_MAP.put(chalKey, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });
                }
            }
            Logger.debug('verification_duplicate_skipped', { userId, verifyId: existingChallenge, hasPending: !!pendingMsgId });
            return;
        }
    }

    // 楠岃瘉璇锋眰閫熺巼闄愬埗锛氫粎鍦ㄩ渶瑕佸垱寤烘柊鎸戞垬鏃舵鏌?    const verifyLimit = await checkRateLimit(userId, env, 'verify', CONFIG.RATE_LIMIT_VERIFY, 300);
    if (!verifyLimit.allowed) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "鈿狅笍 楠岃瘉璇锋眰杩囦簬棰戠箒锛岃5鍒嗛挓鍚庡啀璇曘€?
        });
        return;
    }

    // 銆愪慨澶?#9銆戜娇鐢ㄥ姞瀵嗗畨鍏ㄧ殑闅忔満鏁?    const q = LOCAL_QUESTIONS[secureRandomInt(0, LOCAL_QUESTIONS.length)];
    const challenge = {
        question: q.question,
        correct: q.correct_answer,
        options: shuffleArray([...q.incorrect_answers, q.correct_answer])
    };

    // 銆愪慨澶?#9銆戜娇鐢ㄥ姞瀵嗗畨鍏ㄧ殑ID鐢熸垚
    const verifyId = secureRandomId(CONFIG.VERIFY_ID_LENGTH);

    // 銆愪慨澶?#6銆戜娇鐢ㄧ瓟妗堢储寮曡€岄潪鏂囨湰锛岄伩鍏嶆埅鏂棶棰?    const answerIndex = challenge.options.indexOf(challenge.correct);

    const state = {
        answerIndex: answerIndex,      // 瀛樺偍绱㈠紩
        options: challenge.options,     // 瀛樺偍瀹屾暣閫夐」鍒楄〃
        pending_ids: pendingMsgId ? [pendingMsgId] : [],
        userId: userId                  // 娣诲姞鐢ㄦ埛ID楠岃瘉
    };

    await env.TOPIC_MAP.put(`chal:${verifyId}`, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });

    // 銆愪慨澶?#1銆戞爣璁扮敤鎴锋鍦ㄩ獙璇佷腑
    await env.TOPIC_MAP.put(`user_challenge:${userId}`, verifyId, { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });

    Logger.info('verification_sent', {
        userId,
        verifyId,
        question: q.question,
        pendingCount: state.pending_ids.length
    });

    // 銆愪慨澶?#6銆戞寜閽娇鐢ㄧ储寮曡€岄潪鏂囨湰
    const buttons = challenge.options.map((opt, idx) => ({
        text: opt,
        callback_data: `verify:${verifyId}:${idx}`  // 浣跨敤绱㈠紩
    }));

    const keyboard = [];
    for (let i = 0; i < buttons.length; i += CONFIG.BUTTON_COLUMNS) {
        keyboard.push(buttons.slice(i, i + CONFIG.BUTTON_COLUMNS));
    }

    await tgCall(env, "sendMessage", {
        chat_id: userId,
        text: `馃洝锔?**浜烘満楠岃瘉**\n\n${challenge.question}\n\n璇风偣鍑讳笅鏂规寜閽洖绛?(鍥炵瓟姝ｇ‘鍚庡皢鑷姩鍙戦€佹偍鍒氭墠鐨勬秷鎭?銆俙,
        parse_mode: "Markdown",
        reply_markup: { inline_keyboard: keyboard }
    });
}

async function handleCallbackQuery(query, env, ctx) {
    try {
        const data = query.data;
        if (!data.startsWith("verify:")) return;

        const parts = data.split(":");
        if (parts.length !== 3) return;

        const verifyId = parts[1];
        const selectedIndex = parseInt(parts[2]);  // 銆愪慨澶?#6銆戠敤鎴烽€夋嫨鐨勭储寮?        const userId = query.from.id;

        const stateStr = await env.TOPIC_MAP.get(`chal:${verifyId}`);
        if (!stateStr) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "鉂?楠岃瘉宸茶繃鏈燂紝璇烽噸鍙戞秷鎭?,
                show_alert: true
            });
            return;
        }

        let state;
        try {
            state = JSON.parse(stateStr);
        } catch(e) {
             await tgCall(env, "answerCallbackQuery", {
                 callback_query_id: query.id,
                 text: "鉂?鏁版嵁閿欒",
                 show_alert: true
             });
             return;
        }

        // 銆愪慨澶?#1銆戦獙璇佺敤鎴稩D鍖归厤
        if (state.userId && state.userId !== userId) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "鉂?鏃犳晥鐨勯獙璇?,
                show_alert: true
            });
            return;
        }

        // 銆愪慨澶?#6銆戦獙璇佺储寮曟湁鏁堟€?        if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= state.options.length) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "鉂?鏃犳晥閫夐」",
                show_alert: true
            });
            return;
        }

        if (selectedIndex === state.answerIndex) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "鉁?楠岃瘉閫氳繃"
            });

            Logger.info('verification_passed', {
                userId,
                verifyId,
                selectedOption: state.options[selectedIndex]
            });

            // 30澶╂湁鏁堟湡 - 浣跨敤閰嶇疆甯搁噺
            await env.TOPIC_MAP.put(`verified:${userId}`, "1", { expirationTtl: CONFIG.VERIFIED_EXPIRE_SECONDS });
            await env.TOPIC_MAP.delete(`needs_verify:${userId}`);

            // 銆愪慨澶?#1銆戞竻鐞嗘墍鏈夌浉鍏虫寫鎴?            await env.TOPIC_MAP.delete(`chal:${verifyId}`);
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);

            await tgCall(env, "editMessageText", {
                chat_id: userId,
                message_id: query.message.message_id,
                text: "鉁?**楠岃瘉鎴愬姛**\n\n鎮ㄧ幇鍦ㄥ彲浠ヨ嚜鐢卞璇濅簡銆?,
                parse_mode: "Markdown"
            });

            const hasPending = (Array.isArray(state.pending_ids) && state.pending_ids.length > 0) || !!state.pending;
            if (hasPending) {
                try {
                    let pendingIds = [];
                    if (Array.isArray(state.pending_ids)) {
                        pendingIds = state.pending_ids.slice();
                    } else if (state.pending) {
                        pendingIds = [state.pending];
                    }

                    // 闄愬埗涓€娆℃€ц浆鍙戦噺锛岄伩鍏嶇敤鎴锋伓鎰忓爢绉鑷存墽琛岃秴鏃?                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }

                    let forwardedCount = 0;
                    for (const pendingId of pendingIds) {
                        if (!pendingId) continue;
                        const forwardedKey = `forwarded:${userId}:${pendingId}`;
                        const alreadyForwarded = await env.TOPIC_MAP.get(forwardedKey);
                        if (alreadyForwarded) {
                            Logger.info('message_forward_duplicate_skipped', { userId, messageId: pendingId });
                            continue;
                        }

                        const fakeMsg = {
                            message_id: pendingId,
                            chat: { id: userId, type: "private" },
                            from: query.from,
                        };

                        await forwardToTopic(fakeMsg, userId, `user:${userId}`, env, ctx);
                        await env.TOPIC_MAP.put(forwardedKey, "1", { expirationTtl: 3600 });
                        forwardedCount++;
                    }

                    if (forwardedCount > 0) {
                        await tgCall(env, "sendMessage", {
                            chat_id: userId,
                            text: `馃摡 鍒氭墠鐨?${forwardedCount} 鏉℃秷鎭凡甯偍閫佽揪銆俙
                        });
                    }
                } catch (e) {
                    Logger.error('pending_message_forward_failed', e, { userId });
                    await tgCall(env, "sendMessage", {
                        chat_id: userId,
                        text: "鈿狅笍 鑷姩鍙戦€佸け璐ワ紝璇烽噸鏂板彂閫佹偍鐨勬秷鎭€?
                    });
                }
            }
        } else {
            Logger.info('verification_failed', {
                userId,
                verifyId,
                selectedIndex,
                correctIndex: state.answerIndex
            });

            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "鉂?绛旀閿欒",
                show_alert: true
            });
        }
    } catch (e) {
        Logger.error('callback_query_error', e, {
            userId: query.from?.id,
            callbackData: query.data
        });
        await tgCall(env, "answerCallbackQuery", {
            callback_query_id: query.id,
            text: `鈿狅笍 绯荤粺閿欒锛岃閲嶈瘯`,
            show_alert: true
        });
    }
}

// ---------------- 杈呭姪鍑芥暟 ----------------

/**
 * 銆愪慨澶?#8銆戞壒閲忔竻鐞嗗懡浠ゅ鐞嗗嚱鏁帮紙浼樺寲骞跺彂鎬ц兘锛? *
 * 鍔熻兘璇存槑锛? * 1. 妫€鏌ユ墍鏈夌敤鎴风殑璇濋璁板綍
 * 2. 鎵惧嚭璇濋ID宸蹭笉瀛樺湪锛堣鍒犻櫎锛夌殑鐢ㄦ埛
 * 3. 鍒犻櫎杩欎簺鐢ㄦ埛鐨凨V瀛樺偍璁板綍鍜岄獙璇佺姸鎬? * 4. 璁╀粬浠笅娆″彂娑堟伅鏃堕噸鏂伴獙璇佸苟鍒涘缓鏂拌瘽棰? *
 * 浣跨敤鍦烘櫙锛? * - 绠＄悊鍛樻墜鍔ㄥ垹闄や簡澶氫釜鐢ㄦ埛璇濋鍚? * - 闇€瑕佹壒閲忛噸缃繖浜涚敤鎴风殑鐘舵€? *
 * @param {number} threadId - 褰撳墠璇濋ID锛堥€氬父鍦℅eneral璇濋涓皟鐢級
 * @param {object} env - 鐜鍙橀噺瀵硅薄
 */
async function handleCleanupCommand(threadId, env) {
    const lockKey = "cleanup:lock";
    const locked = await env.TOPIC_MAP.get(lockKey);
    if (locked) {
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: "鈴?**宸叉湁娓呯悊浠诲姟姝ｅ湪杩愯锛岃绋嶅悗鍐嶈瘯銆?*",
            parse_mode: "Markdown"
        }, threadId));
        return;
    }

    await env.TOPIC_MAP.put(lockKey, "1", { expirationTtl: CONFIG.CLEANUP_LOCK_TTL_SECONDS });

    // 鍙戦€佸鐞嗕腑鐨勬秷鎭?    await tgCall(env, "sendMessage", withMessageThreadId({
        chat_id: env.SUPERGROUP_ID,
        text: "馃攧 **姝ｅ湪鎵弿闇€瑕佹竻鐞嗙殑鐢ㄦ埛...**",
        parse_mode: "Markdown"
    }, threadId));

    let cleanedCount = 0;
    let errorCount = 0;
    const cleanedUsers = [];
    let scannedCount = 0;

    try {
        // 閫愰〉鎵弿锛岄伩鍏嶄竴娆℃€ф媺鍙栧叏閮?keys 瀵艰嚧瓒呮椂/鍐呭瓨鑶ㄨ儉
        let cursor = undefined;
        do {
            const result = await env.TOPIC_MAP.list({ prefix: "user:", cursor });
            const names = (result.keys || []).map(k => k.name);
            scannedCount += names.length;

            // 鎵归噺骞跺彂澶勭悊锛堥檺鍒跺苟鍙戞暟锛?            for (let i = 0; i < names.length; i += CONFIG.CLEANUP_BATCH_SIZE) {
                const batch = names.slice(i, i + CONFIG.CLEANUP_BATCH_SIZE);

                const results = await Promise.allSettled(
                    batch.map(async (name) => {
                        const rec = await safeGetJSON(env, name, null);
                    if (!rec || !rec.thread_id) return null;

                    const userId = name.slice(5);
                    const topicThreadId = rec.thread_id;

                    // 妫€娴嬭瘽棰樻槸鍚﹀瓨鍦細灏濊瘯鍚戣瘽棰樺彂閫佹祴璇曟秷鎭?                    const probe = await probeForumThread(env, topicThreadId, {
                        userId,
                        reason: "cleanup_check",
                        doubleCheckOnMissingThreadId: false
                    });

                    // cleanup 瑕佹眰鏇翠繚瀹堬細浠呭湪鏄庣‘缂哄け/閲嶅畾鍚戞椂娓呯悊锛岄伩鍏嶈鍒犳湁鏁堣褰?                    if (probe.status === "redirected" || probe.status === "missing") {
                            await env.TOPIC_MAP.delete(name);
                            await env.TOPIC_MAP.delete(`verified:${userId}`);
                            await env.TOPIC_MAP.delete(`thread:${topicThreadId}`);

                            return {
                                userId,
                                threadId: topicThreadId,
                                title: rec.title || "鏈煡"
                            };
                    } else if (probe.status === "probe_invalid") {
                        Logger.warn('cleanup_probe_invalid_message', {
                            userId,
                            threadId: topicThreadId,
                            errorDescription: probe.description
                        });
                    } else if (probe.status === "unknown_error") {
                        Logger.warn('cleanup_probe_failed_unknown', {
                            userId,
                            threadId: topicThreadId,
                            errorDescription: probe.description
                        });
                    } else if (probe.status === "missing_thread_id") {
                        Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                    }

                    return null;
                })
            );

            // 澶勭悊缁撴灉
            results.forEach(result => {
                if (result.status === 'fulfilled' && result.value) {
                    cleanedCount++;
                    cleanedUsers.push(result.value);
                    Logger.info('cleanup_user', {
                        userId: result.value.userId,
                        threadId: result.value.threadId
                    });
                } else if (result.status === 'rejected') {
                    errorCount++;
                    Logger.error('cleanup_batch_error', result.reason);
                }
            });

                // 闃叉閫熺巼闄愬埗
                if (i + CONFIG.CLEANUP_BATCH_SIZE < names.length) {
                    await new Promise(r => setTimeout(r, 600));
                }
            }

            cursor = result.list_complete ? undefined : result.cursor;

            // 鍦ㄥ垎椤典箣闂磋鍑烘椂闂寸墖锛岄檷浣庡崟娆℃墽琛屽帇鍔?            if (cursor) {
                await new Promise(r => setTimeout(r, 200));
            }
        } while (cursor);

        // 鐢熸垚骞跺彂閫佹竻鐞嗘姤鍛?        let reportText = `鉁?**娓呯悊瀹屾垚**\n\n`;
        reportText += `馃搳 **缁熻淇℃伅**\n`;
        reportText += `- 鎵弿鐢ㄦ埛鏁? ${scannedCount}\n`;
        reportText += `- 宸叉竻鐞嗙敤鎴锋暟: ${cleanedCount}\n`;
        reportText += `- 閿欒鏁? ${errorCount}\n\n`;

        if (cleanedCount > 0) {
            reportText += `馃棏锔?**宸叉竻鐞嗙殑鐢ㄦ埛** (璇濋宸插垹闄?:\n`;
            for (const user of cleanedUsers.slice(0, CONFIG.MAX_CLEANUP_DISPLAY)) {
                reportText += `- UID: \`${user.userId}\` | 璇濋: ${user.title}\n`;
            }
            if (cleanedUsers.length > CONFIG.MAX_CLEANUP_DISPLAY) {
                reportText += `\n...(杩樻湁 ${cleanedUsers.length - CONFIG.MAX_CLEANUP_DISPLAY} 涓敤鎴?\n`;
            }
            reportText += `\n馃挕 杩欎簺鐢ㄦ埛涓嬫鍙戞秷鎭椂灏嗛噸鏂拌繘琛屼汉鏈洪獙璇佸苟鍒涘缓鏂拌瘽棰樸€俙;
        } else {
            reportText += `鉁?娌℃湁鍙戠幇闇€瑕佹竻鐞嗙殑鐢ㄦ埛璁板綍銆俙;
        }

        Logger.info('cleanup_completed', {
            cleanedCount,
            errorCount,
            totalUsers: scannedCount
        });

        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: reportText,
            parse_mode: "Markdown"
        }, threadId));

    } catch (e) {
        Logger.error('cleanup_failed', e, { threadId });
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `鉂?**娓呯悊杩囩▼鍑洪敊**\n\n閿欒淇℃伅: \`${e.message}\``,
            parse_mode: "Markdown"
        }, threadId));
    } finally {
        await env.TOPIC_MAP.delete(lockKey);
    }
}

// ---------------- 鍏朵粬杈呭姪鍑芥暟 ----------------

// 涓鸿瘽棰樺缓绔?thread->user 鏄犲皠锛岄伩鍏嶇鐞嗗憳鍛戒护鏃跺叏閲?KV 鍙嶆煡
async function createTopic(from, key, env, userId) {
    const title = buildTopicTitle(from);
    if (!env.SUPERGROUP_ID.toString().startsWith("-100")) throw new Error("SUPERGROUP_ID蹇呴』浠?100寮€澶?);
    const res = await tgCall(env, "createForumTopic", { chat_id: env.SUPERGROUP_ID, name: title });
    if (!res.ok) throw new Error(`鍒涘缓璇濋澶辫触: ${res.description}`);
    const rec = { thread_id: res.result.message_thread_id, title, closed: false };
    await env.TOPIC_MAP.put(key, JSON.stringify(rec));
    if (userId) {
        await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
    }
    return rec;
}

// 銆愪慨澶?#2銆戞洿鏂拌瘽棰樼姸鎬?- 淇寮傛鎿嶄綔鏈瓑寰?async function updateThreadStatus(threadId, isClosed, env) {
    try {
        const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
        if (mappedUser) {
            const userKey = `user:${mappedUser}`;
            const rec = await safeGetJSON(env, userKey, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                await env.TOPIC_MAP.put(userKey, JSON.stringify(rec));
                Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                return;
            }

            // 鏄犲皠澶辨晥锛氭竻鐞嗗悗闄嶇骇鍏ㄩ噺鎵弿
            await env.TOPIC_MAP.delete(`thread:${threadId}`);
        }

        const allKeys = await getAllKeys(env, "user:");
        const updates = [];

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                updates.push(env.TOPIC_MAP.put(name, JSON.stringify(rec)));
            }
        }

        await Promise.all(updates);
        Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: updates.length });
    } catch (e) {
        Logger.error('thread_status_update_failed', e, { threadId, isClosed });
        throw e;
    }
}

// 鏀硅繘鐨勮瘽棰樻爣棰樻瀯寤猴紙娓呯悊鐗规畩瀛楃锛?function buildTopicTitle(from) {
  const firstName = (from.first_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);
  const lastName = (from.last_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);

  // 娓呯悊 username
  let username = "";
  if (from.username) {
      username = from.username
          .replace(/[^\w]/g, '')  // 鍙繚鐣欏瓧姣嶆暟瀛椾笅鍒掔嚎
          .substring(0, 20);
  }

  // 绉婚櫎鎺у埗瀛楃鍜屾崲琛岀
  const cleanName = (firstName + " " + lastName)
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
      .replace(/\s+/g, ' ')
      .trim();

  const name = cleanName || "User";
  const usernameStr = username ? ` @${username}` : "";

  // Telegram 璇濋鏍囬鏈€澶ч暱搴︿负 128 瀛楃
  const title = (name + usernameStr).substring(0, CONFIG.MAX_TITLE_LENGTH);

  return title;
}

// 鏀硅繘鐨?Telegram API 璋冪敤锛堟坊鍔犺秴鏃跺拰 HTTPS 寮哄埗锛?async function tgCall(env, method, body, timeout = CONFIG.API_TIMEOUT_MS) {
  let base = env.API_BASE || "https://api.telegram.org";

  // 銆愪慨澶?#20銆戝己鍒?HTTPS
  if (base.startsWith("http://")) {
      Logger.warn('api_http_upgraded', { originalBase: base });
      base = base.replace("http://", "https://");
  }

  // 楠岃瘉 URL 鏍煎紡
  try {
      new URL(`${base}/test`);
  } catch (e) {
      Logger.error('api_base_invalid', e, { base });
      base = "https://api.telegram.org";
  }

  // 銆愪慨澶?#13銆戞坊鍔犺秴鏃舵帶鍒?  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
      const resp = await fetch(`${base}/bot${env.BOT_TOKEN}/${method}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
          signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!resp.ok && resp.status >= 500) {
          Logger.warn('telegram_api_server_error', {
              method,
              status: resp.status
          });
      }

      const result = await resp.json();

      // 璁板綍閫熺巼闄愬埗
      if (!result.ok && result.description && result.description.includes('Too Many Requests')) {
          const retryAfter = result.parameters?.retry_after || 5;
          Logger.warn('telegram_api_rate_limit', {
              method,
              retryAfter
          });
      }

      return result;
  } catch (e) {
      clearTimeout(timeoutId);

      if (e.name === 'AbortError') {
          Logger.error('telegram_api_timeout', e, { method, timeout });
          return { ok: false, description: 'Request timeout' };
      }

      Logger.error('telegram_api_failed', e, { method });
      throw e;
  }
}

async function handleMediaGroup(msg, env, ctx, { direction, targetChat, threadId }) {
    const groupId = msg.media_group_id;
    const key = `mg:${direction}:${groupId}`;
    const item = extractMedia(msg);
    if (!item) {
        await tgCall(env, "copyMessage", withMessageThreadId({
            chat_id: targetChat,
            from_chat_id: msg.chat.id,
            message_id: msg.message_id
        }, threadId));
        return;
    }
    let rec = await safeGetJSON(env, key, null);
    if (!rec) rec = { direction, targetChat, threadId: (threadId === null ? undefined : threadId), items: [], last_ts: Date.now() };
    rec.items.push({ ...item, msg_id: msg.message_id });
    rec.last_ts = Date.now();
    await env.TOPIC_MAP.put(key, JSON.stringify(rec), { expirationTtl: CONFIG.MEDIA_GROUP_EXPIRE_SECONDS });
    ctx.waitUntil(delaySend(env, key, rec.last_ts));
}

// 銆愪慨澶?#15, #19銆戞敼杩涚殑濯掍綋鎻愬彇锛堟敮鎸佹洿澶氱被鍨嬶紝涓嶄慨鏀瑰師鏁扮粍锛?function extractMedia(msg) {
    // 鍥剧墖
    if (msg.photo && msg.photo.length > 0) {
        const highestResolution = msg.photo[msg.photo.length - 1];  // 涓嶄娇鐢?pop()
        return {
            type: "photo",
            id: highestResolution.file_id,
            cap: msg.caption || ""
        };
    }

    // 瑙嗛
    if (msg.video) {
        return {
            type: "video",
            id: msg.video.file_id,
            cap: msg.caption || ""
        };
    }

    // 鏂囨。
    if (msg.document) {
        return {
            type: "document",
            id: msg.document.file_id,
            cap: msg.caption || ""
        };
    }

    // 闊抽
    if (msg.audio) {
        return {
            type: "audio",
            id: msg.audio.file_id,
            cap: msg.caption || ""
        };
    }

    // 鍔ㄥ浘
    if (msg.animation) {
        return {
            type: "animation",
            id: msg.animation.file_id,
            cap: msg.caption || ""
        };
    }

    // 璇煶鍜岃棰戞秷鎭笉鏀寔 media group
    return null;
}

// 銆愪慨澶?#21銆戝疄鐜板獟浣撶粍娓呯悊
async function flushExpiredMediaGroups(env, now) {
    try {
        const prefix = "mg:";
        const allKeys = await getAllKeys(env, prefix);
        let deletedCount = 0;

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && rec.last_ts && (now - rec.last_ts > 300000)) { // 瓒呰繃 5 鍒嗛挓
                await env.TOPIC_MAP.delete(name);
                deletedCount++;
            }
        }

        if (deletedCount > 0) {
            Logger.info('media_groups_cleaned', { deletedCount });
        }
    } catch (e) {
        Logger.error('media_group_cleanup_failed', e);
    }
}

// 銆愪慨澶?#12, #28銆戞敼杩涘獟浣撶粍寤惰繜鍙戦€?async function delaySend(env, key, ts) {
    await new Promise(r => setTimeout(r, CONFIG.MEDIA_GROUP_DELAY_MS));

    const rec = await safeGetJSON(env, key, null);

    if (rec && rec.last_ts === ts) {
        // 楠岃瘉濯掍綋鏁扮粍
        if (!rec.items || rec.items.length === 0) {
            Logger.warn('media_group_empty', { key });
            await env.TOPIC_MAP.delete(key);
            return;
        }

        const media = rec.items.map((it, i) => {
            if (!it.type || !it.id) {
                Logger.warn('media_group_invalid_item', { key, item: it });
                return null;
            }
            // 銆愪慨澶?#28銆戦檺鍒?caption 闀垮害
            const caption = i === 0 ? (it.cap || "").substring(0, 1024) : "";
            return { 
                type: it.type,
                media: it.id,
                caption
            };
        }).filter(Boolean);  // 杩囨护鎺夋棤鏁堥」

        if (media.length > 0) {
            try {
                const result = await tgCall(env, "sendMediaGroup", withMessageThreadId({
                    chat_id: rec.targetChat,
                    media
                }, rec.threadId));

                if (!result.ok) {
                    Logger.error('media_group_send_failed', result.description, {
                        key,
                        mediaCount: media.length
                    });
                } else {
                    Logger.info('media_group_sent', {
                        key,
                        mediaCount: media.length,
                        targetChat: rec.targetChat
                    });
                }
            } catch (e) {
                Logger.error('media_group_send_exception', e, { key });
            }
        }

        await env.TOPIC_MAP.delete(key);
    }
}
