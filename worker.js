const CONFIG = {
  VERIFY_TTL_SECONDS: 300,
  VERIFIED_TTL_SECONDS: 2592000,
  ADMIN_MESSAGE_MAP_TTL_SECONDS: 2592000,
};

const LOCAL_QUESTIONS = [
  { question: "冰融化后会变成什么？", correct_answer: "水", incorrect_answers: ["石头", "木头", "火"] },
  { question: "正常人有几只眼睛？", correct_answer: "2", incorrect_answers: ["1", "3", "4"] },
  { question: "以下哪个属于水果？", correct_answer: "香蕉", incorrect_answers: ["白菜", "猪肉", "大米"] },
  { question: "1 + 2 等于几？", correct_answer: "3", incorrect_answers: ["2", "4", "5"] },
  { question: "5 - 2 等于几？", correct_answer: "3", incorrect_answers: ["1", "2", "4"] },
  { question: "2 × 3 等于几？", correct_answer: "6", incorrect_answers: ["4", "5", "7"] },
  { question: "10 + 5 等于几？", correct_answer: "15", incorrect_answers: ["10", "12", "20"] },
  { question: "8 - 4 等于几？", correct_answer: "4", incorrect_answers: ["2", "3", "5"] },
  { question: "在天上飞的交通工具是什么？", correct_answer: "飞机", incorrect_answers: ["汽车", "轮船", "自行车"] },
  { question: "星期一后面是星期几？", correct_answer: "星期二", incorrect_answers: ["星期日", "星期三", "星期一"] },
  { question: "鱼通常生活在哪里？", correct_answer: "水里", incorrect_answers: ["树上", "土里", "火里"] },
  { question: "我们用什么器官来听声音？", correct_answer: "耳朵", incorrect_answers: ["眼睛", "鼻子", "嘴巴"] },
  { question: "晴朗的天空通常是什么颜色？", correct_answer: "蓝色", incorrect_answers: ["绿色", "红色", "紫色"] },
  { question: "太阳从哪个方向升起？", correct_answer: "东方", incorrect_answers: ["西方", "南方", "北方"] },
  { question: "小狗发出的叫声通常是？", correct_answer: "汪汪", incorrect_answers: ["喵喵", "咩咩", "呱呱"] },
];

export default {
  async fetch(request, env) {
    if (!env.BOT_TOKEN) return new Response("BOT_TOKEN missing", { status: 500 });
    if (!env.ADMIN_UID) return new Response("ADMIN_UID missing", { status: 500 });
    if (!env.TOPIC_MAP) return new Response("TOPIC_MAP missing", { status: 500 });
    if (request.method !== "POST") return new Response("OK");

    let update;
    try {
      update = await request.json();
    } catch {
      return new Response("OK");
    }

    if (update.callback_query) {
      await handleCallback(update.callback_query, env);
      return new Response("OK");
    }

    const msg = update.message;
    if (!msg || msg.chat?.type !== "private") return new Response("OK");

    const senderId = String(msg.from?.id || msg.chat.id);
    if (senderId === String(env.ADMIN_UID)) {
      await handleAdminMessage(msg, env);
    } else {
      await handleUserMessage(msg, env);
    }

    return new Response("OK");
  },
};

async function handleUserMessage(msg, env) {
  const userId = String(msg.chat.id);
  const text = (msg.text || "").trim();

  if (text.startsWith("/") && text !== "/start") return;

  const banned = await env.TOPIC_MAP.get(`banned:${userId}`);
  if (banned === "1") return;

  const closed = await env.TOPIC_MAP.get(`closed:${userId}`);
  if (closed === "1") {
    await tgCall(env, "sendMessage", { chat_id: Number(userId), text: "当前会话已被管理员关闭。" });
    return;
  }

  const verified = await env.TOPIC_MAP.get(`verified:${userId}`);
  if (!verified) {
    await sendOrUpdateChallenge(userId, text === "/start" ? null : msg.message_id, env);
    return;
  }

  if (text === "/start") {
    await tgCall(env, "sendMessage", { chat_id: Number(userId), text: "已连接客服，请直接发送消息。" });
    return;
  }

  await forwardUserMessageToAdmin(msg, env);
}

async function sendOrUpdateChallenge(userId, pendingMsgId, env) {
  const key = `challenge:${userId}`;
  const existingStr = await env.TOPIC_MAP.get(key);

  if (existingStr) {
    try {
      const existing = JSON.parse(existingStr);
      if (pendingMsgId) existing.pending_msg_id = pendingMsgId;
      await env.TOPIC_MAP.put(key, JSON.stringify(existing), { expirationTtl: CONFIG.VERIFY_TTL_SECONDS });
      return;
    } catch {
      // recreate
    }
  }

  const item = LOCAL_QUESTIONS[Math.floor(Math.random() * LOCAL_QUESTIONS.length)];
  const options = shuffle([item.correct_answer, ...item.incorrect_answers]);
  const answerIndex = options.findIndex((x) => x === item.correct_answer);

  const state = { answerIndex, options, pending_msg_id: pendingMsgId || null };
  await env.TOPIC_MAP.put(key, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_TTL_SECONDS });

  const keyboard = options.map((t, i) => [{ text: t, callback_data: `verify:${userId}:${i}` }]);
  await tgCall(env, "sendMessage", {
    chat_id: Number(userId),
    text: `🛡️ 人机验证\n\n${item.question}\n\n请点击正确答案（5分钟内有效）`,
    reply_markup: { inline_keyboard: keyboard },
  });
}

async function handleCallback(query, env) {
  const data = query.data || "";
  if (!data.startsWith("verify:")) {
    await tgCall(env, "answerCallbackQuery", { callback_query_id: query.id });
    return;
  }

  const parts = data.split(":");
  if (parts.length !== 3) return;

  const userId = String(query.from?.id || "");
  const dataUserId = parts[1];
  const selected = Number(parts[2]);

  if (userId !== dataUserId) {
    await tgCall(env, "answerCallbackQuery", {
      callback_query_id: query.id,
      text: "这不是你的验证按钮。",
      show_alert: true,
    });
    return;
  }

  const stateStr = await env.TOPIC_MAP.get(`challenge:${userId}`);
  if (!stateStr) {
    await tgCall(env, "answerCallbackQuery", {
      callback_query_id: query.id,
      text: "验证已过期，请重新发送消息。",
      show_alert: true,
    });
    return;
  }

  let state;
  try {
    state = JSON.parse(stateStr);
  } catch {
    await env.TOPIC_MAP.delete(`challenge:${userId}`);
    await tgCall(env, "answerCallbackQuery", {
      callback_query_id: query.id,
      text: "验证状态异常，请重试。",
      show_alert: true,
    });
    return;
  }

  if (selected !== state.answerIndex) {
    await env.TOPIC_MAP.delete(`challenge:${userId}`);
    await tgCall(env, "answerCallbackQuery", {
      callback_query_id: query.id,
      text: "答案错误，请重新发送消息触发验证。",
      show_alert: true,
    });
    return;
  }

  await env.TOPIC_MAP.put(`verified:${userId}`, "1", { expirationTtl: CONFIG.VERIFIED_TTL_SECONDS });
  await env.TOPIC_MAP.delete(`challenge:${userId}`);

  await tgCall(env, "answerCallbackQuery", { callback_query_id: query.id, text: "验证通过。" });
  if (query.message?.chat?.id && query.message?.message_id) {
    await tgCall(env, "editMessageText", {
      chat_id: query.message.chat.id,
      message_id: query.message.message_id,
      text: "✅ 验证通过。现在可以继续发送消息。",
    });
  }

  if (state.pending_msg_id) {
    await forwardUserMessageToAdmin({ chat: { id: Number(userId) }, message_id: state.pending_msg_id }, env);
  }
}

async function forwardUserMessageToAdmin(msg, env) {
  const userId = String(msg.chat.id);
  const res = await tgCall(env, "forwardMessage", {
    chat_id: Number(env.ADMIN_UID),
    from_chat_id: Number(userId),
    message_id: msg.message_id,
  });

  if (res.ok && res.result?.message_id) {
    await env.TOPIC_MAP.put(`admin_msg:${res.result.message_id}`, userId, {
      expirationTtl: CONFIG.ADMIN_MESSAGE_MAP_TTL_SECONDS,
    });
  }
}

async function handleAdminMessage(msg, env) {
  const text = (msg.text || "").trim();
  const targetId = (await resolveTargetUserId(msg, env)) || parseUidFromCommand(text);

  if (text.startsWith("/")) {
    await handleAdminCommand(text, targetId, env, msg.chat.id);
    return;
  }

  if (!targetId) {
    await tgCall(env, "sendMessage", {
      chat_id: Number(env.ADMIN_UID),
      text: "请回复一条用户消息，或使用 /命令 <uid>。",
    });
    return;
  }

  await tgCall(env, "copyMessage", {
    chat_id: Number(targetId),
    from_chat_id: Number(env.ADMIN_UID),
    message_id: msg.message_id,
  });
}

async function handleAdminCommand(text, uid, env, adminChatId) {
  const cmd = text.split(/\s+/)[0].toLowerCase();
  if (!uid && cmd !== "/help") {
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: "需要目标 UID。" });
    return;
  }

  if (cmd === "/ban") {
    await env.TOPIC_MAP.put(`banned:${uid}`, "1");
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: `已封禁 ${uid}` });
    return;
  }
  if (cmd === "/unban") {
    await env.TOPIC_MAP.delete(`banned:${uid}`);
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: `已解封 ${uid}` });
    return;
  }
  if (cmd === "/close") {
    await env.TOPIC_MAP.put(`closed:${uid}`, "1");
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: `已关闭会话 ${uid}` });
    return;
  }
  if (cmd === "/open") {
    await env.TOPIC_MAP.delete(`closed:${uid}`);
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: `已恢复会话 ${uid}` });
    return;
  }
  if (cmd === "/reset") {
    await env.TOPIC_MAP.delete(`verified:${uid}`);
    await env.TOPIC_MAP.delete(`challenge:${uid}`);
    await tgCall(env, "sendMessage", { chat_id: Number(adminChatId), text: `已重置验证 ${uid}` });
    return;
  }
  if (cmd === "/info") {
    const banned = (await env.TOPIC_MAP.get(`banned:${uid}`)) === "1";
    const closed = (await env.TOPIC_MAP.get(`closed:${uid}`)) === "1";
    const verified = (await env.TOPIC_MAP.get(`verified:${uid}`)) ? "yes" : "no";
    await tgCall(env, "sendMessage", {
      chat_id: Number(adminChatId),
      text: `UID: ${uid}\nVerified: ${verified}\nBanned: ${banned ? "yes" : "no"}\nClosed: ${closed ? "yes" : "no"}`,
    });
    return;
  }

  await tgCall(env, "sendMessage", {
    chat_id: Number(adminChatId),
    text: "命令: /ban /unban /close /open /reset /info（回复消息或加 <uid>）",
  });
}

function parseUidFromCommand(text) {
  const p = (text || "").trim().split(/\s+/);
  if (p.length < 2) return null;
  const n = Number(p[1]);
  return Number.isFinite(n) ? String(Math.trunc(n)) : null;
}

async function resolveTargetUserId(msg, env) {
  const replyId = msg.reply_to_message?.message_id;
  if (!replyId) return null;
  return await env.TOPIC_MAP.get(`admin_msg:${replyId}`);
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function tgCall(env, method, body) {
  const url = `https://api.telegram.org/bot${env.BOT_TOKEN}/${method}`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  try {
    return await resp.json();
  } catch {
    return { ok: false, description: "invalid json response" };
  }
}
