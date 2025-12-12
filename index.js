import express from "express";
import axios from "axios";
import cron from "node-cron";
import Database from "better-sqlite3";
import { z } from "zod";

const app = express();
app.use(express.json({ limit: "1mb" }));

/**
 * =========================
 * CONFIGURATION
 * =========================
 */

const CURRENT_BASE_URL = "https://api.current-rms.com/api/v1";
const CURRENT_SUBDOMAIN = process.env.CURRENT_SUBDOMAIN;
const CURRENT_API_KEY = process.env.CURRENT_API_KEY;

/**
 * =========================
 * SIMPLE LOCAL DATABASE
 * (used to remember what we already sent)
 * =========================
 */

const db = new Database("state.db");

db.exec(`
  CREATE TABLE IF NOT EXISTS hooks (
    event TEXT PRIMARY KEY,
    url TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS cursors (
    name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS deliveries (
    idempotency_key TEXT PRIMARY KEY,
    delivered_at TEXT NOT NULL
  );
`);

/**
 * =========================
 * HELPER FUNCTIONS
 * =========================
 */

function getHook(event) {
  const row = db
    .prepare("SELECT url FROM hooks WHERE event = ?")
    .get(event);
  return row?.url || null;
}

function setHook(event, url) {
  db.prepare(`
    INSERT INTO hooks (event, url)
    VALUES (?, ?)
    ON CONFLICT(event) DO UPDATE SET url = excluded.url
  `).run(event, url);
}

function getCursor(name, fallbackISO) {
  const row = db
    .prepare("SELECT cursor FROM cursors WHERE name = ?")
    .get(name);
  return row?.cursor || fallbackISO;
}

function setCursor(name, cursorISO) {
  db.prepare(`
    INSERT INTO cursors (name, cursor)
    VALUES (?, ?)
    ON CONFLICT(name) DO UPDATE SET cursor = excluded.cursor
  `).run(name, cursorISO);
}

function alreadyDelivered(key) {
  return !!db
    .prepare("SELECT 1 FROM deliveries WHERE idempotency_key = ?")
    .get(key);
}

function markDelivered(key, iso) {
  db.prepare(`
    INSERT OR IGNORE INTO deliveries (idempotency_key, delivered_at)
    VALUES (?, ?)
  `).run(key, iso);
}

/**
 * =========================
 * CURRENT RMS API CLIENT
 * =========================
 */

function currentClient() {
  if (!CURRENT_SUBDOMAIN || !CURRENT_API_KEY) {
    throw new Error("Missing CURRENT_SUBDOMAIN or CURRENT_API_KEY");
  }

  return axios.create({
    baseURL: CURRENT_BASE_URL,
    headers: {
      "X-SUBDOMAIN": CURRENT_SUBDOMAIN,
      "X-AUTH-TOKEN": CURRENT_API_KEY,
      "Accept": "application/json"
    },
    timeout: 30000
  });
}

/**
 * =========================
 * FILTER BUILDER
 * NOTE:
 * You may need to adjust this depending on
 * your Current RMS search syntax.
 * =========================
 */

function buildUpdatedSinceParams(updatedSinceISO) {
  return {
    // Try this first. If no results come back,
    // change this to match Current RMS filtering syntax.
    updated_since: updatedSinceISO,
    per_page: 100
  };
}

/**
 * =========================
 * SEND TO ZAPIER
 * =========================
 */

async function postToZapier(eventName, payload) {
  const hookUrl = getHook(eventName);
  if (!hookUrl) return;

  await axios.post(hookUrl, payload, { timeout: 30000 });
}

/**
 * =========================
 * POLLER: OPPORTUNITIES
 * =========================
 */

async function pollOpportunities() {
  const client = currentClient();

  const cursorName = "opportunities.updated_at";

  // Default: start 5 minutes ago
  const since = getCursor(
    cursorName,
    new Date(Date.now() - 5 * 60 * 1000).toISOString()
  );

  const params = buildUpdatedSinceParams(since);

  const response = await client.get("/opportunities", { params });

  const opportunities =
    response.data?.opportunities ||
    response.data?.data ||
    response.data ||
    [];

  let newestTimestamp = since;

  for (const opp of opportunities) {
    const updatedAt =
      opp.updated_at || opp.updatedAt || opp.updated;

    if (updatedAt && updatedAt > newestTimestamp) {
      newestTimestamp = updatedAt;
    }

    const occurredAt = updatedAt || new Date().toISOString();
    const idempotencyKey = `opportunity:${opp.id}:${occurredAt}`;

    if (alreadyDelivered(idempotencyKey)) continue;

    const payload = {
      source: "current-rms",
      event: "opportunity.updated",
      occurred_at: occurredAt,
      idempotency_key: idempotencyKey,
      data: opp
    };

    await postToZapier("opportunity.updated", payload);
    markDelivered(idempotencyKey, new Date().toISOString());
  }

  setCursor(cursorName, newestTimestamp);
}

/**
 * =========================
 * SCHEDULE (RUNS EVERY MINUTE)
 * =========================
 */

cron.schedule("* * * * *", async () => {
  try {
    await pollOpportunities();
  } catch (error) {
    console.error("Polling error:", error.message);
  }
});

/**
 * =========================
 * HTTP ENDPOINTS
 * =========================
 */

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.post("/hooks/register", (req, res) => {
  const schema = z.object({
    event: z.string(),
    url: z.string().url()
  });

  const result = schema.safeParse(req.body);

  if (!result.success) {
    return res.status(400).json({ error: "Invalid request body" });
  }

  setHook(result.data.event, result.data.url);
  res.json({ ok: true });
});

/**
 * =========================
 * START SERVER
 * =========================
 */

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Middleware running on port ${PORT}`);
});
