/*
 FinServe CRM - Customers REST API (Azure Functions, Node.js, no npm dependencies)

   GET /api/customers/meta?modified_since=<ts>
       -> { modified_since, record_count, max_modified }      (ADF uses this as the "new watermark" lookup)
   GET /api/customers?modified_since=<ts>&modified_until=<ts>&page=1&page_size=5000
       -> { page, page_size, total_records, total_pages, next_page, data: [...] }
          next_page is an absolute URL (null on the last page) -> ADF pagination rule  AbsoluteUrl = $.next_page

 Data = customers_raw.csv shipped in the package (duplicates + dirty values kept on purpose)
 PLUS simulated CRM activity: every hour since LAUNCH, 50 customers change city/email and 10 new customers are created,
 so every incremental ADF run finds genuinely new / changed records. Deterministic: same answer on every call.
*/
const fs = require("fs");
const path = require("path");

const LAUNCH = Date.UTC(2026, 9, 16, 0, 0, 0);          // 2026-10-16 00:00 UTC
const UPDATES_PER_HOUR = 50, NEW_PER_HOUR = 10;
const FIELDS = ["CustomerID", "FirstName", "LastName", "Email", "Phone", "City", "State", "CreatedDate", "ModifiedDate", "Status"];
const CITIES = [["Navi Mumbai", "Maharashtra"], ["Thane", "Maharashtra"], ["Pune", "Maharashtra"], ["Austin", "Texas"]];
let BASE = null;

function parseTs(s) {
  s = (s || "").trim().replace("T", " ").replace("Z", "");
  if (!s) return Date.UTC(1900, 0, 1);
  if (s.length === 10) s += " 00:00:00";
  return Date.parse(s.slice(0, 19).replace(" ", "T") + "Z");
}
const pad = (n) => String(n).padStart(2, "0");
function fmt(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
function rng(seed) {                                     // mulberry32: small deterministic PRNG
  let a = seed >>> 0;
  return () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}

function base() {
  if (BASE) return BASE;
  const text = fs.readFileSync(path.join(__dirname, "..", "customers_raw.csv"), "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.length);
  const header = lines[0].split(",");
  BASE = lines.slice(1).map((line) => {
    const v = line.split(",");                           // file has no quoted commas
    const r = {}; header.forEach((h, i) => (r[h] = v[i] ?? ""));
    return [parseTs(r.ModifiedDate), r];
  });
  return BASE;
}

function currentRecords(now) {
  const b = base();
  const hours = Math.max(0, Math.floor((now - LAUNCH) / 3600000));
  const byId = new Map();
  for (const [, r] of b) if (!byId.has(r.CustomerID)) byId.set(r.CustomerID, r);
  const latest = new Map();                              // CustomerID -> [modifiedMs, record]
  for (let h = 1; h <= hours; h++) {
    const ts = LAUNCH + h * 3600000, rand = rng(h);
    for (let k = 0; k < UPDATES_PER_HOUR; k++) {
      const cid = String(1 + Math.floor(rand() * 100000));
      const src = latest.has(cid) ? latest.get(cid)[1] : byId.get(cid);
      if (!src) continue;
      const [city, state] = CITIES[Math.floor(rand() * CITIES.length)];
      latest.set(cid, [ts, { ...src, City: city, State: state, ModifiedDate: fmt(ts),
        Email: `${src.FirstName.trim().toLowerCase()}.${cid}@finservemail.com` }]);
    }
    for (let k = 0; k < NEW_PER_HOUR; k++) {
      const cid = String(100000 + (h - 1) * NEW_PER_HOUR + k + 1);
      latest.set(cid, [ts, { CustomerID: cid,
        FirstName: ["Aarav", "Priya", "Rohan", " sneha ", "Vikram"][Math.floor(rand() * 5)],
        LastName: ["Sharma", "PATEL", "Iyer", "Nair"][Math.floor(rand() * 4)],
        Email: k === 0 ? "" : `new${cid}@finservemail.com`,
        Phone: `98${10000000 + Math.floor(rand() * 89999999)}`, City: "Mumbai", State: "Maharashtra",
        CreatedDate: fmt(ts), ModifiedDate: fmt(ts), Status: ["Active", "ACTIVE", "A"][Math.floor(rand() * 3)] }]);
    }
  }
  const out = b.filter(([, r]) => !latest.has(r.CustomerID));   // unchanged base rows (incl. duplicates)
  for (const v of latest.values()) out.push(v);
  return out;
}

function window(query) {
  const since = parseTs(query.modified_since || "1900-01-01");
  const now = Date.now();
  const until = query.modified_until ? Math.min(parseTs(query.modified_until), now) : now;
  return { since, until, now };
}

function meta(query) {
  const { since, until, now } = window(query);
  let n = 0, max = null;
  for (const [m] of currentRecords(now)) if (m > since && m <= until) { n++; if (max === null || m > max) max = m; }
  return { modified_since: fmt(since), record_count: n, max_modified: max === null ? null : fmt(max) + ".000" };
}

function page(query, url) {
  const { since, until, now } = window(query);
  const p = Math.max(1, parseInt(query.page || "1", 10));
  const size = Math.min(10000, Math.max(1, parseInt(query.page_size || "5000", 10)));
  const rows = currentRecords(now).filter(([m]) => m > since && m <= until)
    .sort((a, b) => a[0] - b[0] || Number(a[1].CustomerID) - Number(b[1].CustomerID));   // stable order -> stable pages
  const total = rows.length, pages = Math.max(1, Math.ceil(total / size));
  const data = rows.slice((p - 1) * size, p * size).map(([, r]) => Object.fromEntries(FIELDS.map((f) => [f, r[f] ?? ""])));
  let next = null;
  if (p < pages) {
    const baseUrl = url.split("?")[0];
    next = `${baseUrl}?modified_since=${encodeURIComponent(fmt(since))}&modified_until=${encodeURIComponent(fmt(until))}&page=${p + 1}&page_size=${size}`;
  }
  return { page: p, page_size: size, total_records: total, total_pages: pages, next_page: next, data };
}

function respond(context, fn) {
  try { context.res = { status: 200, headers: { "Content-Type": "application/json" }, body: fn() }; }
  catch (e) { context.res = { status: 400, headers: { "Content-Type": "application/json" }, body: { error: String(e) } }; }
}

module.exports = { meta, page, respond };
