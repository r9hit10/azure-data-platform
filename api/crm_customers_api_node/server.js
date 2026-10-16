/*
 Runs the FinServe CRM API on your laptop (no Azure Function needed).
   node server.js        ->  http://localhost:7071/api/customers  and  /api/customers/meta
 ADF reaches it through the Self-hosted Integration Runtime on this same machine.
 Keep this window open while ADF pipelines run.
*/
const http = require("http");
const crm = require("./shared/crm");
const PORT = process.env.PORT || 7071;

http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const q = Object.fromEntries(u.searchParams);
  let status = 200, body;
  try {
    if (u.pathname === "/api/customers/meta") body = crm.meta(q);
    else if (u.pathname === "/api/customers") body = crm.page(q, `http://${req.headers.host}${u.pathname}`);
    else { status = 404; body = { error: "use /api/customers or /api/customers/meta" }; }
  } catch (e) { status = 400; body = { error: String(e) }; }
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
  const info = body && body.record_count !== undefined ? `records=${body.record_count}` :
               body && body.page ? `page ${body.page}/${body.total_pages} rows=${body.data.length}` : "";
  console.log(`${new Date().toISOString()}  ${status}  ${u.pathname}  ${info}`);
}).listen(PORT, () => console.log(`FinServe CRM API running on http://localhost:${PORT}/api/customers  (Ctrl+C to stop)`));
