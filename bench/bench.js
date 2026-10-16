// Latency overhead of a gateway vs calling the upstream directly. Same client, same upstream, same network.
// Add another gateway by adding an entry to TARGETS (any OpenAI-compatible endpoint + key).
const http = require('http');
const N = +process.env.N || 5000, WARM = 300;
const TARGETS = [
  { name: 'DIRECT ', host: 'mock', port: 9102, headers: {}, model: 'x' },
  { name: 'NEXORA ', host: 'nexora', port: 8080, headers: { authorization: 'Bearer nx_live_bench' }, model: 'fast' },
  ...(process.env.EXTRA ? JSON.parse(process.env.EXTRA) : []),
];
const agent = new http.Agent({ keepAlive: true, maxSockets: 256 });

async function run(t, n, conc) {
  const body = JSON.stringify({ model: t.model, messages: [] });
  let bad = 0, i = 0;
  const lat = [];
  const one = () => new Promise((res) => {
    const t0 = process.hrtime.bigint();
    const q = http.request({ host: t.host, port: t.port, path: '/v1/chat/completions', method: 'POST', agent,
      headers: { 'content-type': 'application/json', ...t.headers } }, (s) => {
      if (s.statusCode !== 200) bad++;                       // a fast error must never look like a fast success
      s.resume();
      s.on('end', () => res(Number(process.hrtime.bigint() - t0) / 1e6));
    });
    q.end(body);
  });
  const start = Date.now();
  await Promise.all(Array.from({ length: conc }, async () => { while (i++ < n) lat.push(await one()); }));
  lat.sort((a, b) => a - b);
  const p = (x) => lat[Math.min(lat.length - 1, Math.floor(lat.length * x))].toFixed(2);
  return { bad, p50: p(0.5), p99: p(0.99), rps: Math.round(n / ((Date.now() - start) / 1000)) };
}

(async () => {
  for (const conc of [1, 32]) {
    for (const t of TARGETS) {
      await run(t, WARM, conc);
      const r = await run(t, N, conc);
      console.log(`conc=${String(conc).padEnd(2)} ${t.name} p50=${r.p50}ms p99=${r.p99}ms rps=${r.rps} non200=${r.bad}`);
      if (r.bad) { console.log('INVALID RUN: non-200 responses, numbers above are meaningless'); process.exit(1); }
    }
  }
  console.log('\nNote: at high concurrency this Node client/mock saturates first; only conc=1 isolates gateway overhead.');
  process.exit(0);
})();
