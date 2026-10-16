// Minimal OpenAI-compatible upstream: fixed JSON answer with usage. Intentionally trivial so it is
// never the interesting part; if it saturates, the run says so (see bench.js).
require('http').createServer((q, s) => {
  q.resume();
  q.on('end', () => {
    s.writeHead(200, { 'content-type': 'application/json' });
    s.end('{"id":"x","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":3}}');
  });
}).listen(9102, '0.0.0.0');
