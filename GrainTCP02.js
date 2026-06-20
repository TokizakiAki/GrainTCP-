import { connect } from 'cloudflare:sockets';

const CFG = { id: '931b2d5a-840e-4513-b920-fd923a4316db', chunk: 64 * 1024, dnPack: 32 * 1024, dnTail: 512, dnMs: 0, upPack: 16 * 1024, upQMax: 256 * 1024, maxED: 8 * 1024, concur: 4 };

// 注意：修改了入口，将 env 传入 ws 函数以支持环境变量
export default { fetch: (req, env) => req.headers.get('Upgrade')?.toLowerCase() === 'websocket' ? ws(req, env) : new Response('Hello world!') }; 

const hex = c => (c > 64 ? c + 9 : c) & 0xF;
const idB = new Uint8Array(16), dec = new TextDecoder(); for (let i = 0, p = 0, c, h; i < 16; i++) { c = CFG.id.charCodeAt(p++); c === 45 && (c = CFG.id.charCodeAt(p++)); h = hex(c); c = CFG.id.charCodeAt(p++); c === 45 && (c = CFG.id.charCodeAt(p++)); idB[i] = h << 4 | hex(c); }
const [I0, I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15] = idB;
const matchID = c => c[1] === I0 && c[2] === I1 && c[3] === I2 && c[4] === I3 && c[5] === I4 && c[6] === I5 && c[7] === I6 && c[8] === I7 && c[9] === I8 && c[10] === I9 && c[11] === I10 && c[12] === I11 && c[13] === I12 && c[14] === I13 && c[15] === I14 && c[16] === I15;
const addr = (t, b) => t === 1 ? `${b[0]}.${b[1]}.${b[2]}.${b[3]}` : t === 3 ? dec.decode(b) : `[${Array.from({ length: 8 }, (_, i) => ((b[i * 2] << 8) | b[i * 2 + 1]).toString(16)).join(':')}]`;

// GrainTCP 原版连接函数保持不变，性能最高
const sprout = (f, h, p, s = f.connect({ hostname: h, port: p })) => s.opened.then(() => s);
const raceSprout = (f, h, p) => { if (!f?.connect) return Promise.reject(new Error('connect unavailable')); if (CFG.concur <= 1) return sprout(f, h, p); const ts = Array(CFG.concur).fill().map(() => sprout(f, h, p)); return Promise.any(ts).then(w => { ts.forEach(t => t.then(s => s !== w && s.close(), () => {})); return w; }); };

const parseAddr = (b, o, t) => { const l = t === 3 ? b[o++] : t === 1 ? 4 : t === 4 ? 16 : null; if (l === null) return null; const n = o + l; return n > b.length ? null : { targetAddrBytes: b.subarray(o, n), dataOffset: n }; };
const vless = c => { if (c.length < 24 || !matchID(c)) return null; let o = 19 + c[17]; const p = (c[o] << 8) | c[o + 1]; let t = c[o + 2]; if (t !== 1) t += 1; const a = parseAddr(c, o + 3, t); return a ? { addrType: t, ...a, port: p } : null; };

const mkQ = (cap, qCap = cap, itemsMax = Math.max(1, qCap >> 8)) => {
  let q = [], h = 0, qB = 0, buf = null;
  const trim = () => { h > 32 && h * 2 >= q.length && (q = q.slice(h), h = 0); };
  const take = () => { if (h >= q.length) return null; const d = q[h]; q[h++] = undefined; qB -= d.byteLength; trim(); return d; };
  return { get bytes() { return qB; }, get size() { return q.length - h; }, get empty() { return h >= q.length; }, clear() { q = []; h = 0; qB = 0; },
    sow(d) { const n = d?.byteLength || 0; if (!n) return 1; if (qB + n > qCap || q.length - h >= itemsMax) return 0; q.push(d); qB += n; return 1; },
    bundle(d) {
      d ||= take(); if (!d || h >= q.length || d.byteLength >= cap) return [d, 0];
      let n = d.byteLength, e = h; while (e < q.length) { const x = q[e], nn = n + x.byteLength; if (nn > cap) break; n = nn; e++; }
      if (e === h) return [d, 0]; const out = buf ||= new Uint8Array(cap); out.set(d);
      for (let o = d.byteLength; h < e;) { const x = q[h]; q[h++] = undefined; qB -= x.byteLength; out.set(x, o); o += x.byteLength; } trim(); return [out.subarray(0, n), 1]; } }; };

const mkDn = w => {
  const cap = CFG.dnPack, tail = CFG.dnTail, low = Math.max(4096, tail << 3);
  let pb = new Uint8Array(cap), p = 0, tp = 0, mq = 0, gen = 0, qk = 0, qr = 0;
  const reap = () => { tp && clearTimeout(tp); tp = 0; mq = 0; if (!p) return; w.send(pb.subarray(0, p).slice()); pb = new Uint8Array(cap); p = 0; qr = 0; };
  const ripen = () => { if (tp || mq) return; mq = 1; qk = gen; queueMicrotask(() => { mq = 0; if (!p || tp) return; if (cap - p < tail) return reap(); tp = setTimeout(() => { tp = 0; if (!p) return; if (cap - p < tail) return reap(); if (qr < 2 && (gen !== qk || p < low)) { qr++; qk = gen; return ripen(); } reap(); }, Math.max(CFG.dnMs, 1)); }); };
  return {
    send(u) { let o = 0, n = u?.byteLength || 0; if (!n) return; while (o < n) { if (!p && n - o >= cap) { const m = Math.min(cap, n - o); w.send(o || m !== n ? u.subarray(o, o + m) : u); o += m; continue; } const m = Math.min(cap - p, n - o); pb.set(u.subarray(o, o + m), p); p += m; o += m; gen++; if (p === cap || cap - p < tail) reap(); else ripen(); } }, reap }; };

const mill = async (rd, w) => { const r = rd.getReader({ mode: 'byob' }), tx = mkDn(w); let buf = new ArrayBuffer(CFG.chunk);
  try { for (;;) { const { done, value: v } = await r.read(new Uint8Array(buf, 0, CFG.chunk)); if (done) break; if (!v?.byteLength) continue; if (v.byteLength >= (CFG.chunk >> 1)) tx.reap(), w.send(v), buf = new ArrayBuffer(CFG.chunk); else tx.send(v.slice()), buf = v.buffer; } tx.reap(); } catch {} finally { try { tx.reap(); } catch {} try { r.releaseLock(); } catch {} } };

const ws = async (req, env) => {
  const [client, server] = Object.values(new WebSocketPair()); server.accept({ allowHalfOpen: true }); server.binaryType = 'arraybuffer'; 
  const fetcher = req.fetcher; 
  const _url = new URL(req.url);

  // --- 动态 ProxyIP 解析模块开始 ---
  const edStr = req.headers.get('sec-websocket-protocol'); 
  const ed = edStr && edStr.length <= CFG.maxED * 4 / 3 + 4 ? /** @type {*} */ (Uint8Array).fromBase64(edStr, { alphabet: 'base64url' }) : null; 
  let curW = null, sock = null, closed = false, busy = false;

  const uq = mkQ(CFG.upPack, CFG.upQMax, CFG.upQMax >> 8);
  const wither = () => { if (closed) return; closed = true; uq.clear(); try { curW?.releaseLock(); } catch {} try { sock?.close(); } catch {} try { server.close(); } catch {} };
  const toU8 = d => d instanceof Uint8Array ? d : ArrayBuffer.isView(d) ? new Uint8Array(d.buffer, d.byteOffset, d.byteLength) : new Uint8Array(d);
  const sow = d => { const u = toU8(d), n = u.byteLength; if (!n) return 1; if (uq.sow(u)) return 1; wither(); return 0; };

  // 解析 URL 路径中的代理参数或读取环境变量 PROXYIP
  const _pathPxyRaw = _url.pathname.match(/\/proxyip=([^/?#]+)/i)?.[1] || ''; 
  const globalProxy = /&global/i.test(_pathPxyRaw); 
  const proxyList = (_pathPxyRaw.replace(/&global/i, '') || (env?.PROXYIP || '')).trim().split(/[\n,]+/).map(s => s.trim()).filter(Boolean);

  // 随机选择并解析一个代理节点
  const pickProxy = () => { 
    if (!proxyList.length) return null; 
    const raw = proxyList[Math.floor(Math.random() * proxyList.length)]; 
    if (raw.startsWith('[')) { 
      const e = raw.indexOf(']'); 
      if (e > 0) { 
        const h = raw.slice(1, e), ps = raw.slice(e + 1), p = ps.startsWith(':') ? parseInt(ps.slice(1)) : NaN; 
        return { h, p: p > 0 && p < 65536 ? p : null }; 
      } 
    } 
    const i = raw.lastIndexOf(':'); 
    if (i > 0) { 
      const p = parseInt(raw.slice(i + 1)); 
      if (p > 0 && p < 65536) return { h: raw.slice(0, i), p }; 
    } 
    return { h: raw, p: null }; 
  };
  // --- 动态 ProxyIP 解析模块结束 ---

  const thresh = async () => { if (busy || closed) return; busy = true; try { for (;;) {
    if (closed) break; 
    if (!sock) { 
        const [d] = uq.bundle(); if (!d) break; const r = vless(d); if (!r) throw wither(); 
        server.send(new Uint8Array([d[0], 0])); 
        const host = addr(r.addrType, r.targetAddrBytes), port = r.port, payload = d.subarray(r.dataOffset); 
        
        // --- 核心故障转移逻辑缝合 ---
        sock = await (globalProxy && proxyList.length ? 
            // 模式 1：如果指定了 &global，直接全局强制走代理
            (p => raceSprout(fetcher, p.h, p.p || port))(pickProxy()) : 
            // 模式 2：尝试直连，如果失败（如遇到 CF 官网的 Error 1000），则捕获异常并切换到 ProxyIP
            raceSprout(fetcher, host, port).catch(async () => { 
                const pxy = pickProxy(); 
                if (!pxy) throw new Error('direct failed'); 
                return raceSprout(fetcher, pxy.h, pxy.p || port); 
            })
        );
        // -----------------------------

        if (!sock) throw wither(); 
        curW = sock.writable.getWriter(); 
        const [first] = uq.bundle(payload); first?.byteLength && await curW.write(first); 
        mill(sock.readable, server).finally(() => wither()); 
        continue; 
    }
    const [d] = uq.bundle(); if (!d) break; await curW.write(d);
  } } catch { wither(); } finally { busy = false; !uq.empty && !closed && queueMicrotask(thresh); } };

  if (ed && sow(ed)) thresh();
  server.addEventListener('message', e => { closed || (sow(e.data) && thresh()); });
  server.addEventListener('close', () => wither()); server.addEventListener('error', () => wither());
  
  return new Response(null, { status: 101, webSocket: client, headers: { 'Sec-WebSocket-Extensions': '' } }); 
};
