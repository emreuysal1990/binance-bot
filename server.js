'use strict';
/*
 * SURVIVE & GROW — Binance hesap yoneticisi (backend)
 * Modlar: paper (sahte para/gercek fiyat) | testnet | live (GERCEK PARA)
 * Strateji: trend filtresi + RSI/MACD/StochRSI/Bollinger + ATR'ye gore trailing stop, sermaye dilimleme.
 * Guvenlik: gun-ici kayip kill-switch, max islem siniri, sadece spot, manuel mudahale.
 * UYARI: Bu yatirim tavsiyesi degildir. Once paper -> testnet -> kucuk live ile dogrula.
 */
try { require('dotenv').config(); } catch (e) {}
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const CFG = {
  mode: (process.env.MODE || 'paper').toLowerCase(),
  quote: process.env.QUOTE || 'USDT',
  startCash: +(process.env.START_CASH || 50),
  univMax: Math.min(60, +(process.env.UNIVERSE || 40)),
  maxPositions: +(process.env.MAX_POSITIONS || 4),
  riskMode: process.env.RISK_MODE || 'normal',
  pollMs: +(process.env.POLL_MS || 6000),
  minNotional: +(process.env.MIN_NOTIONAL || 10),
  dailyLossStop: +(process.env.DAILY_LOSS_STOP || 0.15),
  maxTradeUsdt: +(process.env.MAX_TRADE_USDT || 1000),
  token: process.env.DASH_TOKEN || 'change-me',
  port: +(process.env.PORT || 8080),
  key: process.env.BINANCE_KEY || '',
  secret: process.env.BINANCE_SECRET || '',
};
const FEE = 0.001, DUST = 1;
const STATE_FILE = path.join(__dirname, 'state.json');
const STABLE = new Set(['usdt','usdc','dai','fdusd','tusd','usde','busd','usds','pyusd','usdd','gusd']);
const MODES = {
  safe:   { entry:0.40, maxPos:3, stopMult:1.0, trailMult:0.8, actMult:0.9 },
  normal: { entry:0.30, maxPos:4, stopMult:1.2, trailMult:1.0, actMult:0.8 },
  degen:  { entry:0.22, maxPos:5, stopMult:1.5, trailMult:1.2, actMult:0.7 },
};
const clamp = (x,a,b)=>Math.max(a,Math.min(b,x));
const isStable = (s,p,c)=>{ s=(s||'').toLowerCase();
  if (STABLE.has(s)) return true; if (s.indexOf('usd')>=0) return true;
  if (['eur','eurc','eurt','gbp','jpy','try','xaut','paxg'].indexOf(s)>=0) return true;
  if (isFinite(p)&&isFinite(c)&&Math.abs(p-1)<=0.015&&Math.abs(c)<0.5) return true; return false; };

// ---------- DURUM ----------
let S = {
  mode: CFG.mode, running: true, cash: CFG.startCash, positions: {}, trades: 0,
  peak: CFG.startCash, equityHist: [CFG.startCash], dayStartEquity: CFG.startCash, dayKey: dayKey(),
  killed: false, log: [], startedAt: Date.now(), lastError: '',
};
function dayKey(){ return new Date().toISOString().slice(0,10); }
process.on('unhandledRejection', (e)=>{ try{ S.lastError='unhandled: '+String(e&&e.message||e); }catch(_){} console.error('unhandledRejection:', e&&e.message||e); });
process.on('uncaughtException', (e)=>{ try{ S.lastError='uncaught: '+String(e&&e.message||e); }catch(_){} console.error('uncaughtException:', e&&e.message||e); });
function loadState(){ try{ const o=JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); if(o&&typeof o.cash==='number') S=Object.assign(S,o); log('info','Durum yuklendi','Varlik ~'+equity().toFixed(2)+' '+CFG.quote); }catch(e){} }
function saveState(){ try{ fs.writeFileSync(STATE_FILE, JSON.stringify(S)); }catch(e){} }
function log(type, action, detail){ const e={ ts:Date.now(), type, action, detail }; S.log.push(e); if(S.log.length>200)S.log.shift(); console.log(`[${new Date().toISOString()}] ${type.toUpperCase()} ${action} — ${detail}`); }

// ---------- BORSA / VERI ----------
const BN = 'https://data-api.binance.vision';     // Binance public market data (cografi engelsiz + yuksek limit, 429 yok)
let tradeEx = null;                               // ccxt (dinamik import) yalniz testnet/live icin initData'da kurulur

// ---------- PIYASA VERISI ----------
let universe = [];          // [{base, pair}]
let prices = {}, chg = {}, hist = {}, startPx = {}, scoreS = {}, cooldown = {};
const COOLDOWN_MS = 20000;
let lastFetch = 0, lastLoopErr = '';

async function fetchJSON(url){ const r=await fetch(url,{headers:{'accept':'application/json','user-agent':'survive-grow-bot/1.0'}}); if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); }
async function buildUniverse(){
  const d = await fetchJSON(BN+'/api/v3/ticker/24hr');     // tum semboller (tek istek)
  const rows=[];
  for(const t of d){ const s=t.symbol; if(!s||!s.endsWith('USDT')||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
    const base=s.slice(0,-4), price=parseFloat(t.lastPrice), vol=parseFloat(t.quoteVolume), c=parseFloat(t.priceChangePercent);
    if(!isFinite(price)||price<=0||isStable(base,price,c)) continue;
    rows.push({base, sym:s, price, vol, c}); }
  rows.sort((a,b)=>b.vol-a.vol);
  const top=rows.slice(0, CFG.univMax), u2=[];
  for(const r of top){ u2.push({base:r.base, sym:r.sym, pair:r.base+'/'+CFG.quote}); prices[r.base]=r.price; chg[r.base]=r.c; startPx[r.base]=startPx[r.base]||r.price; }
  if(u2.length<5) throw new Error('evren kucuk');
  universe=u2;
  log('info','Evren hazir', universe.length+' coin (Binance, hacme gore, stablecoin haric).');
  seedHistory().catch(()=>{});   // gecmisi arka planda klines ile besle
}
async function seedHistory(){
  for(const u of universe){
    if(hist[u.base] && hist[u.base].length>=40) continue;
    try{ const k=await fetchJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=1m&limit=100'); if(Array.isArray(k)&&k.length) hist[u.base]=k.map(x=>parseFloat(x[4])); }
    catch(e){ if(!hist[u.base]) hist[u.base]=Array(30).fill(prices[u.base]||1); }
  }
  log('info','Gecmis yuklendi','Indikatorler 1 dakikalik mumlarla hazir.');
}
async function refreshPrices(){
  if(!universe.length) return;
  const syms = encodeURIComponent(JSON.stringify(universe.map(u=>u.sym)));
  const d = await fetchJSON(BN+'/api/v3/ticker/24hr?symbols='+syms);
  const map={}; universe.forEach(u=>map[u.sym]=u.base);
  for(const t of d){ const b=map[t.symbol]; if(!b) continue; const p=parseFloat(t.lastPrice);
    if(isFinite(p)&&p>0){ prices[b]=p; chg[b]=parseFloat(t.priceChangePercent)||chg[b]||0;
      if(!hist[b]) hist[b]=Array(30).fill(p); else { hist[b].push(p); if(hist[b].length>220) hist[b].shift(); } } }
}

// ---------- INDIKATORLER ----------
const sma=(a,p)=>{ if(a.length<p) return null; let s=0; for(let i=a.length-p;i<a.length;i++)s+=a[i]; return s/p; };
const emaVal=(a,p)=>{ if(a.length<p) return null; const k=2/(p+1); let e=0; for(let i=0;i<p;i++)e+=a[i]; e/=p; for(let i=p;i<a.length;i++)e=a[i]*k+e*(1-k); return e; };
function emaArr(a,p){ if(a.length<p) return null; const k=2/(p+1),o=Array(a.length).fill(null); let e=0; for(let i=0;i<p;i++)e+=a[i]; e/=p; o[p-1]=e; for(let i=p;i<a.length;i++){e=a[i]*k+e*(1-k);o[i]=e;} return o; }
function rsiVal(a,len=14){ if(a.length<len+1) return 50; let g=0,l=0; for(let i=a.length-len;i<a.length;i++){const d=a[i]-a[i-1]; if(d>=0)g+=d; else l-=d;} if(l===0)return 100; return 100-100/(1+(g/len)/(l/len)); }
function rsiArr(a,len=14){ if(a.length<len+1) return null; const o=Array(a.length).fill(null); let g=0,l=0; for(let i=1;i<=len;i++){const d=a[i]-a[i-1]; if(d>=0)g+=d; else l-=d;} g/=len;l/=len; o[len]=l===0?100:100-100/(1+g/l); for(let i=len+1;i<a.length;i++){const d=a[i]-a[i-1],u=d>0?d:0,n=d<0?-d:0; g=(g*(len-1)+u)/len; l=(l*(len-1)+n)/len; o[i]=l===0?100:100-100/(1+g/l);} return o; }
function macd(a){ const f=emaArr(a,12),s=emaArr(a,26); if(!f||!s) return null; const line=[]; for(let i=0;i<a.length;i++) if(f[i]!=null&&s[i]!=null) line.push(f[i]-s[i]); if(line.length<10) return null; const sig=emaArr(line,9); if(!sig) return null; const n=line.length; return { hist:line[n-1]-sig[n-1], crossUp:(line[n-2]<=sig[n-2]&&line[n-1]>sig[n-1]) }; }
function stochRsi(a,len=14){ const rs=rsiArr(a,len); if(!rs) return null; const v=rs.filter(x=>x!=null); if(v.length<len) return null; const w=v.slice(-len),cur=w[w.length-1],mn=Math.min(...w),mx=Math.max(...w); return mx===mn?0.5:(cur-mn)/(mx-mn); }
function boll(a,p=20,m=2){ if(a.length<p) return null; const w=a.slice(-p),mean=w.reduce((x,y)=>x+y,0)/p; const sd=Math.sqrt(w.reduce((x,y)=>x+(y-mean)*(y-mean),0)/p),up=mean+m*sd,lo=mean-m*sd,price=a[a.length-1]; return { pctB:up===lo?0.5:(price-lo)/(up-lo) }; }
function rangePct(a,p=20){ if(a.length<p) return 0.02; const w=a.slice(-p),mx=Math.max(...w),mn=Math.min(...w),mean=w.reduce((x,y)=>x+y,0)/p; return mean>0?(mx-mn)/mean:0.02; }
function analyze(b){ const h=hist[b]; if(!h||h.length<30) return null;
  const price=h[h.length-1], r=rsiVal(h,14), m=macd(h), ef=emaVal(h,9), es=emaVal(h,21), el=emaVal(h,50), bb=boll(h,20,2), sr=stochRsi(h,14), roc=(price-h[h.length-6])/h[h.length-6], c=chg[b]||0;
  let score=0; const why=[];
  if(ef!=null&&es!=null){ if(ef>es){score+=0.20;why.push('EMA9>EMA21');} else score-=0.15; }
  if(el!=null&&price>el){score+=0.10;why.push('EMA50 ustu');} else if(el!=null) score-=0.08;
  if(m){ if(m.crossUp){score+=0.20;why.push('MACD kesisim');} else if(m.hist>0){score+=0.12;why.push('MACD+');} if(m.hist<0)score-=0.10; }
  if(r<30){score+=0.18;why.push('RSI asiri satim');} else if(r>72)score-=0.22; else if(r>=48&&r<=63)score+=0.08;
  if(sr!=null){ if(sr<0.2){score+=0.10;why.push('StochRSI dip');} else if(sr>0.85)score-=0.10; }
  if(bb){ if(bb.pctB<0.1){score+=0.10;why.push('BB alt bant');} else if(bb.pctB>0.96)score-=0.12; }
  if(roc>0.004){score+=0.10;why.push('momentum+');} else if(roc<-0.012)score-=0.08;
  if(c<-9)score-=0.06; else if(c>0&&c<18)score+=0.03;
  const trendUp=(ef!=null&&es!=null&&ef>es)&&(el==null||price>el)&&(es>0&&(ef-es)/es>0.0003);
  return { score, r, why, price, trendUp, vp:rangePct(h,20) };
}

// ---------- EMIRLER ----------
function equity(){ let v=S.cash; for(const b in S.positions){ const p=S.positions[b]; v+=p.qty*(prices[b]||p.entry); } return v; }
async function placeBuy(base, costUsdt, a, M){
  costUsdt = Math.min(costUsdt, CFG.maxTradeUsdt);
  const pair = base+'/'+CFG.quote, px = prices[base];
  if (S.mode === 'paper'){
    const fee=costUsdt*FEE, qty=(costUsdt-fee)/px; S.cash-=costUsdt; openLedger(base,qty,px,costUsdt,a,M);
  } else {
    const o = await tradeEx.createMarketBuyOrderWithCost(pair, costUsdt);
    const filledQty = o.filled || (o.amount||0), spent = o.cost || costUsdt, avg = o.average || px;
    S.cash -= spent; openLedger(base, filledQty, avg, spent, a, M);
  }
}
function openLedger(base, qty, px, cost, a, M){
  const vp=a&&a.vp?a.vp:0.02;
  const stop=clamp(M.stopMult*vp,0.008,0.030), trail=clamp(M.trailMult*vp,0.006,0.035), act=clamp(M.actMult*vp,0.008,0.020), tp=clamp(3.0*stop,0.030,0.120);
  S.positions[base]={ qty, entry:px, cost, high:px, openTs:Date.now(), stop, trail, act, tp };
  S.trades++;
  log('buy','AL '+base, `${cost.toFixed(2)} ${CFG.quote} @ ${px} — skor ${(scoreS[base]||0).toFixed(2)} · ${(a?a.why.slice(0,3).join(', '):'sinyal')} · stop -%${(stop*100).toFixed(1)} / hedef +%${(tp*100).toFixed(1)}`);
}
async function placeSell(base, why){
  const p = S.positions[base]; if(!p) return; const pair=base+'/'+CFG.quote, px=prices[base]||p.entry;
  let gross, fee, net;
  if (S.mode === 'paper'){ gross=p.qty*px; fee=gross*FEE; net=gross-fee; S.cash+=net; }
  else { const o=await tradeEx.createMarketSellOrder(pair, p.qty); gross=o.cost||(p.qty*px); fee=(o.fee&&o.fee.cost)||gross*FEE; net=gross-fee; S.cash+=net; }
  const pnl=net-p.cost, pct=(pnl/p.cost)*100; delete S.positions[base]; cooldown[base]=Date.now()+COOLDOWN_MS;
  log(pnl>=0?'sell-win':'sell-loss','SAT '+base, `@ ${px} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} ${CFG.quote} (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
}

// ---------- STRATEJI ----------
function strategy(){
  const eq=equity(); const M=Object.assign({}, MODES[S.mode==='paper'?CFG.riskMode:CFG.riskMode] || MODES.normal);
  let entry=M.entry, maxPos=Math.min(M.maxPos, CFG.maxPositions);
  if(eq<CFG.startCash*0.6){ maxPos=Math.max(1,maxPos-1); entry+=0.05; }
  if(eq<CFG.startCash*0.35){ maxPos=Math.max(1,maxPos-1); entry+=0.05; }
  const ana={};
  for(const u of universe){ const a=analyze(u.base); ana[u.base]=a; if(a){ scoreS[u.base]= scoreS[u.base]==null?a.score:scoreS[u.base]*0.6+a.score*0.4; } }
  // CIKISLAR
  for(const b of Object.keys(S.positions)){
    const p=S.positions[b], px=prices[b]; if(!px) continue; if(px>p.high)p.high=px;
    const gross=(px-p.entry)/p.entry, net=gross-2*FEE, ss=scoreS[b]!=null?scoreS[b]:0, heldMs=Date.now()-p.openTs; let why=null;
    if(px<=p.entry*(1-p.stop)) why='zarar-kes';
    else if(px>=p.entry*(1+p.tp)) why='hedef';
    else if(heldMs>2*CFG.pollMs && p.high>=p.entry*(1+p.act) && px<=p.high*(1-p.trail)) why='trailing stop';
    else if(heldMs>8*CFG.pollMs && ss<=-0.30) why='momentum oldu';
    if(why) queue(()=>placeSell(b, `${why} (%${(net*100).toFixed(2)})`));
  }
  // GIRISLER (sadece trend yukari + skor esik; konviksiyona gore boyut; sermaye dilimleme)
  let open=Object.keys(S.positions).length;
  const minOrder = (S.mode==='paper')?DUST:CFG.minNotional;
  let avail = S.cash;   // bu tik icin yerel kullanilabilir nakit (S.cash iki kez dusulmesin)
  const cand=universe.map(u=>u.base).filter(b=>!S.positions[b]&&prices[b]&&!isStable(b,prices[b],chg[b])&&(!cooldown[b]||Date.now()>cooldown[b])&&ana[b]&&ana[b].trendUp&&scoreS[b]!=null&&scoreS[b]>=entry).sort((x,y)=>scoreS[y]-scoreS[x]);
  for(const b of cand){ if(open>=maxPos) break;
    const conv=clamp(0.6+(scoreS[b]-entry)*1.5,0.6,1.0);            // yuksek skor -> daha buyuk (dilim icinde)
    let size=Math.min(equity()/maxPos*conv, avail*0.99);
    if(size<minOrder) continue;
    queue(()=>placeBuy(b, size, ana[b], M)); open++; avail-=size; // YALNIZ yerel rezervasyon; gercek dusum placeBuy'da
  }
}
// emir kuyrugu: ayni dongude paralel borsa cagrilarini sirayla calistirir
let q=Promise.resolve();
function queue(fn){ q=q.then(fn).catch(e=>{ S.lastError=String(e&&e.message||e); log('warn','Emir hatasi', S.lastError); }); }

// ---------- DONGU + GUVENLIK ----------
async function loop(){
  try{
    if(!universe.length){ await buildUniverse(); }
    if(dayKey()!==S.dayKey){ S.dayKey=dayKey(); S.dayStartEquity=equity(); S.killed=false; log('info','Yeni gun','Gun-ici kayip sayaci sifirlandi.'); }
    await refreshPrices();
    const eq=equity();
    // kill-switch: gun-ici kayip
    if(!S.killed && eq <= S.dayStartEquity*(1-CFG.dailyLossStop)){
      S.killed=true; S.running=false;
      log('warn','KILL-SWITCH', `Gun-ici kayip %${(CFG.dailyLossStop*100).toFixed(0)} asildi. Tum pozisyonlar kapatiliyor, bot durduruldu.`);
      for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'kill-switch'));
    }
    if(S.running && !S.killed) strategy();
    S.equityHist.push(eq); if(S.equityHist.length>500)S.equityHist.shift(); if(eq>S.peak)S.peak=eq;
    saveState(); broadcast();
  }catch(e){ const m=String(e&&e.message||e); S.lastError=m; if(m!==lastLoopErr){ lastLoopErr=m; log('warn','Dongu hatasi', m); } }
}

// ---------- API + PANEL ----------
const app=express(); app.use(express.json()); app.get('/',(req,res)=>res.sendFile(path.join(__dirname,'index.html')));
function auth(req,res,next){ const t=req.headers['x-token']||req.query.token; if(t!==CFG.token) return res.status(401).json({error:'token'}); next(); }
function snapshot(){ const eq=equity(); return {
  mode:S.mode, running:S.running, killed:S.killed, equity:eq, cash:S.cash, peak:S.peak, start:CFG.startCash,
  pnlPct:(eq-CFG.startCash)/CFG.startCash*100, trades:S.trades, quote:CFG.quote, univ:universe.length,
  positions:Object.entries(S.positions).map(([b,p])=>({ base:b, entry:p.entry, price:prices[b]||p.entry, qty:p.qty, cost:p.cost,
    upnl:(prices[b]||p.entry)*p.qty-p.cost, stopLvl:(p.high>=p.entry*(1+p.act))?p.high*(1-p.trail):p.entry*(1-p.stop) })),
  market:universe.map(u=>({ base:u.base, price:prices[u.base], chg:chg[u.base]||0, score:scoreS[u.base]||0, held:!!S.positions[u.base] }))
    .filter(x=>x.price).sort((a,b)=>(b.held-a.held)||(b.score-a.score)).slice(0,18),
  equityHist:S.equityHist.slice(-160), log:S.log.slice(-60), lastError:S.lastError };
}
app.get('/api/state', auth, (req,res)=>res.json(snapshot()));
app.post('/api/pause', auth, (req,res)=>{ S.running=false; log('info','Manuel','Bot duraklatildi.'); res.json({ok:true}); broadcast(); });
app.post('/api/resume', auth, (req,res)=>{ if(S.killed){ S.killed=false; S.dayStartEquity=equity(); } S.running=true; log('info','Manuel','Bot devam ediyor.'); res.json({ok:true}); broadcast(); });
app.post('/api/mode', auth, (req,res)=>{ const m=(req.body.mode||'').toLowerCase(); if(MODES[m]){ CFG.riskMode=m; log('info','Manuel','Risk modu: '+m); } res.json({ok:true}); });
app.post('/api/buy', auth, (req,res)=>{ const b=(req.body.symbol||'').toUpperCase(); if(!universe.some(u=>u.base===b)) return res.status(400).json({error:'symbol'});
  const M=MODES[CFG.riskMode], size=Math.min(equity()/Math.min(M.maxPos,CFG.maxPositions), S.cash*0.99); if(size<DUST) return res.status(400).json({error:'nakit yok'});
  queue(()=>placeBuy(b,size,analyze(b),M)); log('info','Manuel','Elle ALIM: '+b); res.json({ok:true}); });
app.post('/api/sell', auth, (req,res)=>{ const b=(req.body.symbol||'').toUpperCase(); if(!S.positions[b]) return res.status(400).json({error:'pozisyon yok'});
  queue(()=>placeSell(b,'manuel satis')); log('info','Manuel','Elle SATIS: '+b); res.json({ok:true}); });
app.post('/api/close-all', auth, (req,res)=>{ for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'manuel: hepsini kapat')); log('info','Manuel','Tum pozisyonlar kapatiliyor.'); res.json({ok:true}); });
app.post('/api/panic', auth, (req,res)=>{ S.running=false; for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'PANIK')); log('warn','Manuel','PANIK: hepsi kapatildi + bot durduruldu.'); res.json({ok:true}); broadcast(); });
app.post('/api/reset', auth, (req,res)=>{ S.cash=CFG.startCash; S.positions={}; S.trades=0; S.equityHist=[CFG.startCash]; S.peak=CFG.startCash; S.killed=false; S.running=true; S.dayStartEquity=CFG.startCash; S.dayKey=dayKey(); S.log=[]; cooldown={}; scoreS={}; try{fs.unlinkSync(STATE_FILE);}catch(e){} log('info','Manuel','Sifirlandi — yeni $'+CFG.startCash+' kasa.'); res.json({ok:true}); broadcast(); });

const server=http.createServer(app);
const wss=new WebSocketServer({ server, path:'/ws' });
wss.on('connection',(socket,req)=>{ const u=new URL(req.url,'http://x'); if(u.searchParams.get('token')!==CFG.token){ socket.close(); return; } socket.send(JSON.stringify(snapshot())); });
function broadcast(){ const msg=JSON.stringify(snapshot()); wss.clients.forEach(c=>{ if(c.readyState===1) c.send(msg); }); }

// ---------- BASLAT ----------
function start(){
  log('info','Baslatiliyor', `mod=${CFG.mode} · risk=${CFG.riskMode} · evren=${CFG.univMax} · maxPoz=${CFG.maxPositions}`);
  if(CFG.mode==='live') log('warn','GERCEK PARA', 'LIVE modundasin — gercek fonlarla islem yapilacak. Kucuk basla.');
  loadState();
  server.listen(CFG.port, ()=>log('info','Panel hazir', `port ${CFG.port} dinleniyor (token ile gir)`)); // ONCE portu ac
  initData();                                                                                            // veri + dongu arka planda
}
async function initData(){
  try{
    if(CFG.mode!=='paper'){
      try{
        const mod = await import('ccxt'); const ccxt = mod.default || mod;
        tradeEx = new ccxt.binance({ apiKey:CFG.key, secret:CFG.secret, enableRateLimit:true,
          options:{ defaultType:'spot', createMarketBuyOrderRequiresPrice:false } });
        if(CFG.mode==='testnet') tradeEx.setSandboxMode(true);
        await tradeEx.loadMarkets();
        try{ const bal=await tradeEx.fetchBalance(); const free=bal.free&&bal.free[CFG.quote]; if(typeof free==='number'){ S.cash=free; log('info','Bakiye','Serbest '+CFG.quote+': '+free.toFixed(2)); } }catch(e){ log('warn','Bakiye okunamadi',String(e.message||e)); }
      }catch(e){ S.lastError='ccxt: '+String(e&&e.message||e); log('warn','ccxt yuklenemedi', S.lastError); }
    }
    await buildUniverse();
  }catch(e){ S.lastError=String(e&&e.message||e); log('warn','Veri hatasi', S.lastError+' — dongude tekrar denenecek.'); }
  setInterval(loop, CFG.pollMs); loop();
}
start();
