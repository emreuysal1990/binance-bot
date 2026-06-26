'use strict';
/* Survive & Grow — Binance Hesap Yoneticisi (KATI TREND TAKIP SURUMU)
 * Strateji: 1h Mum, ADX > 25, HMA Up, VWAP Up, HTF (4h) Onayi
 * Cikis: Erken kar alma YOK. Sadece ATR bazli Iz Suren Stop (Trailing Stop).
 */
try { require('dotenv').config(); } catch (e) {}
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

// ---------- AYARLAR ----------
const CFG = {
  mode: (process.env.MODE || 'paper').toLowerCase(),
  token: process.env.DASH_TOKEN || 'change-me',
  quote: (process.env.QUOTE || 'USDT').toUpperCase(),
  port: +(process.env.PORT || 8080),
  startCash: +(process.env.START_CASH || 100),
  maxPositions: +(process.env.MAX_POSITIONS || 5),     // DEĞİŞTİ: Daha az coin, daha odaklı sermaye
  univMax: +(process.env.UNIVERSE || 40),              // DEĞİŞTİ: Sadece en hacimli 40 coin
  pollMs: +(process.env.POLL_MS || 10000),             // 10 saniyede bir kontrol yeterli
  fee: +(process.env.FEE || 0.001),                    
  slip: +(process.env.SLIP || 0.001),                  // Gerçekçi slipaj
  htf: true,                                           // HTF onayı ZORUNLU
  interval: '1h',                                      // DEĞİŞTİ: 1 Saatlik mumlar (Gürültüyü engeller)
  pumpMax: +(process.env.PUMP_MAX || 25),              // %25 üzeri günlük pumplara girme
  dailyLossStop: +(process.env.DAILY_LOSS_STOP || 0.10),
  minNotional: +(process.env.MIN_NOTIONAL || 10),
  maxTrade: +(process.env.MAX_TRADE_USDT || 0),        
  cooldownMin: +(process.env.COOLDOWN_MIN || 240),     // DEĞİŞTİ: Stop olunca o coine 4 saat bulaşma
  warmupSec: +(process.env.WARMUP_SEC || 60),          
  maxNewPerTick: +(process.env.MAX_NEW_PER_TICK || 2), 
  baseFrac: +(process.env.BASE_POS_PCT || 20)/100,     // Kasanın %20'si ile gir
  maxFrac: +(process.env.MAX_POS_PCT || 35)/100,      
  investTarget: +(process.env.INVEST_TARGET || 90)/100, 
  key: process.env.BINANCE_KEY || '',
  secret: process.env.BINANCE_SECRET || '',
};
const BN = 'https://data-api.binance.vision';
const INT_MIN = 60; // 1h
const VWAP_LEN = 24; // 24 saat
const STATE_FILE = path.join(__dirname, 'state.json');
const STABLES = ['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD','PYUSD','EUR','TRY','GBP','AEUR','XUSD','RLUSD','USDE','USD1','GUSD','LUSD','FRAX','USTC','EURI'];
function dayKey(){ return new Date().toISOString().slice(0,10); }

let S = {
  mode: CFG.mode, running: true, killed: false, cash: CFG.startCash, positions: {}, trades: 0,
  peak: CFG.startCash, dayStartEquity: CFG.startCash, dayKey: dayKey(),
  equityHist: [CFG.startCash], closed: [], log: [], startedAt: Date.now(), lastError: '', _lastEq: 0,
};
process.on('unhandledRejection', e=>{ try{S.lastError='unhandled: '+String(e&&e.message||e);}catch(_){} console.error('unhandledRejection:', e&&e.message||e); });
process.on('uncaughtException',  e=>{ try{S.lastError='uncaught: '+String(e&&e.message||e);}catch(_){} console.error('uncaughtException:', e&&e.message||e); });

let prices={}, chg={}, hist={}, hist1h={}, hist4h={}, universe=[], cooldown={}, ana={}, prevPx={}, lastReseed=0, lastLoopErr='';
let dataReady=false, warmupUntil=0;
let tradeEx=null;
let q=Promise.resolve(); function queue(fn){ q=q.then(fn).catch(e=>log('warn','Kuyruk',String(e&&e.message||e))); return q; }

function log(type,action,detail){ S.log.unshift({type,action,detail,ts:Date.now()}); if(S.log.length>140) S.log.pop(); }
function equity(){ let e=S.cash; for(const b in S.positions){ const p=S.positions[b]; e+=p.qty*(prices[b]||p.entry); } return e; }
function isStable(b){ return STABLES.includes(b); }
function saveState(){ try{ fs.writeFileSync(STATE_FILE, JSON.stringify({cash:S.cash,positions:S.positions,trades:S.trades,peak:S.peak,dayStartEquity:S.dayStartEquity,dayKey:S.dayKey,killed:S.killed,running:S.running,equityHist:S.equityHist.slice(-2000),closed:S.closed.slice(-1000)})); }catch(e){} }
function loadState(){ try{ const j=JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); Object.assign(S,j); if(!Array.isArray(S.equityHist))S.equityHist=[CFG.startCash]; if(!Array.isArray(S.closed))S.closed=[]; log('info','Durum','Onceki durum yuklendi.'); }catch(e){} }
function recordClosed(base,pnl,pct,why){ S.closed.push({base,pnl,pct,why,ts:Date.now()}); if(S.closed.length>1000) S.closed.shift(); }

// ---------- VERI (Binance) ----------
async function getJSON(url){ const r=await fetch(url,{headers:{'accept':'application/json','user-agent':'sg-bot/2.0'}}); if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); }
async function buildUniverse(){
  const d = await getJSON(BN+'/api/v3/ticker/24hr');
  const rows=[];
  for(const t of d){ const s=t.symbol; if(!s.endsWith(CFG.quote)||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
    const base=s.slice(0,-CFG.quote.length); if(isStable(base)) continue;
    const price=parseFloat(t.lastPrice), vol=parseFloat(t.quoteVolume), c=parseFloat(t.priceChangePercent);
    if(!isFinite(price)||price<=0) continue;
    rows.push({base,sym:s,price,vol,c}); }
  rows.sort((a,b)=>b.vol-a.vol);
  const top=rows.slice(0,CFG.univMax);
  universe=top.map(r=>({base:r.base,sym:r.sym}));
  for(const r of top){ prices[r.base]=r.price; chg[r.base]=r.c; }
  if(universe.length<5) throw new Error('evren kucuk');
  log('info','Evren', universe.length+' coin (Binance, hacme gore, stablecoin haric).');
  await seedHistory();
}
async function seedHistory(){
  for(const u of universe){
    try{ const k=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval='+CFG.interval+'&limit=150');
      if(Array.isArray(k)&&k.length) hist[u.base]=k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]})); }catch(e){}
    if(CFG.htf){
      try{ const k4=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=4h&limit=100'); if(Array.isArray(k4)&&k4.length) hist4h[u.base]=k4.map(x=>+x[4]); }catch(e){}
    }
  }
  lastReseed=Date.now();
  if(!dataReady){ dataReady=true; warmupUntil=Date.now()+CFG.warmupSec*1000;
    log('info','Hazir', `Veri oturdu — isinma sonrasi alim baslar.`); }
}
async function refreshPrices(){
  if(!universe.length) return;
  const syms=encodeURIComponent(JSON.stringify(universe.map(u=>u.sym)));
  const d=await getJSON(BN+'/api/v3/ticker/24hr?symbols='+syms);
  for(const t of d){ const base=t.symbol.slice(0,-CFG.quote.length); const p=parseFloat(t.lastPrice);
    if(!isFinite(p)||p<=0) continue; prices[base]=p; chg[base]=parseFloat(t.priceChangePercent)||chg[base]||0;
    const h=hist[base]; if(h&&h.length){ const last=h[h.length-1]; last.c=p; if(p>last.h)last.h=p; if(p<last.l)last.l=p; } }
  if(Date.now()-lastReseed > 15*60*1000) seedHistory().catch(()=>{});
}

// ---------- INDIKATORLER ----------
function wmaSeries(a,len){ const n=0.5*len*(len+1); return a.map((_,i,ar)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=ar[i-j]*(len-j); return s/n; }); }
function calcHMA(a,p=21){ if(a.length<p)return null; const wh=wmaSeries(a,Math.floor(p/2)), wf=wmaSeries(a,p);
  const diff=wh.map((x,i)=>(x!=null&&wf[i]!=null)?2*x-wf[i]:null).filter(x=>x!=null); return wmaSeries(diff,Math.max(2,Math.floor(Math.sqrt(p)))); }
function calcVWAP(h,p=24){ if(h.length<10)return null; let sPV=0,sV=0; h.slice(-p).forEach(x=>{ const t=(x.h+x.l+x.c)/3; sPV+=t*x.v; sV+=x.v; }); return sV===0?h[h.length-1].c:sPV/sV; }
function calcADX(h,p=14){ if(h.length<p*2)return 20; const tr=[],pdm=[],ndm=[];
  for(let i=1;i<h.length;i++){ tr.push(Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c)));
    const up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); }
  const sm=arr=>{ const r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; };
  const trS=sm(tr),pdmS=sm(pdm),ndmS=sm(ndm);
  const dx=trS.map((t,i)=>{ if(t===0)return 0; const pdi=100*pdmS[i]/t, ndi=100*ndmS[i]/t, sum=pdi+ndi; return sum===0?0:100*Math.abs(pdi-ndi)/sum; });
  return dx.slice(-p).reduce((a,b)=>a+b,0)/p; }
function ema(arr,p){ if(!arr||arr.length<p)return null; const k=2/(p+1); let e=arr.slice(0,p).reduce((a,b)=>a+b,0)/p; for(let i=p;i<arr.length;i++) e=arr[i]*k+e*(1-k); return e; }
function legUp(closes){ if(!closes||closes.length<30) return null; const e=ema(closes,20); return e? closes[closes.length-1]>e : null; }
function clampN(x,a,b){ return Math.max(a,Math.min(b,x)); }
function calcATR(h,p=14){ if(!h||h.length<p+1)return null; let s=0; for(let i=h.length-p;i<h.length;i++) s+=Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c)); return s/p; }

function analyze(b){
  const h=hist[b]; if(!h||h.length<50) return {score:0,trendUp:false,atrPct:null,px:prices[b]||0};
  const cArr=h.map(x=>x.c), px=cArr[cArr.length-1];
  const vwap=calcVWAP(h,VWAP_LEN), hma=calcHMA(cArr), adx=calcADX(h);
  const atr=calcATR(h,14), atrPct=(atr&&px>0)?clampN(atr/px*100,1.0,8.0):2.0;
  
  const aboveVwap=vwap?px>vwap:false;
  const hmaUp=!!(hma&&hma.length>=2&&hma[hma.length-1]!=null&&hma[hma.length-2]!=null&&hma[hma.length-1]>hma[hma.length-2]);
  const htfUp=(CFG.htf && hist4h[b]) ? legUp(hist4h[b]) : true;
  const overbought=(chg[b]||0)>=CFG.pumpMax;
  
  let score=0;
  if(aboveVwap) score+=1; 
  if(hmaUp)     score+=1; 
  if(htfUp)     score+=1;

  // YENİ KURAL: ADX 25'in üzerinde olmalı VE 3 indikatörün en az 2'si olumlu olmalı
  const trendUp = (adx >= 25) && (score >= 2) && !overbought;
  
  return {score,trendUp,adx,atrPct,aboveVwap,hmaUp,htfUp,overbought,px};
}
function refreshAnalysis(){ for(const u of universe){ ana[u.base]=analyze(u.base); } }

// ---------- EMIRLER ----------
function mkSym(base){ return base+'/'+CFG.quote; }
function roundAmt(base, amt){ try{ return parseFloat(tradeEx.amountToPrecision(mkSym(base), amt)); }catch(e){ return amt; } }
function minCostOf(base){ try{ const m=tradeEx.market(mkSym(base)); return (m&&m.limits&&m.limits.cost&&m.limits.cost.min)||CFG.minNotional; }catch(e){ return CFG.minNotional; } }

async function placeBuy(base, cost){
  const px=prices[base]; if(!px||px<=0) return;
  const a=ana[base]||{}; const atrPct=a.atrPct||2.0;
  if(CFG.maxTrade>0) cost=Math.min(cost,CFG.maxTrade);
  if(cost>S.cash) cost=S.cash;
  if(cost<1) return;
  if(S.mode==='paper'){
    const fillPx=px*(1+CFG.slip), fee=cost*CFG.fee, qty=(cost-fee)/fillPx; S.cash-=cost;
    S.positions[base]={qty,cost,entry:fillPx,high:fillPx,openTs:Date.now(),atrPct}; S.trades++;
    log('buy','AL '+base, `${cost.toFixed(2)} ${CFG.quote} @ ${fillPx.toFixed(8)}`);
  } else if(tradeEx){
    try{
      let cst=Math.floor(cost*100)/100;
      try{ if(tradeEx.costToPrecision) cst=parseFloat(tradeEx.costToPrecision(mkSym(base),cost)); }catch(e){}
      const minC=minCostOf(base); if(cst<minC){ return; }
      const o=await tradeEx.createMarketBuyOrderWithCost(mkSym(base), cst);
      const qty=o.filled||o.amount||0, spent=o.cost||cst, avg=o.average||px;
      S.cash-=spent; S.positions[base]={qty,cost:spent,entry:avg,high:avg,openTs:Date.now(),atrPct}; S.trades++;
      log('buy','AL '+base, `Gercek emir ${spent.toFixed(2)} ${CFG.quote} @ ${avg}`);
    }catch(e){ log('warn','HATA','Alim '+base+': '+(e.message||e)); }
  }
  broadcast();
}
async function placeSell(base, why){
  const p=S.positions[base]; if(!p) return; 
  const px=prices[base]||p.entry; const sellQty=p.qty, costPart=p.cost;
  if(S.mode==='paper'){
    const fillPx=px*(1-CFG.slip), gross=sellQty*fillPx, fee=gross*CFG.fee, net=gross-fee; S.cash+=net;
    const pnl=net-costPart, pct=costPart>0?pnl/costPart*100:0;
    delete S.positions[base]; cooldown[base]=Date.now()+CFG.cooldownMin*60000;
    recordClosed(base,pnl,pct,why);
    log(pnl>=0?'sell-win':'sell-loss','SAT '+base, `@ ${fillPx.toFixed(8)} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} ${CFG.quote} (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
  } else if(tradeEx){
    try{
      let qty=roundAmt(base, sellQty); if(!(qty>0)){ return; }
      const minC=minCostOf(base);
      if(qty*px < minC){ qty=roundAmt(base, p.qty); }
      const o=await tradeEx.createMarketSellOrder(mkSym(base), qty);
      const gross=o.cost||qty*px, fee=(o.fee&&o.fee.cost)||gross*CFG.fee, net=gross-fee; S.cash+=net;
      const pnl=net-costPart, pct=costPart>0?pnl/costPart*100:0;
      delete S.positions[base]; cooldown[base]=Date.now()+CFG.cooldownMin*60000;
      recordClosed(base,pnl,pct,why);
      log(pnl>=0?'sell-win':'sell-loss','SAT '+base, `@ ${px} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} ${CFG.quote} (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
    }catch(e){ log('warn','HATA','Satis '+base+': '+(e.message||e)); }
  }
  broadcast();
}

// ---------- STRATEJI ----------
function strategy(){
  if(dayKey()!==S.dayKey){ S.dayKey=dayKey(); S.dayStartEquity=equity(); if(!S.killed) log('info','Yeni gun','Sayac sifirlandi.'); }
  const eq=equity(); if(eq>S.peak)S.peak=eq;
  if(!S.killed && eq <= S.dayStartEquity*(1-CFG.dailyLossStop)){
    S.killed=true; S.running=false;
    for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'kill-switch'));
  }
  
  // CIKISLAR (YENI: Katı Iz Suren Stop ve Zarar Kes)
  for(const b of Object.keys(S.positions)){
    const p=S.positions[b], px=prices[b]; if(!px) continue; if(px>p.high)p.high=px;
    const ap = p.atrPct || 2.0;
    
    const stopPct = ap * 2.0;           // Kural 1: %2x ATR Zarar Kes
    const trailAct = ap * 2.5;          // Kural 2: Kâr %2.5x ATR'ye ulaştığında iz sürmeye başla
    const trailDist = ap * 2.0;         // Kural 3: Zirveden %2x ATR aşağısını stop kabul et
    
    const netPct=((px*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    const peakPct=((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    
    // Ani Çöküş Koruması
    const ref=prevPx[b]; const fastRet=(ref&&ref>0)?(px/ref-1)*100:0;
    if(fastRet <= -(ap * 1.5)){ queue(()=>placeSell(b, `Ani Çöküş (${fastRet.toFixed(1)}%)`)); continue; }
    
    let why=null;
    
    // İz Süren Stop (Kârı Koru)
    if (peakPct >= trailAct) {
        const trailingStopLevel = peakPct - trailDist;
        if (netPct <= trailingStopLevel) why = `İz Süren Stop (Tepe:%${peakPct.toFixed(1)})`;
    } 
    // Sert Zarar Kes (Stop Loss)
    else if (netPct <= -stopPct) {
        why = `Zarar Kes (%${netPct.toFixed(1)})`;
    }
    // Başa Baş (Break-even) Koruması (Biraz kara gecince zarar etmeden cikis)
    else if (peakPct >= (ap*1.5) && netPct <= 0.2) {
        why = `Başa Baş Koruması`;
    }
    
    if(why) queue(()=>placeSell(b, why));
  }
  
  // GIRISLER
  if(!S.running||S.killed) return;
  if(!dataReady || Date.now()<warmupUntil) return; 
  let open=Object.keys(S.positions).length, avail=S.cash;
  const cands=[];
  for(const u of universe){ const b=u.base, a=ana[b]; if(!a) continue;
    if(S.positions[b]||!(prices[b]>0)||(cooldown[b]&&Date.now()<=cooldown[b])) continue;
    if(!hist[b]||hist[b].length<50) continue; 
    
    if(a.trendUp) cands.push({b,rank:a.adx + (a.score*10)});
  }
  cands.sort((x,y)=>y.rank-x.rank);
  let placed=0;
  for(const c of cands){ if(open>=CFG.maxPositions || placed>=CFG.maxNewPerTick) break;
    const perSlot = CFG.investTarget / CFG.maxPositions; 
    let alloc = equity() * perSlot; 
    alloc = Math.min(alloc, equity()*CFG.maxFrac, avail*0.95);
    if(CFG.maxTrade>0) alloc=Math.min(alloc,CFG.maxTrade);
    if(alloc < CFG.minNotional){ if(avail >= CFG.minNotional) alloc=CFG.minNotional; else continue; } 
    alloc = Math.round(alloc*100)/100;
    queue(()=>placeBuy(c.b, alloc)); open++; placed++; avail-=alloc;
  }
}

// ---------- API / SUNUCU ----------
function statsOf(){ const c=S.closed; let w=0,l=0,sw=0,sl=0,swp=0,slp=0,tot=0;
  for(const t of c){ tot+=t.pnl; if(t.pnl>=0){w++;sw+=t.pnl;swp+=t.pct;} else {l++;sl+=Math.abs(t.pnl);slp+=t.pct;} }
  const n=c.length; return { closed:n, wins:w, losses:l, winRate:n?w/n*100:0, avgWinPct:w?swp/w:0, avgLossPct:l?slp/l:0, pf: sl>0?sw/sl:(sw>0?99:0), totalPnl:tot }; }
function snapshot(){
  const eq=equity();
  const positions=Object.entries(S.positions).map(([b,p])=>{ const px=prices[b]||p.entry, val=p.qty*px;
    return { base:b, entry:p.entry, price:px, cost:p.cost, qty:p.qty, upnl:val-p.cost-(p.cost*CFG.fee*2) }; });
  return { mode:S.mode, running:S.running, equity:eq, pnlPct:(eq-CFG.startCash)/CFG.startCash*100, cash:S.cash, positions, stats:statsOf() };
}
const app=express(); app.use(express.json());
const auth=(req,res,next)=>{ const t=req.headers['x-token']||req.query.token; if(t!==CFG.token) return res.status(401).json({error:'token'}); next(); };
app.get('/api/state', auth, (req,res)=>res.json(snapshot()));
const server=http.createServer(app);
const wss=new WebSocketServer({ server, path:'/ws' });
wss.on('connection',(socket,req)=>{ let t=''; try{ t=new URL(req.url,'http://x').searchParams.get('token'); }catch(e){}
  if(t!==CFG.token){ socket.close(); return; } socket.send(JSON.stringify(snapshot())); });
function broadcast(){ let m; try{ m=JSON.stringify(snapshot()); }catch(e){ return; } wss.clients.forEach(c=>{ if(c.readyState===1) c.send(m); }); }

// ---------- BASLAT ----------
function start(){
  log('info','Baslatiliyor', `mod=${CFG.mode} · kasa=$${CFG.startCash} · mum=${CFG.interval}`);
  loadState();
  server.listen(CFG.port, ()=>log('info','Panel hazir', `port ${CFG.port} dinleniyor (token ile gir)`));
  initData();
}
async function initData(){
  try{
    if(CFG.mode!=='paper'){
      try{ const mod=await import('ccxt'); const ccxt=mod.default||mod;
        tradeEx=new ccxt.binance({apiKey:CFG.key,secret:CFG.secret,enableRateLimit:true,options:{defaultType:'spot',createMarketBuyOrderRequiresPrice:false}});
        if(CFG.mode==='testnet') tradeEx.setSandboxMode(true);
        await tradeEx.loadMarkets();
        try{ const bal=await tradeEx.fetchBalance(); const free=bal.free&&bal.free[CFG.quote]; if(typeof free==='number'){ S.cash=free; } }catch(e){}
      }catch(e){}
    }
    await buildUniverse();
  }catch(e){}
  setInterval(loopTick, CFG.pollMs);
  setInterval(saveState, 15000);
  loopTick();
}
async function loopTick(){
  try{
    if(!universe.length){ await buildUniverse(); }
    await refreshPrices(); refreshAnalysis(); strategy();
    for(const u of universe){ if(prices[u.base]>0) prevPx[u.base]=prices[u.base]; } 
    const now=Date.now(); if(now-(S._lastEq||0)>20000){ S._lastEq=now; S.equityHist.push(equity()); if(S.equityHist.length>2000) S.equityHist.shift(); }
  }catch(e){ } broadcast();
}
start();
