'use strict';
/* Survive & Grow — Binance Hesap Yoneticisi
 * Veri: Binance (data-api.binance.vision) · Strateji: HMA + VWAP + ADX (5m) + 1h trend onayi
 * Filtreler: trend yukari + asiri-pump engeli · Cikis: kismi kar + zirveden donus + basabas + sert stop
 * Gerceklik: komisyon + slipaj · Live: Binance lot/min-notional yuvarlama · Gun-ici kill-switch
 * Panel: token korumali REST + WS · elle al (coin detay+tutar) · sermaye egrisi + istatistik + CSV
 * YATIRIM TAVSIYESI DEGILDIR. Once paper -> testnet -> kucuk live.
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
  maxPositions: +(process.env.MAX_POSITIONS || 6),
  univMax: +(process.env.UNIVERSE || 75),
  pollMs: +(process.env.POLL_MS || 6000),
  fee: +(process.env.FEE || 0.001),                   // komisyon (tek yon). BNB ile ~0.00075
  slip: +(process.env.SLIP || 0.0005),                // market emir slipaj tahmini (tek yon)
  entryScore: +(process.env.ENTRY_SCORE || 0.40),     // trend skor esigi (maks 0.60)
  htf: (process.env.HTF || '1') !== '0',              // 1h trend onayi acik/kapali
  pumpMax: +(process.env.PUMP_MAX || 30),             // 24s %X uzeri pompalandiysa alma
  tp1Pct: +(process.env.TP1_PCT || 1.5),              // kismi kar seviyesi (net %)
  tp1Frac: +(process.env.TP1_FRAC || 0.5),            // kismi karda satilacak oran
  dailyLossStop: +(process.env.DAILY_LOSS_STOP || 0.15),
  minNotional: +(process.env.MIN_NOTIONAL || 10),
  maxTrade: +(process.env.MAX_TRADE_USDT || 0),       // 0 = sinirsiz
  cooldownMin: +(process.env.COOLDOWN_MIN || 15),
  key: process.env.BINANCE_KEY || '',
  secret: process.env.BINANCE_SECRET || '',
};
const BN = 'https://data-api.binance.vision';
const STATE_FILE = path.join(__dirname, 'state.json');
const STABLES = ['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD','PYUSD','EUR','TRY','GBP','AEUR','XUSD'];
function dayKey(){ return new Date().toISOString().slice(0,10); }

let S = {
  mode: CFG.mode, running: true, killed: false, cash: CFG.startCash, positions: {}, trades: 0,
  peak: CFG.startCash, dayStartEquity: CFG.startCash, dayKey: dayKey(),
  equityHist: [CFG.startCash], closed: [], log: [], startedAt: Date.now(), lastError: '', _lastEq: 0,
};
process.on('unhandledRejection', e=>{ try{S.lastError='unhandled: '+String(e&&e.message||e);}catch(_){} console.error('unhandledRejection:', e&&e.message||e); });
process.on('uncaughtException',  e=>{ try{S.lastError='uncaught: '+String(e&&e.message||e);}catch(_){} console.error('uncaughtException:', e&&e.message||e); });

let prices={}, chg={}, hist={}, hist1h={}, universe=[], cooldown={}, ana={}, lastReseed=0, lastLoopErr='';
let tradeEx=null;
let q=Promise.resolve(); function queue(fn){ q=q.then(fn).catch(e=>log('warn','Kuyruk',String(e&&e.message||e))); return q; }

function log(type,action,detail){ S.log.unshift({type,action,detail,ts:Date.now()}); if(S.log.length>140) S.log.pop(); }
function equity(){ let e=S.cash; for(const b in S.positions){ const p=S.positions[b]; e+=p.qty*(prices[b]||p.entry); } return e; }
function isStable(b){ return STABLES.includes(b); }
function saveState(){ try{ fs.writeFileSync(STATE_FILE, JSON.stringify({cash:S.cash,positions:S.positions,trades:S.trades,peak:S.peak,dayStartEquity:S.dayStartEquity,dayKey:S.dayKey,killed:S.killed,running:S.running,equityHist:S.equityHist.slice(-2000),closed:S.closed.slice(-1000)})); }catch(e){} }
function loadState(){ try{ const j=JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); Object.assign(S,j); if(!Array.isArray(S.equityHist))S.equityHist=[CFG.startCash]; if(!Array.isArray(S.closed))S.closed=[]; log('info','Durum','Onceki durum yuklendi.'); }catch(e){} }
function recordClosed(base,pnl,pct,why){ S.closed.push({base,pnl,pct,why,ts:Date.now()}); if(S.closed.length>1000) S.closed.shift(); }

// ---------- VERI (Binance) ----------
async function getJSON(url){ const r=await fetch(url,{headers:{'accept':'application/json','user-agent':'sg-bot/1.0'}}); if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); }
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
    try{ const k=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=5m&limit=300');
      if(Array.isArray(k)&&k.length) hist[u.base]=k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]})); }catch(e){}
    if(CFG.htf){ try{ const k1=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=1h&limit=120');
      if(Array.isArray(k1)&&k1.length) hist1h[u.base]=k1.map(x=>+x[4]); }catch(e){} }
  }
  lastReseed=Date.now();
  log('info','Gecmis','5dk + 1h mumlar yuklendi — indikatorler hazir.');
}
async function refreshPrices(){
  if(!universe.length) return;
  const syms=encodeURIComponent(JSON.stringify(universe.map(u=>u.sym)));
  const d=await getJSON(BN+'/api/v3/ticker/24hr?symbols='+syms);
  for(const t of d){ const base=t.symbol.slice(0,-CFG.quote.length); const p=parseFloat(t.lastPrice);
    if(!isFinite(p)||p<=0) continue; prices[base]=p; chg[base]=parseFloat(t.priceChangePercent)||chg[base]||0;
    const h=hist[base]; if(h&&h.length){ const last=h[h.length-1]; last.c=p; if(p>last.h)last.h=p; if(p<last.l)last.l=p; } }
  if(Date.now()-lastReseed > 5*60*1000) seedHistory().catch(()=>{});
}

// ---------- INDIKATORLER ----------
function wmaSeries(a,len){ const n=0.5*len*(len+1); return a.map((_,i,ar)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=ar[i-j]*(len-j); return s/n; }); }
function calcHMA(a,p=55){ if(a.length<p)return null; const wh=wmaSeries(a,Math.floor(p/2)), wf=wmaSeries(a,p);
  const diff=wh.map((x,i)=>(x!=null&&wf[i]!=null)?2*x-wf[i]:null).filter(x=>x!=null); return wmaSeries(diff,Math.max(2,Math.floor(Math.sqrt(p)))); }
function calcVWAP(h,p=288){ if(h.length<10)return null; let sPV=0,sV=0; h.slice(-p).forEach(x=>{ const t=(x.h+x.l+x.c)/3; sPV+=t*x.v; sV+=x.v; }); return sV===0?h[h.length-1].c:sPV/sV; }
function calcADX(h,p=14){ if(h.length<p*2)return 20; const tr=[],pdm=[],ndm=[];
  for(let i=1;i<h.length;i++){ tr.push(Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c)));
    const up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); }
  const sm=arr=>{ const r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; };
  const trS=sm(tr),pdmS=sm(pdm),ndmS=sm(ndm);
  const dx=trS.map((t,i)=>{ if(t===0)return 0; const pdi=100*pdmS[i]/t, ndi=100*ndmS[i]/t, sum=pdi+ndi; return sum===0?0:100*Math.abs(pdi-ndi)/sum; });
  return dx.slice(-p).reduce((a,b)=>a+b,0)/p; }
function ema(arr,p){ if(!arr||arr.length<p)return null; const k=2/(p+1); let e=arr.slice(0,p).reduce((a,b)=>a+b,0)/p; for(let i=p;i<arr.length;i++) e=arr[i]*k+e*(1-k); return e; }
function htfUpOf(b){ if(!CFG.htf) return true; const c=hist1h[b]; if(!c||c.length<55) return true; const e=ema(c,50); return e? c[c.length-1]>e : true; }

function analyze(b){
  const h=hist[b]; if(!h||h.length<100) return {score:0,trendUp:false,adx:0,aboveVwap:false,hmaUp:false,htfUp:htfUpOf(b),overbought:(chg[b]||0)>=CFG.pumpMax,px:prices[b]||0};
  const cArr=h.map(x=>x.c), px=cArr[cArr.length-1];
  const vwap=calcVWAP(h), hma=calcHMA(cArr), adx=calcADX(h);
  const aboveVwap = vwap? px>vwap : false;
  const hmaUp = !!(hma && hma.length>=2 && hma[hma.length-1]!=null && hma[hma.length-2]!=null && hma[hma.length-1]>hma[hma.length-2]);
  const htfUp = htfUpOf(b);
  const overbought = (chg[b]||0) >= CFG.pumpMax;
  let score=0;
  if(aboveVwap) score+=0.20; else score-=0.20;
  if(hmaUp)     score+=0.20; else score-=0.25;
  if(adx>25)    score+=0.20; else if(adx<20) score-=0.30;
  const trendUp = (score>=CFG.entryScore) && htfUp && !overbought;
  return {score, trendUp, adx, aboveVwap, hmaUp, htfUp, overbought, px};
}
function refreshAnalysis(){ for(const u of universe){ ana[u.base]=analyze(u.base); } }

// ---------- EMIRLER (komisyon + slipaj + live yuvarlama) ----------
function mkSym(base){ return base+'/'+CFG.quote; }
function roundAmt(base, amt){ try{ return parseFloat(tradeEx.amountToPrecision(mkSym(base), amt)); }catch(e){ return amt; } }
function minCostOf(base){ try{ const m=tradeEx.market(mkSym(base)); return (m&&m.limits&&m.limits.cost&&m.limits.cost.min)||CFG.minNotional; }catch(e){ return CFG.minNotional; } }

async function placeBuy(base, cost){
  const px=prices[base]; if(!px||px<=0) return;
  if(CFG.maxTrade>0) cost=Math.min(cost,CFG.maxTrade);
  if(cost>S.cash) cost=S.cash;
  if(cost<1) return;
  if(S.mode==='paper'){
    const fillPx=px*(1+CFG.slip), fee=cost*CFG.fee, qty=(cost-fee)/fillPx; S.cash-=cost;
    S.positions[base]={qty,cost,entry:fillPx,high:fillPx,openTs:Date.now(),tp1done:false}; S.trades++;
    log('buy','AL '+base, `${cost.toFixed(2)} ${CFG.quote} @ ${fillPx.toFixed(8)} (slipaj+komisyon dahil)`);
  } else if(tradeEx){
    try{
      const minC=minCostOf(base); if(cost<minC){ log('warn','Atlandi', base+' min emir '+minC+' '+CFG.quote); return; }
      const o=await tradeEx.createMarketBuyOrderWithCost(mkSym(base), cost);
      const qty=o.filled||o.amount||0, spent=o.cost||cost, avg=o.average||px;
      S.cash-=spent; S.positions[base]={qty,cost:spent,entry:avg,high:avg,openTs:Date.now(),tp1done:false}; S.trades++;
      log('buy','AL '+base, `Gercek emir ${spent.toFixed(2)} ${CFG.quote} @ ${avg}`);
    }catch(e){ log('warn','HATA','Alim '+base+': '+(e.message||e)); }
  }
  broadcast();
}
async function placeSell(base, why, frac){
  const p=S.positions[base]; if(!p) return; frac=Math.min(1,Math.max(0,frac||1));
  const px=prices[base]||p.entry; const sellQty=p.qty*frac, costPart=p.cost*frac;
  const partial = frac<0.999;
  if(S.mode==='paper'){
    const fillPx=px*(1-CFG.slip), gross=sellQty*fillPx, fee=gross*CFG.fee, net=gross-fee; S.cash+=net;
    const pnl=net-costPart, pct=costPart>0?pnl/costPart*100:0;
    if(partial){ p.qty-=sellQty; p.cost-=costPart; p.tp1done=true; }
    else { delete S.positions[base]; cooldown[base]=Date.now()+CFG.cooldownMin*60000; }
    recordClosed(base,pnl,pct,why);
    log(pnl>=0?'sell-win':'sell-loss',(partial?'KISMI ':'SAT ')+base, `@ ${fillPx.toFixed(8)} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} ${CFG.quote} (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
  } else if(tradeEx){
    try{
      const qty=roundAmt(base, sellQty); if(!(qty>0)){ return; }
      const o=await tradeEx.createMarketSellOrder(mkSym(base), qty);
      const gross=o.cost||qty*px, fee=(o.fee&&o.fee.cost)||gross*CFG.fee, net=gross-fee; S.cash+=net;
      const pnl=net-costPart, pct=costPart>0?pnl/costPart*100:0;
      if(partial){ p.qty-=sellQty; p.cost-=costPart; p.tp1done=true; }
      else { delete S.positions[base]; cooldown[base]=Date.now()+CFG.cooldownMin*60000; }
      recordClosed(base,pnl,pct,why);
      log(pnl>=0?'sell-win':'sell-loss',(partial?'KISMI ':'SAT ')+base, `@ ${px} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} ${CFG.quote} (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
    }catch(e){ log('warn','HATA','Satis '+base+': '+(e.message||e)); }
  }
  broadcast();
}
function stopLevelOf(p){
  const peakPct=((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
  if(peakPct>=2.5) return p.entry*(1+(peakPct-0.8)/100);
  if(peakPct>=1.2) return p.entry*(1+0.2/100);
  return p.entry*(1-0.03);
}

// ---------- STRATEJI ----------
function strategy(){
  if(dayKey()!==S.dayKey){ S.dayKey=dayKey(); S.dayStartEquity=equity(); if(!S.killed) log('info','Yeni gun','Gun-ici kayip sayaci sifirlandi.'); }
  const eq=equity(); if(eq>S.peak)S.peak=eq;
  if(!S.killed && eq <= S.dayStartEquity*(1-CFG.dailyLossStop)){
    S.killed=true; S.running=false;
    log('warn','KILL-SWITCH', `Gun-ici kayip %${(CFG.dailyLossStop*100).toFixed(0)} asildi. Tum pozisyonlar kapatiliyor, bot durduruldu.`);
    for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'kill-switch',1));
  }
  // CIKISLAR
  for(const b of Object.keys(S.positions)){
    const p=S.positions[b], px=prices[b]; if(!px) continue; if(px>p.high)p.high=px;
    const netPct=((px*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    const peakPct=((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    if(!p.tp1done && netPct>=CFG.tp1Pct){ p.tp1done=true; queue(()=>placeSell(b, `kismi kar +%${netPct.toFixed(2)}`, CFG.tp1Frac)); continue; }
    let why=null;
    if(peakPct>=2.5 && netPct<=peakPct-0.8) why='zirveden donus (kar kilitlendi)';
    else if(peakPct>=1.2 && netPct<=0.2)    why='basabas korumasi';
    else if(netPct<=-3.0)                    why='zarar-kes';
    if(why) queue(()=>placeSell(b, `${why} (%${netPct.toFixed(2)})`, 1));
  }
  // GIRISLER
  if(!S.running||S.killed) return;
  let open=Object.keys(S.positions).length, avail=S.cash;
  const cand=universe.map(u=>u.base)
    .filter(b=>!S.positions[b] && prices[b]>0 && (!cooldown[b]||Date.now()>cooldown[b]) && ana[b] && ana[b].trendUp)
    .sort((x,y)=>(ana[y].score)-(ana[x].score));
  for(const b of cand){ if(open>=CFG.maxPositions) break;
    let target=Math.max(equity()/CFG.maxPositions, CFG.minNotional);
    let alloc=Math.min(target, avail*0.95);
    if(CFG.maxTrade>0) alloc=Math.min(alloc,CFG.maxTrade);
    if(alloc < CFG.minNotional) continue;
    queue(()=>placeBuy(b, alloc)); open++; avail-=alloc;
  }
}

// ---------- ELLE AL: COIN DETAY ----------
async function quoteCoin(base){
  base=base.toUpperCase().replace(new RegExp(CFG.quote+'$'),''); const sym=base+CFG.quote;
  const inU=universe.some(u=>u.base===base);
  if(!hist[base] || hist[base].length<100){
    const k=await getJSON(BN+'/api/v3/klines?symbol='+sym+'&interval=5m&limit=300');
    if(!Array.isArray(k)||!k.length) throw new Error('coin bulunamadi: '+sym);
    hist[base]=k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]}));
  }
  if(CFG.htf && (!hist1h[base]||hist1h[base].length<55)){ try{ const k1=await getJSON(BN+'/api/v3/klines?symbol='+sym+'&interval=1h&limit=120'); if(Array.isArray(k1))hist1h[base]=k1.map(x=>+x[4]); }catch(e){} }
  if(prices[base]==null){ try{ const t=await getJSON(BN+'/api/v3/ticker/24hr?symbol='+sym); prices[base]=parseFloat(t.lastPrice); chg[base]=parseFloat(t.priceChangePercent); }catch(e){} }
  const a=analyze(base);
  let why=[]; if(!a.htfUp)why.push('1h trend asagi'); if(a.overbought)why.push(`24s +%${(chg[base]||0).toFixed(0)} asiri pump`); if(a.score<CFG.entryScore)why.push('skor dusuk');
  return { base, sym, price:prices[base]||a.px||0, chg:chg[base]||0, score:a.score, trendUp:a.trendUp,
    adx:Math.round(a.adx), aboveVwap:a.aboveVwap, hmaUp:a.hmaUp, htfUp:a.htfUp, overbought:a.overbought, inUniverse:inU,
    suggest: a.trendUp ? 'Trend YUKARI — sinyal alima uygun' : ('Sistem normalde almaz ('+(why.join(', ')||'zayif')+') — elle alabilirsin') };
}

// ---------- ISTATISTIK + PANEL VERI ----------
function statsOf(){ const c=S.closed; let wins=0,losses=0,sw=0,sl=0,swp=0,slp=0,tot=0;
  for(const t of c){ tot+=t.pnl; if(t.pnl>=0){wins++;sw+=t.pnl;swp+=t.pct;} else {losses++;sl+=Math.abs(t.pnl);slp+=t.pct;} }
  const n=c.length; return { closed:n, wins, losses, winRate:n?wins/n*100:0, avgWinPct:wins?swp/wins:0, avgLossPct:losses?slp/losses:0, pf: sl>0?sw/sl:(sw>0?99:0), totalPnl:tot }; }
function snapshot(){
  const eq=equity();
  const positions=Object.entries(S.positions).map(([b,p])=>{ const px=prices[b]||p.entry, val=p.qty*px;
    return { base:b, entry:p.entry, price:px, cost:p.cost, qty:p.qty, upnl:val-p.cost-(p.cost*CFG.fee*2), stopLvl:stopLevelOf(p), tp1done:!!p.tp1done }; });
  const held=new Set(Object.keys(S.positions));
  const market=universe.map(u=>{ const a=ana[u.base]||{score:0}; return {base:u.base, price:prices[u.base]||0, chg:chg[u.base]||0, score:a.score||0, held:held.has(u.base)}; })
    .sort((x,y)=>y.score-x.score).slice(0,24);
  const eh=S.equityHist, step=Math.max(1,Math.ceil(eh.length/120)), equitySeries=eh.filter((_,i)=>i%step===0||i===eh.length-1);
  return { mode:S.mode, running:S.running, killed:S.killed, quote:CFG.quote, start:CFG.startCash,
    equity:eq, pnlPct:(eq-CFG.startCash)/CFG.startCash*100, cash:S.cash, trades:S.trades,
    univ:universe.length, positions, market, stats:statsOf(), equitySeries, log:S.log.slice(0,60) };
}

// ---------- SUNUCU ----------
const app=express(); app.use(express.json());
const auth=(req,res,next)=>{ const t=req.headers['x-token']||req.query.token; if(t!==CFG.token) return res.status(401).json({error:'token'}); next(); };
app.get('/', (req,res)=>res.sendFile(path.join(__dirname,'index.html')));
app.get('/api/state', auth, (req,res)=>res.json(snapshot()));
app.get('/api/trades.csv', auth, (req,res)=>{ const rows=[['zaman','coin','sebep','pnl_'+CFG.quote,'pnl_pct']];
  for(const c of S.closed) rows.push([new Date(c.ts).toISOString(), c.base, '"'+String(c.why).replace(/"/g,'')+'"', c.pnl.toFixed(4), c.pct.toFixed(2)]);
  res.setHeader('Content-Type','text/csv; charset=utf-8'); res.setHeader('Content-Disposition','attachment; filename=trades.csv'); res.send(rows.map(r=>r.join(',')).join('\n')); });
app.post('/api/pause',  auth, (req,res)=>{ S.running=false; log('info','Manuel','Duraklatildi.'); res.json({ok:true}); broadcast(); });
app.post('/api/resume', auth, (req,res)=>{ S.running=true; if(S.killed){S.killed=false; S.dayStartEquity=equity();} log('info','Manuel','Devam.'); res.json({ok:true}); broadcast(); });
app.post('/api/reset',  auth, (req,res)=>{ S.cash=CFG.startCash; S.positions={}; S.trades=0; S.peak=CFG.startCash; S.dayStartEquity=CFG.startCash; S.dayKey=dayKey(); S.killed=false; S.running=true; S.equityHist=[CFG.startCash]; S.closed=[]; S.log=[]; cooldown={}; try{fs.unlinkSync(STATE_FILE);}catch(e){} log('info','Manuel','Sifirlandi — yeni $'+CFG.startCash+' kasa.'); res.json({ok:true}); broadcast(); });
app.post('/api/close-all', auth, (req,res)=>{ for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'manuel kapat',1)); log('info','Manuel','Hepsi kapatiliyor.'); res.json({ok:true}); });
app.post('/api/panic', auth, (req,res)=>{ S.running=false; for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'PANIK',1)); log('warn','Manuel','PANIK — hepsi kapatildi + durduruldu.'); res.json({ok:true}); });
app.post('/api/quote', auth, async (req,res)=>{ const b=(req.body.symbol||'').toUpperCase(); if(!b) return res.status(400).json({error:'symbol'}); try{ res.json(await quoteCoin(b)); }catch(e){ res.status(400).json({error:String(e.message||e)}); } });
app.post('/api/buy', auth, async (req,res)=>{ const b=(req.body.symbol||'').toUpperCase().replace(new RegExp(CFG.quote+'$'),''); if(!b) return res.status(400).json({error:'symbol'});
  let usd=parseFloat(req.body.usdt); if(!isFinite(usd)||usd<=0) usd=Math.min(equity()/CFG.maxPositions, S.cash*0.95);
  if(prices[b]==null){ try{ await quoteCoin(b); }catch(e){ return res.status(400).json({error:String(e.message||e)}); } }
  if(usd>S.cash) return res.status(400).json({error:'nakit yetersiz'});
  if(usd<1) return res.status(400).json({error:'tutar cok kucuk'});
  queue(()=>placeBuy(b, usd)); log('info','Manuel','Elle ALIM: '+b+' ~$'+usd.toFixed(2)); res.json({ok:true}); });
app.post('/api/sell', auth, (req,res)=>{ const b=(req.body.symbol||'').toUpperCase(); if(!S.positions[b]) return res.status(400).json({error:'pozisyon yok'}); queue(()=>placeSell(b,'manuel',1)); log('info','Manuel','Elle SATIM: '+b); res.json({ok:true}); });

const server=http.createServer(app);
const wss=new WebSocketServer({ server, path:'/ws' });
wss.on('connection',(socket,req)=>{ let t=''; try{ t=new URL(req.url,'http://x').searchParams.get('token'); }catch(e){}
  if(t!==CFG.token){ socket.close(); return; } socket.send(JSON.stringify(snapshot())); });
function broadcast(){ let m; try{ m=JSON.stringify(snapshot()); }catch(e){ return; } wss.clients.forEach(c=>{ if(c.readyState===1) c.send(m); }); }

// ---------- BASLAT ----------
function start(){
  log('info','Baslatiliyor', `mod=${CFG.mode} · kasa=$${CFG.startCash} · evren=${CFG.univMax} · maxPoz=${CFG.maxPositions} · 1hOnay=${CFG.htf?'acik':'kapali'}`);
  if(CFG.mode==='live') log('warn','GERCEK PARA','LIVE modundasin — gercek fonlarla islem yapilacak. Kucuk basla.');
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
        try{ const bal=await tradeEx.fetchBalance(); const free=bal.free&&bal.free[CFG.quote]; if(typeof free==='number'){ S.cash=free; log('info','Bakiye','Serbest '+CFG.quote+': '+free.toFixed(2)); } }catch(e){ log('warn','Bakiye okunamadi',String(e.message||e)); }
      }catch(e){ S.lastError='ccxt: '+String(e&&e.message||e); log('warn','ccxt yuklenemedi', S.lastError); }
    }
    await buildUniverse();
  }catch(e){ S.lastError=String(e&&e.message||e); log('warn','Veri hatasi', S.lastError+' — dongude tekrar denenecek.'); }
  setInterval(loopTick, CFG.pollMs);
  setInterval(saveState, 15000);
  loopTick();
}
async function loopTick(){
  try{
    if(!universe.length){ await buildUniverse(); }
    await refreshPrices();
    refreshAnalysis();
    strategy();
    const now=Date.now(); if(now-(S._lastEq||0)>20000){ S._lastEq=now; S.equityHist.push(equity()); if(S.equityHist.length>2000) S.equityHist.shift(); }
  }catch(e){ const m=String(e&&e.message||e); S.lastError=m; if(m!==lastLoopErr){ lastLoopErr=m; log('warn','Dongu hatasi', m); } }
  broadcast();
}
start();
