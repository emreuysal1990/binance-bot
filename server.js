'use strict';
/* Survive & Grow — Binance Hesap Yoneticisi
 * Veri: Binance (data-api.binance.vision) · Strateji: HMA + VWAP + ADX (ana mum, vars. 15m) + 1h/4h yon onayi
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
  entryScore: +(process.env.ENTRY_SCORE || 0.20),     // trend skor esigi (maks 0.60) — dusuk = daha cok firsat
  htf: (process.env.HTF || '1') !== '0',              // 1h + 4h yon onayi acik/kapali
  interval: (process.env.INTERVAL || '15m'),          // ana sinyal mum araligi (15m onerilir; 5m daha gurultulu)
  pumpMax: +(process.env.PUMP_MAX || 40),             // 24s %X uzeri pompalandiysa alma
  tp1Pct: +(process.env.TP1_PCT || 1.5),              // kismi kar seviyesi (net %) — ATR kapaliyken
  tp1Frac: +(process.env.TP1_FRAC || 0.5),            // kismi karda satilacak oran
  dailyLossStop: +(process.env.DAILY_LOSS_STOP || 0.15),
  minNotional: +(process.env.MIN_NOTIONAL || 10),
  maxTrade: +(process.env.MAX_TRADE_USDT || 0),       // 0 = sinirsiz
  cooldownMin: +(process.env.COOLDOWN_MIN || 35),     // satistan sonra ayni coine girmeden once bekleme (churn'u keser)
  warmupSec: +(process.env.WARMUP_SEC || 60),          // acilista veri otursun diye alim yapmadan beklenecek sure
  maxNewPerTick: +(process.env.MAX_NEW_PER_TICK || 2), // her turda en fazla kac yeni pozisyon (acilista toplu alimi onler)
  atrExits: (process.env.ATR_EXITS || '1') !== '0',   // ATR'ye gore uyarlanan stop/hedef/trailing (volatiliteye gore)
  baseFrac: +(process.env.BASE_POS_PCT || 12)/100,    // ortalama pozisyon = KASANIN %'si (para buyur/kuculur, gercek bakiye farkli olursa otomatik olcek)
  maxFrac: +(process.env.MAX_POS_PCT || 30)/100,      // tek pozisyon ust siniri = kasanin %'si
  riskPct: +(process.env.RISK_PCT || 0.015),          // (ATR boyutlandirma referansi)
  atrStopK: +(process.env.ATR_STOP_K || 1.3),         // sert stop mesafesi = K x ATR (daralttik: kucuk kayip)
  atrTpK: +(process.env.ATR_TP_K || 2.2),             // kismi kar mesafesi = K x ATR (gec al: kazanci buyut)
  maxAtrPct: +(process.env.MAX_ATR_PCT || 6),         // ATR%'si bunu asan asiri vahsi coinleri alma
  flashDropK: +(process.env.FLASH_DROP_K || 1.2),     // ani dusus esigi = K x ATR (acil cikis)
  flashSpikeK: +(process.env.FLASH_SPIKE_K || 1.6),   // ani yukselis esigi = K x ATR (kar kilitle)
  spikeEntryK: +(process.env.SPIKE_ENTRY_K || 2.0),   // dikey spike tepesinden alma
  key: process.env.BINANCE_KEY || '',
  secret: process.env.BINANCE_SECRET || '',
};
const BN = 'https://data-api.binance.vision';
const INT_MIN = ({'1m':1,'3m':3,'5m':5,'15m':15,'30m':30,'1h':60,'2h':120,'4h':240}[CFG.interval]||15);
const VWAP_LEN = Math.max(20, Math.round(1440/INT_MIN));   // ~24 saatlik VWAP penceresi (mum sayisi; 15m->96, 5m->288)
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
    try{ const k=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval='+CFG.interval+'&limit=300');
      if(Array.isArray(k)&&k.length) hist[u.base]=k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]})); }catch(e){}
    if(CFG.htf){
      try{ const k1=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=1h&limit=120'); if(Array.isArray(k1)&&k1.length) hist1h[u.base]=k1.map(x=>+x[4]); }catch(e){}
      try{ const k4=await getJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=4h&limit=120'); if(Array.isArray(k4)&&k4.length) hist4h[u.base]=k4.map(x=>+x[4]); }catch(e){}
    }
  }
  lastReseed=Date.now();
  console.log('[gecmis] '+CFG.interval+' + 1h + 4h mumlar yuklendi');
  if(!dataReady){ dataReady=true; warmupUntil=Date.now()+CFG.warmupSec*1000;
    log('info','Hazir', `Veri oturdu — ${CFG.warmupSec}sn isinma sonrasi alim baslar.`); }
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
function legUp(closes){ if(!closes||closes.length<55) return null; const e=ema(closes,50); return e? closes[closes.length-1]>e : null; }   // null = veri yok
function htfUpOf(b){ if(!CFG.htf) return true; const a=legUp(hist1h[b]), c=legUp(hist4h[b]);
  if(a===null||c===null) return false;   // 1h+4h verisi tam degilse yon onayi YOK (alma)
  return a && c; }                        // ikisi de yukari olmali

function calcRSI(c,p=14){ if(c.length<p+1)return 50; let g=0,l=0; for(let i=c.length-p;i<c.length;i++){ const d=c[i]-c[i-1]; if(d>=0)g+=d; else l-=d; } if(l===0)return 100; const rs=(g/p)/(l/p); return 100-100/(1+rs); }
function calcBB(c,p=20,k=2){ if(c.length<p)return null; const s=c.slice(-p), m=s.reduce((a,b)=>a+b,0)/p; const sd=Math.sqrt(s.reduce((a,b)=>a+(b-m)*(b-m),0)/p); const upper=m+k*sd, lower=m-k*sd, px=c[c.length-1]; return {mid:m,upper,lower,pctB:(upper-lower)>0?(px-lower)/(upper-lower):0.5}; }
function calcCHOP(h,p=14){ if(h.length<p+1)return 50; const seg=h.slice(-p-1); let tr=0; for(let i=1;i<seg.length;i++) tr+=Math.max(seg[i].h-seg[i].l,Math.abs(seg[i].h-seg[i-1].c),Math.abs(seg[i].l-seg[i-1].c)); const hh=Math.max(...seg.slice(1).map(x=>x.h)), ll=Math.min(...seg.slice(1).map(x=>x.l)), rng=hh-ll; if(rng<=0||tr<=0)return 50; return clampN(100*Math.log10(tr/rng)/Math.log10(p),0,100); }
function clampN(x,a,b){ return Math.max(a,Math.min(b,x)); }
function calcATR(h,p=14){ if(!h||h.length<p+1)return null; let s=0; for(let i=h.length-p;i<h.length;i++) s+=Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c)); return s/p; }

function analyze(b){
  const h=hist[b]; if(!h||h.length<100) return {score:0,trendUp:false,meanRevBuy:false,regime:'neutral',adx:0,chop:50,rsi:50,pctB:0.5,atrPct:null,aboveVwap:false,hmaUp:false,htfUp:htfUpOf(b),overbought:(chg[b]||0)>=CFG.pumpMax,px:prices[b]||0};
  const cArr=h.map(x=>x.c), px=cArr[cArr.length-1];
  const vwap=calcVWAP(h,VWAP_LEN), hma=calcHMA(cArr), adx=calcADX(h), chop=calcCHOP(h), rsi=calcRSI(cArr), bb=calcBB(cArr);
  const pctB=bb?bb.pctB:0.5;
  const atr=calcATR(h,14), atrPct=(atr&&px>0)?clampN(atr/px*100,0.3,8):null;
  const aboveVwap=vwap?px>vwap:false;
  const hmaUp=!!(hma&&hma.length>=2&&hma[hma.length-1]!=null&&hma[hma.length-2]!=null&&hma[hma.length-1]>hma[hma.length-2]);
  const htfUp=htfUpOf(b), overbought=(chg[b]||0)>=CFG.pumpMax;
  let regime='neutral'; if(adx>=23&&chop<=55) regime='trend'; else if(chop>=60||adx<15) regime='range';
  let score=0;
  if(aboveVwap) score+=0.20; else score-=0.20;
  if(hmaUp)     score+=0.20; else score-=0.25;
  if(adx>25)    score+=0.20; else if(adx<20) score-=0.30;
  const trendUp   = (regime==='trend') && (score>=CFG.entryScore) && htfUp && !overbought;     // TREND: momentum takibi
  const meanRevBuy= (regime==='range') && rsi<40 && pctB<0.20 && htfUp && !overbought;          // YATAY: dipten al, ortalamaya sat
  return {score,trendUp,meanRevBuy,regime,adx,chop,rsi,pctB,atrPct,aboveVwap,hmaUp,htfUp,overbought,px};
}
function refreshAnalysis(){ for(const u of universe){ ana[u.base]=analyze(u.base); } }

// ---------- EMIRLER (komisyon + slipaj + live yuvarlama) ----------
function mkSym(base){ return base+'/'+CFG.quote; }
function roundAmt(base, amt){ try{ return parseFloat(tradeEx.amountToPrecision(mkSym(base), amt)); }catch(e){ return amt; } }
function minCostOf(base){ try{ const m=tradeEx.market(mkSym(base)); return (m&&m.limits&&m.limits.cost&&m.limits.cost.min)||CFG.minNotional; }catch(e){ return CFG.minNotional; } }

async function placeBuy(base, cost, mode){
  mode = mode || 'trend';
  const px=prices[base]; if(!px||px<=0) return;
  const _ar=calcATR(hist[base]||[],14); const atrPct=(_ar&&px>0)?clampN(_ar/px*100,0.3,8):1.5;
  if(CFG.maxTrade>0) cost=Math.min(cost,CFG.maxTrade);
  if(cost>S.cash) cost=S.cash;
  if(cost<1) return;
  if(S.mode==='paper'){
    const fillPx=px*(1+CFG.slip), fee=cost*CFG.fee, qty=(cost-fee)/fillPx; S.cash-=cost;
    S.positions[base]={qty,cost,entry:fillPx,high:fillPx,openTs:Date.now(),tp1done:false,mode,atrPct}; S.trades++;
    log('buy','AL '+base+' ['+mode+']', `${cost.toFixed(2)} ${CFG.quote} @ ${fillPx.toFixed(8)} (slipaj+komisyon dahil)`);
  } else if(tradeEx){
    try{
      let cst=Math.floor(cost*100)/100;                              // tutari yuvarla (gecerli emir icin)
      try{ if(tradeEx.costToPrecision) cst=parseFloat(tradeEx.costToPrecision(mkSym(base),cost)); }catch(e){}
      const minC=minCostOf(base); if(cst<minC){ log('warn','Atlandi', base+' min emir '+minC+' '+CFG.quote); return; }
      const o=await tradeEx.createMarketBuyOrderWithCost(mkSym(base), cst);
      const qty=o.filled||o.amount||0, spent=o.cost||cst, avg=o.average||px;
      S.cash-=spent; S.positions[base]={qty,cost:spent,entry:avg,high:avg,openTs:Date.now(),tp1done:false,mode,atrPct}; S.trades++;
      log('buy','AL '+base+' ['+mode+']', `Gercek emir ${spent.toFixed(2)} ${CFG.quote} @ ${avg}`);
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
      let qty=roundAmt(base, sellQty); if(!(qty>0)){ return; }       // miktari lot adimina yuvarla
      const minC=minCostOf(base);
      if(qty*px < minC){ if(frac>=0.999) qty=roundAmt(base, p.qty); if(qty*px < minC){ log('warn','Atlandi','Satis min-notional alti: '+base); return; } }
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
  const ap=p.atrPct||1.5;
  const sd  = CFG.atrExits? clampN(CFG.atrStopK*ap,1.2,4.5) : 3;
  const actT= CFG.atrExits? Math.max(1.5,1.8*ap) : 2.5;
  const give= CFG.atrExits? Math.max(0.8,1.2*ap) : 0.8;
  const beT = CFG.atrExits? Math.max(1.2,1.4*ap) : 1.2;
  const peakPct=((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
  if(peakPct>=actT) return p.entry*(1+(peakPct-give)/100);
  if(peakPct>=beT)  return p.entry*(1+0.1/100);
  return p.entry*(1-sd/100);
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
  // CIKISLAR (moda gore, ATR'ye uyarlanan seviyeler)
  for(const b of Object.keys(S.positions)){
    const p=S.positions[b], px=prices[b]; if(!px) continue; if(px>p.high)p.high=px;
    const a=ana[b]||{}; const ap=p.atrPct||1.5;
    let sd,tp,actT,give,beT;
    if(CFG.atrExits){ sd=clampN(CFG.atrStopK*ap,1.2,4.5); tp=Math.max(1.2,CFG.atrTpK*ap); actT=Math.max(1.5,1.8*ap); give=Math.max(0.8,1.2*ap); beT=Math.max(1.2,1.4*ap); }
    else { sd=3; tp=CFG.tp1Pct; actT=2.5; give=0.8; beT=1.2; }
    const netPct=((px*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    const peakPct=((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
    // ANI HAREKET (flash) korumasi — son tik'e gore ani dusus/yukselis
    const ref=prevPx[b]; const fastRet=(ref&&ref>0)?(px/ref-1)*100:0;
    const fDrop=Math.max(1.5, CFG.flashDropK*ap), fSpike=Math.max(2.0, CFG.flashSpikeK*ap);
    if(fastRet<=-fDrop){ queue(()=>placeSell(b, `ani düşüş koruması (%${fastRet.toFixed(2)})`, 1)); continue; }
    if(fastRet>=fSpike && netPct>0 && !p.tp1done){ p.tp1done=true; queue(()=>placeSell(b, `ani yükseliş — kâr kilitle (%${netPct.toFixed(2)})`, CFG.tp1Frac)); continue; }
    if(!p.tp1done && netPct>=tp){ p.tp1done=true; queue(()=>placeSell(b, `kismi kar +%${netPct.toFixed(2)}`, CFG.tp1Frac)); continue; }
    let why=null;
    if(p.mode==='range'){                                   // mean-reversion: ortalamaya donunce sat
      if(a.pctB!=null && a.pctB>=0.5)      why='ortalamaya donus';
      else if(a.rsi!=null && a.rsi>=65)    why='rsi normallesti';
      else if(netPct<=-sd)                 why='zarar-kes';
    } else {                                                // trend: ATR trailing / basabas / sert stop
      if(peakPct>=actT && netPct<=peakPct-give) why='zirveden donus (kar kilitlendi)';
      else if(peakPct>=beT && netPct<=0.1)      why='basabas korumasi';
      else if(netPct<=-sd)                       why='zarar-kes';
    }
    if(why) queue(()=>placeSell(b, `${why} (%${netPct.toFixed(2)})`, 1));
  }
  // GIRISLER (trend + yatay birlikte; en guclu sinyaller once)
  if(!S.running||S.killed) return;
  if(!dataReady || Date.now()<warmupUntil) return;                                   // veri otursun + isinma bitsin
  let open=Object.keys(S.positions).length, avail=S.cash;
  const cands=[];
  for(const u of universe){ const b=u.base, a=ana[b]; if(!a) continue;
    if(S.positions[b]||!(prices[b]>0)||(cooldown[b]&&Date.now()<=cooldown[b])) continue;
    if(!hist[b]||hist[b].length<100) continue;                                       // ana mum verisi tam degil -> alma
    if(CFG.htf && (!hist1h[b]||hist1h[b].length<55||!hist4h[b]||hist4h[b].length<55)) continue;  // 1h/4h yon verisi tam degil -> alma
    if((a.atrPct||0) > CFG.maxAtrPct) continue;                                       // asiri vahsi coin (yuksek ATR%) -> alma
    const ap=a.atrPct||1.5, ref=prevPx[b], fr=(ref&&ref>0)?(prices[b]/ref-1)*100:0;
    if(fr >= Math.max(2.0, CFG.spikeEntryK*ap)) continue;     // dikey spike tepesinden alma
    if(a.trendUp)         cands.push({b,mode:'trend',rank:a.score});
    else if(a.meanRevBuy) cands.push({b,mode:'range',rank:0.5+(40-a.rsi)/70});
  }
  cands.sort((x,y)=>y.rank-x.rank);
  let placed=0;
  for(const c of cands){ if(open>=CFG.maxPositions || placed>=CFG.maxNewPerTick) break;
    const a=ana[c.b]||{}; const ap=a.atrPct||1.5;
    const volF = clampN(1.5/ap, 0.5, 1.5);                                                  // sakin coin -> buyuk, volatil -> kucuk
    const convF= (c.mode==='trend') ? clampN(0.7+(a.score-CFG.entryScore)*1.8, 0.7, 1.5)    // guclu trend skoru -> buyuk
                                    : clampN(0.7+(40-(a.rsi||35))/50, 0.7, 1.4);            // daha derin asiri-satim -> buyuk
    let alloc = equity() * CFG.baseFrac * volF * convF;                                      // BOT tutari kendi belirler (kasanin %'si)
    alloc = Math.min(alloc, equity()*CFG.maxFrac, avail*0.95);
    if(CFG.maxTrade>0) alloc=Math.min(alloc,CFG.maxTrade);
    if(alloc < CFG.minNotional){ if(avail >= CFG.minNotional) alloc=CFG.minNotional; else continue; }   // taban: min emir
    alloc = Math.round(alloc*100)/100;
    queue(()=>placeBuy(c.b, alloc, c.mode)); open++; placed++; avail-=alloc;
  }
}

// ---------- ELLE AL: COIN DETAY ----------
async function quoteCoin(base){
  base=base.toUpperCase().replace(new RegExp(CFG.quote+'$'),''); const sym=base+CFG.quote;
  const inU=universe.some(u=>u.base===base);
  if(!hist[base] || hist[base].length<100){
    const k=await getJSON(BN+'/api/v3/klines?symbol='+sym+'&interval='+CFG.interval+'&limit=300');
    if(!Array.isArray(k)||!k.length) throw new Error('coin bulunamadi: '+sym);
    hist[base]=k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]}));
  }
  if(CFG.htf && (!hist1h[base]||hist1h[base].length<55)){ try{ const k1=await getJSON(BN+'/api/v3/klines?symbol='+sym+'&interval=1h&limit=120'); if(Array.isArray(k1))hist1h[base]=k1.map(x=>+x[4]); }catch(e){} }
  if(CFG.htf && (!hist4h[base]||hist4h[base].length<55)){ try{ const k4=await getJSON(BN+'/api/v3/klines?symbol='+sym+'&interval=4h&limit=120'); if(Array.isArray(k4))hist4h[base]=k4.map(x=>+x[4]); }catch(e){} }
  if(prices[base]==null){ try{ const t=await getJSON(BN+'/api/v3/ticker/24hr?symbol='+sym); prices[base]=parseFloat(t.lastPrice); chg[base]=parseFloat(t.priceChangePercent); }catch(e){} }
  const a=analyze(base);
  const htf1=legUp(hist1h[base]), htf4=legUp(hist4h[base]);
  const sysBuy=a.trendUp||a.meanRevBuy;
  let why=[]; if(!a.htfUp)why.push('1h/4h trend onayı yok'); if(a.overbought)why.push('24s aşırı pump'); if(a.regime==='neutral')why.push('rejim belirsiz'); if(a.regime==='trend'&&a.score<CFG.entryScore)why.push('trend skoru düşük'); if(a.regime==='range'&&!(a.rsi<40&&a.pctB<0.20))why.push('yatayda dip değil');
  return { base, sym, price:prices[base]||a.px||0, chg:chg[base]||0, score:a.score, trendUp:a.trendUp, meanRevBuy:a.meanRevBuy,
    regime:a.regime, rsi:Math.round(a.rsi), pctB:+(a.pctB).toFixed(2), adx:Math.round(a.adx), chop:Math.round(a.chop), atrPct:a.atrPct!=null?+a.atrPct.toFixed(2):null,
    aboveVwap:a.aboveVwap, hmaUp:a.hmaUp, htfUp:a.htfUp, htf1:htf1===true, htf4:htf4===true, overbought:a.overbought, inUniverse:inU,
    suggest: sysBuy ? ('Sistem ALIR — '+(a.trendUp?'TREND (momentum)':'YATAY (dipten al)')) : ('Sistem normalde almaz ('+(why.join(', ')||'zayıf')+') — elle alabilirsin') };
}

// ---------- ISTATISTIK + PANEL VERI ----------
function statsOf(){ const c=S.closed; let wins=0,losses=0,sw=0,sl=0,swp=0,slp=0,tot=0;
  for(const t of c){ tot+=t.pnl; if(t.pnl>=0){wins++;sw+=t.pnl;swp+=t.pct;} else {losses++;sl+=Math.abs(t.pnl);slp+=t.pct;} }
  const n=c.length; return { closed:n, wins, losses, winRate:n?wins/n*100:0, avgWinPct:wins?swp/wins:0, avgLossPct:losses?slp/losses:0, pf: sl>0?sw/sl:(sw>0?99:0), totalPnl:tot }; }
function snapshot(){
  const eq=equity();
  const positions=Object.entries(S.positions).map(([b,p])=>{ const px=prices[b]||p.entry, val=p.qty*px;
    return { base:b, entry:p.entry, price:px, cost:p.cost, qty:p.qty, mode:p.mode||'trend', upnl:val-p.cost-(p.cost*CFG.fee*2), stopLvl:stopLevelOf(p), tp1done:!!p.tp1done }; });
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
  let usd=parseFloat(req.body.usdt); if(!isFinite(usd)||usd<=0) usd=Math.min(equity()*CFG.baseFrac, S.cash*0.95);
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
  log('info','Baslatiliyor', `mod=${CFG.mode} · kasa=$${CFG.startCash} · mum=${CFG.interval} · evren=${CFG.univMax} · maxPoz=${CFG.maxPositions} · 1hOnay=${CFG.htf?'acik':'kapali'}`);
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
    for(const u of universe){ if(prices[u.base]>0) prevPx[u.base]=prices[u.base]; }   // sonraki tik icin referans (ani hareket)
    const now=Date.now(); if(now-(S._lastEq||0)>20000){ S._lastEq=now; S.equityHist.push(equity()); if(S.equityHist.length>2000) S.equityHist.shift(); }
  }catch(e){ const m=String(e&&e.message||e); S.lastError=m; if(m!==lastLoopErr){ lastLoopErr=m; log('warn','Dongu hatasi', m); } }
  broadcast();
}
start();
