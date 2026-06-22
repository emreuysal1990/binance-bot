'use strict';
/*
 * SURVIVE & GROW — Binance hesap yoneticisi (backend)
 * YENI SURUM: HMA, VWAP (Session), STC, Squeeze Momentum, ADX ve Chandelier Exit.
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
  safe:   { entry:0.50, maxPos:3, stopMult:1.0, trailMult:0.8, actMult:0.9 },
  normal: { entry:0.40, maxPos:4, stopMult:1.2, trailMult:1.0, actMult:0.8 },
  degen:  { entry:0.30, maxPos:5, stopMult:1.5, trailMult:1.2, actMult:0.7 },
};
const clamp = (x,a,b)=>Math.max(a,Math.min(b,x));
const isStable = (s,p,c)=>{ s=(s||'').toLowerCase();
  if (STABLE.has(s)) return true; if (s.indexOf('usd')>=0) return true;
  if (['eur','eurc','eurt','gbp','jpy','try','xaut','paxg'].indexOf(s)>=0) return true;
  if (isFinite(p)&&isFinite(c)&&Math.abs(p-1)<=0.015&&Math.abs(c)<0.5) return true; return false; };

let S = {
  mode: CFG.mode, running: true, cash: CFG.startCash, positions: {}, trades: 0,
  peak: CFG.startCash, equityHist: [CFG.startCash], dayStartEquity: CFG.startCash, dayKey: dayKey(),
  killed: false, log: [], startedAt: Date.now(), lastError: '', lastKlineFetch: 0
};
function dayKey(){ return new Date().toISOString().slice(0,10); }
process.on('unhandledRejection', (e)=>{ try{ S.lastError='unhandled: '+String(e&&e.message||e); }catch(_){} });
process.on('uncaughtException', (e)=>{ try{ S.lastError='uncaught: '+String(e&&e.message||e); }catch(_){} });
function loadState(){ try{ const o=JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); if(o&&typeof o.cash==='number') S=Object.assign(S,o); }catch(e){} }
function saveState(){ try{ fs.writeFileSync(STATE_FILE, JSON.stringify(S)); }catch(e){} }
function log(type, action, detail){ const e={ ts:Date.now(), type, action, detail }; S.log.push(e); if(S.log.length>200)S.log.shift(); console.log(`[${new Date().toISOString()}] ${type.toUpperCase()} ${action} — ${detail}`); }

const BN = 'https://data-api.binance.vision'; 
let tradeEx = null, universe = [], prices = {}, chg = {}, hist = {}, scoreS = {}, cooldown = {};
const COOLDOWN_MS = 20000;
let lastLoopErr = '';

async function fetchJSON(url){ const r=await fetch(url,{headers:{'accept':'application/json'}}); if(!r.ok) throw new Error('HTTP '+r.status); return r.json(); }

async function buildUniverse(){
  const d = await fetchJSON(BN+'/api/v3/ticker/24hr');     
  const rows=[];
  for(const t of d){ const s=t.symbol; if(!s||!s.endsWith('USDT')||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
    const base=s.slice(0,-4), price=parseFloat(t.lastPrice), vol=parseFloat(t.quoteVolume), c=parseFloat(t.priceChangePercent);
    if(!isFinite(price)||price<=0||isStable(base,price,c)) continue;
    rows.push({base, sym:s, price, vol, c}); }
  rows.sort((a,b)=>b.vol-a.vol);
  const top=rows.slice(0, CFG.univMax), u2=[];
  for(const r of top){ u2.push({base:r.base, sym:r.sym, pair:r.base+'/'+CFG.quote}); prices[r.base]=r.price; chg[r.base]=r.c; }
  universe=u2;
  log('info','Evren hazir', universe.length+' coin secildi.');
  await seedHistory(); 
}

async function seedHistory(){
  for(const u of universe){
    try{ 
      // VWAP ve HMA(55) icin geriye donuk mum sayisini 300'e cikardik (1 Gunu kapsar)
      const k=await fetchJSON(BN+'/api/v3/klines?symbol='+u.sym+'&interval=5m&limit=300'); 
      if(Array.isArray(k)&&k.length) {
        hist[u.base] = k.map(x=>({ h: parseFloat(x[2]), l: parseFloat(x[3]), c: parseFloat(x[4]), v: parseFloat(x[5]) })); 
      }
    } catch(e){ 
      if(!hist[u.base]) hist[u.base] = Array(100).fill({h: prices[u.base]||1, l: prices[u.base]||1, c: prices[u.base]||1, v: 0}); 
    }
  }
  S.lastKlineFetch = Date.now();
}

async function refreshPrices(){
  if(!universe.length) return;
  const syms = encodeURIComponent(JSON.stringify(universe.map(u=>u.sym)));
  const d = await fetchJSON(BN+'/api/v3/ticker/24hr?symbols='+syms);
  const map={}; universe.forEach(u=>map[u.sym]=u.base);
  for(const t of d){ 
      const b=map[t.symbol]; if(!b) continue; const p=parseFloat(t.lastPrice);
      if(isFinite(p)&&p>0){ 
          prices[b]=p; chg[b]=parseFloat(t.priceChangePercent)||chg[b]||0;
          if(hist[b] && hist[b].length > 0) { 
              const last = hist[b][hist[b].length-1];
              last.c = p; if (p > last.h) last.h = p; if (p < last.l) last.l = p;
          } 
      } 
  }
}

// =================== MATEMATIK & INDIKATORLER ===================
const sma=(a,p)=>{ if(a.length<p) return null; let s=0; for(let i=a.length-p;i<a.length;i++)s+=a[i]; return s/p; };
function emaArr(a,p){ if(a.length<p) return null; const k=2/(p+1),o=Array(a.length).fill(null); let e=0; for(let i=0;i<p;i++)e+=a[i]; e/=p; o[p-1]=e; for(let i=p;i<a.length;i++){e=a[i]*k+e*(1-k);o[i]=e;} return o; }
function wmaArr(a, p) {
    if(a.length<p) return null; let res = Array(a.length).fill(null); let norm = 0.5 * p * (p + 1);
    for(let i=p-1; i<a.length; i++) { let sum = 0; for(let j=0; j<p; j++) sum += a[i - p + 1 + j] * (j + 1); res[i] = sum / norm; } return res;
}

// 1. Hull Moving Average (HMA) - Sifir Gecikmeli Ortalama
function calcHMA(a, p=55) {
    let halfP = Math.floor(p/2), sqrtP = Math.floor(Math.sqrt(p));
    let wmaHalf = wmaArr(a, halfP), wmaFull = wmaArr(a, p);
    if(!wmaHalf || !wmaFull) return null;
    let diff = Array(a.length).fill(null);
    for(let i=0; i<a.length; i++) if(wmaHalf[i]!=null && wmaFull[i]!=null) diff[i] = 2*wmaHalf[i] - wmaFull[i];
    let validDiffs = diff.filter(x => x!=null);
    let hmaRaw = wmaArr(validDiffs, sqrtP);
    if(!hmaRaw) return null;
    let pad = Array(a.length - hmaRaw.length).fill(null);
    return pad.concat(hmaRaw);
}

// 2. Anchored VWAP (Gunluk/Session) - Kurumsal Maliyetlenme
function calcVWAP(hData, p=288) { // 5m grafikte 24 saat = 288 mum
    if(hData.length < 10) return null;
    let limit = Math.min(hData.length, p);
    let sumVol = 0, sumPV = 0;
    for(let i = hData.length - limit; i < hData.length; i++) {
        let typ = (hData[i].h + hData[i].l + hData[i].c) / 3;
        sumPV += typ * hData[i].v; sumVol += hData[i].v;
    }
    return sumVol === 0 ? hData[hData.length-1].c : (sumPV / sumVol);
}

// 3. Schaff Trend Cycle (STC) - Erkenci ve Dalga Yakalayici
function calcSTC(cArr) {
    let mLine = [], f = emaArr(cArr, 23), s = emaArr(cArr, 50);
    if(!f || !s) return null;
    for(let i=0; i<cArr.length; i++) mLine.push(f[i]!=null && s[i]!=null ? f[i]-s[i] : null);
    let stcArr = [], f1Arr = [], pfArr = [], f2Arr = [], len = 10, pf = 0, stc = 0;
    for(let i=len-1; i<mLine.length; i++) {
        let slice = mLine.slice(i-len+1, i+1).filter(x=>x!=null); if(slice.length < len) continue;
        let lowest = Math.min(...slice), highest = Math.max(...slice), denom = highest - lowest;
        f1Arr[i] = denom > 0 ? 100 * ((mLine[i] - lowest) / denom) : (f1Arr[i-1]||0);
    }
    for(let i=0; i<mLine.length; i++) {
        if(f1Arr[i] != null) { pf = pf === 0 ? f1Arr[i] : pf + 0.5 * (f1Arr[i] - pf); pfArr[i] = pf; }
    }
    for(let i=len-1; i<pfArr.length; i++) {
        if(pfArr[i]==null) continue;
        let slice = pfArr.slice(i-len+1, i+1).filter(x=>x!=null); if(slice.length < len) continue;
        let lowest = Math.min(...slice), highest = Math.max(...slice), denom = highest - lowest;
        f2Arr[i] = denom > 0 ? 100 * ((pfArr[i] - lowest) / denom) : (f2Arr[i-1]||0);
    }
    for(let i=0; i<mLine.length; i++) {
        if(f2Arr[i] != null) { stc = stc === 0 ? f2Arr[i] : stc + 0.5 * (f2Arr[i] - stc); stcArr.push(stc); }
    }
    if(stcArr.length < 2) return null;
    return { val: stcArr[stcArr.length-1], prev: stcArr[stcArr.length-2] };
}

// 4. Squeeze Momentum (LazyBear) - Patlama / Sikisma
function calcSqueeze(hData, p=20) {
    if(hData.length < p + 1) return null;
    let getMom = (endIdx) => {
        let slice = hData.slice(endIdx - p, endIdx);
        let cArr = slice.map(x=>x.c), hArr = slice.map(x=>x.h), lArr = slice.map(x=>x.l);
        let sma = cArr.reduce((a,b)=>a+b,0)/p;
        let std = Math.sqrt(cArr.reduce((a,b)=>a+Math.pow(b-sma,2),0)/p);
        let bbUp = sma + 2*std, bbLo = sma - 2*std;
        let trSum = 0;
        for(let i=1; i<slice.length; i++) {
            trSum += Math.max(slice[i].h - slice[i].l, Math.abs(slice[i].h - slice[i-1].c), Math.abs(slice[i].l - slice[i-1].c));
        }
        let kcUp = sma + 1.5 * (trSum/p), kcLo = sma - 1.5 * (trSum/p);
        let isSqueeze = (bbUp < kcUp && bbLo > kcLo);
        let donchianMid = (Math.max(...hArr) + Math.min(...lArr)) / 2;
        let momentum = cArr[cArr.length-1] - ((donchianMid + sma) / 2);
        return { isSqueeze, momentum };
    };
    return { current: getMom(hData.length), prev: getMom(hData.length-1) };
}

// 5. ADX (Average Directional Index) - Trend Gucu Filtresi
function calcADX(hData, p=14) {
    if(hData.length < p * 2) return null;
    let tr=[], pdm=[], ndm=[];
    for(let i=1; i<hData.length; i++){
        tr.push(Math.max(hData[i].h-hData[i].l, Math.abs(hData[i].h-hData[i-1].c), Math.abs(hData[i].l-hData[i-1].c)));
        let up = hData[i].h - hData[i-1].h, dn = hData[i-1].l - hData[i].l;
        pdm.push((up > dn && up > 0) ? up : 0); ndm.push((dn > up && dn > 0) ? dn : 0);
    }
    let smooth = (arr) => {
        let res=[arr.slice(0,p).reduce((a,b)=>a+b,0)];
        for(let i=p; i<arr.length; i++) res.push(res[res.length-1] - (res[res.length-1]/p) + arr[i]);
        return res;
    };
    let trS = smooth(tr), pdmS = smooth(pdm), ndmS = smooth(ndm), dx = [];
    for(let i=0; i<trS.length; i++){
        let pdi = 100 * (pdmS[i]/trS[i]), ndi = 100 * (ndmS[i]/trS[i]);
        let sum = pdi+ndi; dx.push(sum === 0 ? 0 : 100 * Math.abs(pdi-ndi)/sum);
    }
    let adx = dx.slice(0,p).reduce((a,b)=>a+b,0)/p;
    for(let i=p; i<dx.length; i++) adx = (adx * (p-1) + dx[i]) / p;
    return adx;
}

// Temel Risk Yonetimi: ATR ve Chandelier Exit
function calcATR(hData, len = 14) {
    if (hData.length < len + 1) return null; let tr = [];
    for (let i = 1; i < hData.length; i++) tr.push(Math.max(hData[i].h - hData[i].l, Math.abs(hData[i].h - hData[i-1].c), Math.abs(hData[i].l - hData[i-1].c)));
    let atr = tr.slice(0, len).reduce((a,b)=>a+b,0) / len;
    for(let i = len; i < tr.length; i++) atr = (atr * (len - 1) + tr[i]) / len; return atr;
}
function chandelierExit(hData, atr, period = 22, multiplier = 2.5) {
    if (hData.length < period || !atr) return null;
    const highestHigh = Math.max(...hData.slice(-period).map(x => x.h)); return highestHigh - (atr * multiplier); 
}

// =================== BEYIN (ANALIZ VE SKORLAMA) ===================
function analyze(b){ 
  const h=hist[b]; if(!h||h.length<100) return null;
  const cArr = h.map(x => x.c), price=cArr[cArr.length-1];
  
  const vwap = calcVWAP(h, 288);
  const hma = calcHMA(cArr, 55);
  const stc = calcSTC(cArr);
  const sq = calcSqueeze(h, 20);
  const adx = calcADX(h, 14);
  const atr = calcATR(h, 14);
  const vp = atr ? (atr / price) : 0.02;

  let score=0; const why=[];
  
  // 1. VWAP Kurumsal Maliyet Onayi
  if(vwap && price > vwap) { score += 0.20; why.push('VWAP Ustu'); } else { score -= 0.15; }
  
  // 2. HMA (Sifir Gecikme) Trend Yonu
  if(hma && hma[hma.length-1] != null) {
      let curHma = hma[hma.length-1], prevHma = hma[hma.length-2];
      if(curHma > prevHma) { score += 0.20; why.push('HMA Yukari'); } else { score -= 0.20; }
  }

  // 3. STC Erken Sinyal
  if(stc) {
      if(stc.val > stc.prev && stc.val < 75) { score += 0.25; why.push(`STC Alis(${stc.val.toFixed(0)})`); }
      else if(stc.val > 90) { score -= 0.25; why.push('STC Doygun'); }
  }

  // 4. Squeeze Momentum Patlamasi
  if(sq) {
      if(sq.current.isSqueeze) { score -= 0.10; /* Sikismada islem yapma, bekle */ }
      else if(sq.current.momentum > 0 && sq.current.momentum > sq.prev.momentum) {
          score += 0.25; why.push('Squeeze Patlama');
      }
  }

  // 5. ADX Guc Filtresi (Trend gercek mi?)
  if(adx && adx > 25) { score += 0.15; why.push(`ADX Guclu(${adx.toFixed(0)})`); }
  else if(adx && adx < 20) { score -= 0.10; /* Gucsuz/yatay piyasa */ }

  const trendUp = (score >= 0.40); // Siki filtre: Sadece mukemmel uyumda girer
  
  // Cikis icin yardimci data aktarimi
  const exitData = {
      hmaTurnedDown: (hma && hma[hma.length-1] < hma[hma.length-2]),
      stcDead: (stc && stc.val < stc.prev && stc.val > 80)
  };

  return { score, why, price, trendUp, vp, exitData };
}

// =================== EMIRLER VE CIKIS STRATEJISI ===================
function equity(){ let v=S.cash; for(const b in S.positions){ const p=S.positions[b]; v+=p.qty*(prices[b]||p.entry); } return v; }
async function placeBuy(base, costUsdt, a, M){
  costUsdt = Math.min(costUsdt, CFG.maxTradeUsdt); const pair = base+'/'+CFG.quote, px = prices[base];
  if (S.mode === 'paper'){ const fee=costUsdt*FEE, qty=(costUsdt-fee)/px; S.cash-=costUsdt; openLedger(base,qty,px,costUsdt,a,M); }
  else {
    const o = await tradeEx.createMarketBuyOrderWithCost(pair, costUsdt);
    const filledQty = o.filled || (o.amount||0), spent = o.cost || costUsdt, avg = o.average || px;
    S.cash -= spent; openLedger(base, filledQty, avg, spent, a, M);
  }
}
function openLedger(base, qty, px, cost, a, M){
  S.positions[base]={ qty, entry:px, cost, high:px, openTs:Date.now(), dynStop: 0 }; S.trades++;
  log('buy','AL '+base, `${cost.toFixed(2)} USDT @ ${px} — Skor ${(scoreS[base]||0).toFixed(2)} · ${(a?a.why.join(', '):'sinyal')}`);
}
async function placeSell(base, why){
  const p = S.positions[base]; if(!p) return; const pair=base+'/'+CFG.quote, px=prices[base]||p.entry;
  let gross, fee, net;
  if (S.mode === 'paper'){ gross=p.qty*px; fee=gross*FEE; net=gross-fee; S.cash+=net; }
  else { const o=await tradeEx.createMarketSellOrder(pair, p.qty); gross=o.cost||(p.qty*px); fee=(o.fee&&o.fee.cost)||gross*FEE; net=gross-fee; S.cash+=net; }
  const pnl=net-p.cost, pct=(pnl/p.cost)*100; delete S.positions[base]; cooldown[base]=Date.now()+COOLDOWN_MS;
  log(pnl>=0?'sell-win':'sell-loss','SAT '+base, `@ ${px} — ${why}. ${pnl>=0?'+':''}${pnl.toFixed(2)} USDT (${pct>=0?'+':''}${pct.toFixed(2)}%)`);
}

function strategy(){
  const eq=equity(); const M=MODES[CFG.riskMode] || MODES.normal;
  let entry=M.entry, maxPos=Math.min(M.maxPos, CFG.maxPositions);
  const ana={};
  for(const u of universe){ const a=analyze(u.base); ana[u.base]=a; if(a){ scoreS[u.base]= scoreS[u.base]==null?a.score:scoreS[u.base]*0.6+a.score*0.4; } }
  
  // DINAMIK CIKISLAR (Chandelier, HMA, STC)
  for(const b of Object.keys(S.positions)){
    const p=S.positions[b], px=prices[b]; if(!px) continue; if(px>p.high)p.high=px;
    
    const hData = hist[b]; const currentAtr = hData ? calcATR(hData, 14) : null;
    if(currentAtr) {
        const dStop = chandelierExit(hData, currentAtr, 22, 2.5);
        if (dStop) p.dynStop = Math.max(p.dynStop || 0, dStop); // Trailing: Sadece yukari cikar
    }

    const netPct=((px*p.qty - p.cost - 2*(px*p.qty*FEE))/p.cost)*100; 
    let why=null;
    const aData = ana[b];
    
    // 1. Dinamik Stop (Chandelier) Kirildi
    if (p.dynStop && px <= p.dynStop) why = 'Chandelier Stop';
    
    // 2. Erken Kar Al / Kacin (HMA donduyse ve STC tepe yapip curuduyse)
    else if (aData && aData.exitData) {
        if (netPct > 2.0 && aData.exitData.hmaTurnedDown && aData.exitData.stcDead) why = 'HMA/STC Zirve Donusu';
        else if (netPct < -1.0 && aData.exitData.hmaTurnedDown && scoreS[b] < -0.10) why = 'Trend Cokusu';
    }

    if(why) queue(()=>placeSell(b, `${why} (%${netPct.toFixed(2)})`));
  }
  
  // GIRISLER
  let open=Object.keys(S.positions).length; const minOrder = (S.mode==='paper')?DUST:CFG.minNotional; let avail = S.cash;   
  const cand=universe.map(u=>u.base).filter(b=>!S.positions[b]&&prices[b]&&!isStable(b,prices[b],chg[b])&&(!cooldown[b]||Date.now()>cooldown[b])&&ana[b]&&ana[b].trendUp&&scoreS[b]!=null&&scoreS[b]>=entry).sort((x,y)=>scoreS[y]-scoreS[x]);
  for(const b of cand){ if(open>=maxPos) break;
    const conv=clamp(0.6+(scoreS[b]-entry)*1.5,0.6,1.0);            
    let size=Math.min(equity()/maxPos*conv, avail*0.99);
    if(size<minOrder) continue;
    queue(()=>placeBuy(b, size, ana[b], M)); open++; avail-=size; 
  }
}

let q=Promise.resolve();
function queue(fn){ q=q.then(fn).catch(e=>{ S.lastError=String(e&&e.message||e); log('warn','Emir hatasi', S.lastError); }); }

async function loop(){
  try{
    if(!universe.length){ await buildUniverse(); }
    if(dayKey()!==S.dayKey){ S.dayKey=dayKey(); S.dayStartEquity=equity(); S.killed=false; log('info','Gunluk Senkron', 'Sayaclar sifirlandi.'); }
    
    if (Date.now() - S.lastKlineFetch > 5 * 60 * 1000) await seedHistory(); // 5 dakikada bir ana guncelleme
    await refreshPrices();
    
    const eq=equity();
    if(!S.killed && eq <= S.dayStartEquity*(1-CFG.dailyLossStop)){
      S.killed=true; S.running=false; log('warn','KILL-SWITCH', `Gun-ici kayip %${(CFG.dailyLossStop*100).toFixed(0)} asildi. Kapatiliyor.`);
      for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'kill-switch'));
    }
    if(S.running && !S.killed) strategy();
    S.equityHist.push(eq); if(S.equityHist.length>500)S.equityHist.shift(); if(eq>S.peak)S.peak=eq;
    saveState(); broadcast();
  }catch(e){ const m=String(e&&e.message||e); S.lastError=m; if(m!==lastLoopErr){ lastLoopErr=m; log('warn','Dongu hatasi', m); } }
}

const app=express(); app.use(express.json()); app.get('/',(req,res)=>res.sendFile(path.join(__dirname,'index.html')));
function auth(req,res,next){ const t=req.headers['x-token']||req.query.token; if(t!==CFG.token) return res.status(401).json({error:'token'}); next(); }
function snapshot(){ const eq=equity(); return {
  mode:S.mode, running:S.running, killed:S.killed, equity:eq, cash:S.cash, peak:S.peak, start:CFG.startCash,
  pnlPct:(eq-CFG.startCash)/CFG.startCash*100, trades:S.trades, quote:CFG.quote, univ:universe.length,
  positions:Object.entries(S.positions).map(([b,p])=>({ base:b, entry:p.entry, price:prices[b]||p.entry, qty:p.qty, cost:p.cost,
    upnl:(prices[b]||p.entry)*p.qty-p.cost, stopLvl:p.dynStop||0 })),
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
  queue(()=>placeBuy(b,size,null,M)); log('info','Manuel','Elle ALIM: '+b); res.json({ok:true}); });
app.post('/api/sell', auth, (req,res)=>{ const b=(req.body.symbol||'').toUpperCase(); if(!S.positions[b]) return res.status(400).json({error:'pozisyon yok'});
  queue(()=>placeSell(b,'manuel satis')); log('info','Manuel','Elle SATIS: '+b); res.json({ok:true}); });
app.post('/api/close-all', auth, (req,res)=>{ for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'manuel: hepsini kapat')); log('info','Manuel','Tum pozisyonlar kapatiliyor.'); res.json({ok:true}); });
app.post('/api/panic', auth, (req,res)=>{ S.running=false; for(const b of Object.keys(S.positions)) queue(()=>placeSell(b,'PANIK')); log('warn','Manuel','PANIK: hepsi kapatildi + bot durduruldu.'); res.json({ok:true}); broadcast(); });
app.post('/api/reset', auth, (req,res)=>{ S.cash=CFG.startCash; S.positions={}; S.trades=0; S.equityHist=[CFG.startCash]; S.peak=CFG.startCash; S.killed=false; S.running=true; S.dayStartEquity=CFG.startCash; S.dayKey=dayKey(); S.log=[]; cooldown={}; scoreS={}; try{fs.unlinkSync(STATE_FILE);}catch(e){} log('info','Manuel','Sifirlandi.'); res.json({ok:true}); broadcast(); });

const server=http.createServer(app); const wss=new WebSocketServer({ server, path:'/ws' });
wss.on('connection',(socket,req)=>{ const u=new URL(req.url,'http://x'); if(u.searchParams.get('token')!==CFG.token){ socket.close(); return; } socket.send(JSON.stringify(snapshot())); });
function broadcast(){ const msg=JSON.stringify(snapshot()); wss.clients.forEach(c=>{ if(c.readyState===1) c.send(msg); }); }

function start(){
  log('info','Akilli Bot Baslatiliyor', `HMA/VWAP/STC/Squeeze aktif.`);
  loadState(); server.listen(CFG.port); initData();                                                                                            
}
async function initData(){
  try{
    if(CFG.mode!=='paper'){
      try{
        const mod = await import('ccxt'); const ccxt = mod.default || mod;
        tradeEx = new ccxt.binance({ apiKey:CFG.key, secret:CFG.secret, enableRateLimit:true, options:{ defaultType:'spot', createMarketBuyOrderRequiresPrice:false } });
        if(CFG.mode==='testnet') tradeEx.setSandboxMode(true); await tradeEx.loadMarkets();
        try{ const bal=await tradeEx.fetchBalance(); const free=bal.free&&bal.free[CFG.quote]; if(typeof free==='number'){ S.cash=free; } }catch(e){}
      }catch(e){ S.lastError='ccxt: '+String(e&&e.message||e); }
    }
    await buildUniverse();
  }catch(e){ S.lastError=String(e&&e.message||e); }
  setInterval(loop, CFG.pollMs); loop();
}
start();
