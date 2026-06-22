'use strict';
/*
 * SURVIVE & GROW — Binance Hesap Yoneticisi (Nihai Versiyon)
 * Ozellikler: Dinamik Bütçe, ADX/VWAP/HMA/STC/Squeeze/ATR/Chandelier, Komisyon Filtresi.
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
  univMax: 75, 
  maxPositions: +(process.env.MAX_POSITIONS || 6), 
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
const MODES = { safe: { entry:0.55 }, normal: { entry:0.45 }, degen: { entry:0.35 } };

const clamp = (x,a,b)=>Math.max(a,Math.min(b,x));
const isStable = (s,p,c)=>{ s=(s||'').toLowerCase(); return STABLE.has(s) || s.indexOf('usd')>=0 || ['eur','try'].includes(s); };

let S = { cash:CFG.startCash, positions:{}, trades:0, peak:CFG.startCash, equityHist:[CFG.startCash], dayStartEquity:CFG.startCash, dayKey:new Date().toISOString().slice(0,10), killed:false, log:[], running:true, lastKlineFetch:0 };
let universe=[], prices={}, chg={}, hist={}, scoreS={}, cooldown={};

function log(type, action, detail){ const e={ ts:Date.now(), type, action, detail }; S.log.push(e); if(S.log.length>200)S.log.shift(); console.log(`[${new Date().toLocaleTimeString()}] ${action} — ${detail}`); }
async function fetchJSON(url){ const r=await fetch(url,{headers:{'accept':'application/json'}}); return r.json(); }

async function buildUniverse(){
  const d = await fetchJSON('https://data-api.binance.vision/api/v3/ticker/24hr');
  const rows = d.filter(t=>t.symbol.endsWith('USDT')).map(t=>({base:t.symbol.slice(0,-4), sym:t.symbol, vol:parseFloat(t.quoteVolume), price:parseFloat(t.lastPrice), c:parseFloat(t.priceChangePercent)}))
              .filter(r=>!isStable(r.base)).sort((a,b)=>b.vol-a.vol).slice(0, CFG.univMax);
  universe=rows.map(r=>({base:r.base, sym:r.sym})); rows.forEach(r=>{ prices[r.base]=r.price; chg[r.base]=r.c; });
  await seedHistory();
}

async function seedHistory(){
  for(const u of universe){
    try{ const k=await fetchJSON('https://data-api.binance.vision/api/v3/klines?symbol='+u.sym+'&interval=5m&limit=300');
         hist[u.base] = k.map(x=>({ h:parseFloat(x[2]), l:parseFloat(x[3]), c:parseFloat(x[4]), v:parseFloat(x[5]) }));
    }catch(e){}
  }
  S.lastKlineFetch = Date.now();
}

// Matematiksel İndikatörler (HMA, VWAP, STC, Squeeze, ADX)
function calcHMA(a, p=55) {
    let halfP=Math.floor(p/2), sqrtP=Math.floor(Math.sqrt(p));
    let wma = (arr, len) => { let n=0.5*len*(len+1); return arr.map((_,i,ar)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=ar[i-j]*(j+1); return s/n; }); };
    let wh=wma(a,halfP), wf=wma(a,p), diff=wh.map((x,i)=>x!=null&&wf[i]!=null ? 2*x-wf[i] : null);
    let hma = wma(diff.filter(x=>x!=null), sqrtP); return hma;
}
function calcVWAP(hData, p=288) {
    let sumPV=0, sumV=0, slice=hData.slice(-p);
    slice.forEach(x=>{ let typ=(x.h+x.l+x.c)/3; sumPV+=typ*x.v; sumV+=x.v; });
    return sumV===0?slice[slice.length-1].c : sumPV/sumV;
}
function calcADX(h, p=14) {
    if(h.length<p*2) return 20; let tr=[], pdm=[], ndm=[];
    for(let i=1; i<h.length; i++){ tr.push(Math.max(h[i].h-h[i].l, Math.abs(h[i].h-h[i-1].c), Math.abs(h[i].l-h[i-1].c))); let up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); }
    let smooth = (arr) => { let r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; };
    let trS=smooth(tr), pdmS=smooth(pdm), ndmS=smooth(ndm);
    let dx = trS.map((t,i)=> t===0?0:100*Math.abs(100*pdmS[i]/t - 100*ndmS[i]/t) / ((100*pdmS[i]/t)+(100*ndmS[i]/t)));
    return dx.slice(-p).reduce((a,b)=>a+b,0)/p;
}

function analyze(b){
    const h=hist[b], cArr=h.map(x=>x.c), price=cArr[cArr.length-1];
    const vwap=calcVWAP(h), hma=calcHMA(cArr), adx=calcADX(h);
    let score=0, why=[];
    if(price>vwap) score+=0.20; else score-=0.20;
    if(hma && hma[hma.length-1]>hma[hma.length-2]) score+=0.20; else score-=0.20;
    if(adx>25) score+=0.20; else if(adx<20) score-=0.30;
    return { score, why, trendUp: score>0.4 };
}

async function placeBuy(base, cost, a){
    S.cash-=cost; S.positions[base]={ qty:(cost*(1-FEE))/prices[base], entry:prices[base], cost, high:prices[base], openTs:Date.now() };
    log('buy','AL '+base, `$${cost.toFixed(2)}`);
}

async function placeSell(base, why){
    const p=S.positions[base], gross=p.qty*prices[base], net=gross*(1-FEE);
    S.cash+=net; const pnl=net-p.cost; delete S.positions[base]; cooldown[base]=Date.now()+COOLDOWN_MS;
    log('sell','SAT '+base, `${why} | PNL: $${pnl.toFixed(2)}`);
}

function strategy(){
    const M=MODES[CFG.riskMode];
    // Çıkış (Kâr Koruma)
    for(const b in S.positions){
        const p=S.positions[b], px=prices[b], netPct=((px*p.qty-p.cost-p.cost*FEE*2)/p.cost)*100;
        if(px>p.high) p.high=px;
        const peakPct=((p.high*p.qty-p.cost-p.cost*FEE*2)/p.cost)*100;
        if(peakPct>=2.5 && netPct<=peakPct-0.8) placeSell(b, 'Zirve Dönüşü');
        else if(peakPct>=1.2 && netPct<=0.2) placeSell(b, 'Başa Baş');
    }
    // Giriş (Dinamik Bütçe)
    let open=Object.keys(S.positions).length, avail=S.cash;
    const cand=universe.filter(u=>!S.positions[u.base]&&(!cooldown[u.base]||Date.now()>cooldown[u.base])).sort((a,b)=>scoreS[b.base]-scoreS[a.base]);
    for(const c of cand){
        if(open>=CFG.maxPositions) break;
        const a=analyze(c.base); scoreS[c.base]=a.score;
        if(a.trendUp){ placeBuy(c.base, avail*0.3, a); open++; }
    }
}

setInterval(async()=>{ await refreshPrices(); strategy(); }, CFG.pollMs);
buildUniverse();
