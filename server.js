'use strict';
/*
 * SURVIVE & GROW — Final Optimum Versiyon
 * Ozellikler: Dinamik Butce, 75 Coin Havuzu, 15dk Cooldown, Zirveden Donus Kar Koruma
 */
try { require('dotenv').config(); } catch (e) {}
const fs = require('fs');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const CFG = {
  mode: (process.env.MODE || 'paper').toLowerCase(),
  quote: process.env.QUOTE || 'USDT',
  startCash: +(process.env.START_CASH || 50),
  univMax: 75,
  maxPositions: +(process.env.MAX_POSITIONS || 6),
  pollMs: 6000,
  minNotional: 10,
  FEE: 0.001
};

let S = { cash:CFG.startCash, positions:{}, trades:0, peak:CFG.startCash, log:[], running:true };
let universe=[], prices={}, hist={}, scoreS={}, cooldown={};

// --- MATEMATIK & INDIKATORLER ---
function calcHMA(a, p=55) {
    let wma = (arr, len) => arr.map((_,i,ar)=>{ if(i<len-1)return null; let s=0, n=0.5*len*(len+1); for(let j=0;j<len;j++) s+=ar[i-j]*(j+1); return s/n; });
    let wh=wma(a,Math.floor(p/2)), wf=wma(a,p), diff=wh.map((x,i)=>x!=null&&wf[i]!=null ? 2*x-wf[i] : null);
    return wma(diff.filter(x=>x!=null), Math.floor(Math.sqrt(p)));
}
function calcVWAP(h, p=288) {
    let sPV=0, sV=0; h.slice(-p).forEach(x=>{ let t=(x.h+x.l+x.c)/3; sPV+=t*x.v; sV+=x.v; });
    return sV===0?h[h.length-1].c : sPV/sV;
}
function calcADX(h, p=14) {
    if(h.length<p*2) return 20; let tr=[], pdm=[], ndm=[];
    for(let i=1; i<h.length; i++){ tr.push(Math.max(h[i].h-h[i].l, Math.abs(h[i].h-h[i-1].c), Math.abs(h[i].l-h[i-1].c))); let up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); }
    let sm=(arr)=>{ let r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; };
    let trS=sm(tr), pdmS=sm(pdm), ndmS=sm(ndm), dx=trS.map((t,i)=> t===0?0:100*Math.abs((100*pdmS[i]/t)-(100*ndmS[i]/t))/((100*pdmS[i]/t)+(100*ndmS[i]/t)));
    return dx.slice(-p).reduce((a,b)=>a+b,0)/p;
}

function analyze(b){
    const h=hist[b], c=h.map(x=>x.c), p=c[c.length-1];
    const vwap=calcVWAP(h), hma=calcHMA(c), adx=calcADX(h);
    let sc=0;
    if(p>vwap) sc+=0.20; else sc-=0.20;
    if(hma && hma[hma.length-1]>hma[hma.length-2]) sc+=0.20; else sc-=0.20;
    if(adx>25) sc+=0.20; else if(adx<20) sc-=0.30;
    return { score:sc, trendUp:sc>0.4 };
}

// --- CORE ---
async function strategy(){
    for(const b in S.positions){
        const p=S.positions[b], px=prices[b], netPct=((px*p.qty-p.cost-p.cost*CFG.FEE*2)/p.cost)*100;
        if(px>p.high) p.high=px;
        const peakPct=((p.high*p.qty-p.cost-p.cost*CFG.FEE*2)/p.cost)*100;
        if(peakPct>=2.5 && netPct<=peakPct-0.8 || (peakPct>=1.2 && netPct<=0.2)) placeSell(b, 'Kar Koruması');
    }
    let open=Object.keys(S.positions).length, avail=S.cash;
    const cand=universe.filter(u=>!S.positions[u.base]&&(!cooldown[u.base]||Date.now()>cooldown[u.base])).sort((a,b)=>scoreS[b.base]-scoreS[a.base]);
    for(const c of cand){
        if(open>=CFG.maxPositions) break;
        const a=analyze(c.base); scoreS[c.base]=a.score;
        if(a.trendUp){ S.cash-=avail*0.3; S.positions[c.base]={qty:((avail*0.3)*(1-CFG.FEE))/prices[c.base], cost:avail*0.3, high:prices[c.base]}; open++; }
    }
}

async function placeSell(b, w){ 
    S.cash+=S.positions[b].qty*prices[b]*(1-CFG.FEE); delete S.positions[b]; cooldown[b]=Date.now()+900000;
    S.log.unshift({action:'SAT '+b, detail:w}); 
}

const app=express(); app.use(express.json());
app.get('/api/state', (req,res)=>res.json({equity:S.cash+Object.values(S.positions).reduce((a,b)=>a+(b.qty*prices[Object.keys(S.positions)[0]]),0), positions:Object.keys(S.positions).map(k=>({base:k, cost:S.positions[k].cost, upnl:10})), log:S.log, cash:S.cash }));
app.listen(CFG.port);
setInterval(async()=>{ await fetch('...').then(d=>universe.forEach(u=>prices[u.base]=d.price)); strategy(); }, 6000);
