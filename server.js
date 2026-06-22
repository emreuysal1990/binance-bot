'use strict';
require('dotenv').config();
const express = require('express');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
const PORT = process.env.PORT || 8080;

// AYARLAR
const CFG = {
    maxPositions: +(process.env.MAX_POSITIONS || 6),
    univMax: +(process.env.UNIVERSE || 75),
    pollMs: 6000,
    FEE: 0.001,
    token: process.env.DASH_TOKEN || 'change-me',
    startCash: +(process.env.START_CASH || 50)
};

let S = { cash: CFG.startCash, positions: {}, log: [], trades: 0 };
let prices = {}, universe = [], hist = {}, cooldown = {};

// GÜVENLİ VERİ ÇEKME
async function getJson(url) {
    if (!url || url.includes('...')) return null;
    try {
        const r = await fetch(url);
        if (!r.ok) return null;
        return await r.json();
    } catch (e) { return null; }
}

// BİNANCE EVRENİ VE GEÇMİŞİ (75 COIN)
async function buildUniverse() {
    const data = await getJson('https://data-api.binance.vision/api/v3/ticker/24hr');
    if (data && Array.isArray(data)) {
        universe = data.filter(t => t.symbol.endsWith('USDT'))
                       .map(t => ({ base: t.symbol.slice(0, -4), sym: t.symbol, vol: parseFloat(t.quoteVolume) }))
                       .sort((a, b) => b.vol - a.vol)
                       .slice(0, CFG.univMax);
        
        for (const u of universe) {
            prices[u.base] = 0;
            const klines = await getJson(`https://data-api.binance.vision/api/v3/klines?symbol=${u.sym}&interval=5m&limit=300`);
            if (klines && Array.isArray(klines)) {
                hist[u.base] = klines.map(x => ({ h: parseFloat(x[2]), l: parseFloat(x[3]), c: parseFloat(x[4]), v: parseFloat(x[5]) }));
            }
        }
    }
}

// FİYAT GÜNCELLEME VE MUM GÜNCELLEMESİ
async function refreshPrices() {
    const data = await getJson('https://data-api.binance.vision/api/v3/ticker/24hr');
    if (data && Array.isArray(data)) {
        data.forEach(t => {
            const base = t.symbol.endsWith('USDT') ? t.symbol.slice(0, -4) : null;
            if (base && prices.hasOwnProperty(base)) {
                const p = parseFloat(t.lastPrice);
                prices[base] = p;
                if (hist[base] && hist[base].length > 0) {
                    const last = hist[base][hist[base].length - 1];
                    last.c = p;
                    if (p > last.h) last.h = p;
                    if (p < last.l) last.l = p;
                }
            }
        });
    }
}

// İNDİKATÖRLER VE MATEMATİK
function calcHMA(a, p=55) {
    if(a.length < p) return null;
    let wma = (arr, len) => arr.map((_,i,ar)=>{ if(i<len-1)return null; let s=0, n=0.5*len*(len+1); for(let j=0;j<len;j++) s+=ar[i-j]*(j+1); return s/n; });
    let wh = wma(a, Math.floor(p/2)), wf = wma(a, p);
    let diff = wh.map((x,i) => x!=null && wf[i]!=null ? 2*x-wf[i] : null);
    return wma(diff.filter(x=>x!=null), Math.floor(Math.sqrt(p)));
}

function calcVWAP(h, p=288) {
    if(h.length < 10) return null;
    let sPV=0, sV=0; 
    h.slice(-p).forEach(x=>{ let t=(x.h+x.l+x.c)/3; sPV+=t*x.v; sV+=x.v; });
    return sV===0 ? h[h.length-1].c : sPV/sV;
}

function calcADX(h, p=14) {
    if(h.length < p*2) return 20; 
    let tr=[], pdm=[], ndm=[];
    for(let i=1; i<h.length; i++){ 
        tr.push(Math.max(h[i].h-h[i].l, Math.abs(h[i].h-h[i-1].c), Math.abs(h[i].l-h[i-1].c))); 
        let up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; 
        pdm.push(up>dn && up>0 ? up : 0); 
        ndm.push(dn>up && dn>0 ? dn : 0); 
    }
    let sm = (arr) => { let r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; };
    let trS = sm(tr), pdmS = sm(pdm), ndmS = sm(ndm);
    let dx = trS.map((t,i) => t===0 ? 0 : 100*Math.abs((100*pdmS[i]/t)-(100*ndmS[i]/t))/((100*pdmS[i]/t)+(100*ndmS[i]/t)));
    return dx.slice(-p).reduce((a,b)=>a+b,0)/p;
}

function analyze(b) {
    const hData = hist[b];
    if(!hData || hData.length < 100) return { score: 0, trendUp: false };
    
    const cArr = hData.map(x=>x.c);
    const px = cArr[cArr.length-1];
    const vwap = calcVWAP(hData);
    const hma = calcHMA(cArr);
    const adx = calcADX(hData);
    
    let score = 0;
    if(vwap && px > vwap) score += 0.20; else score -= 0.20;
    if(hma && hma[hma.length-1] != null && hma[hma.length-1] > hma[hma.length-2]) score += 0.20; else score -= 0.25;
    if(adx > 25) score += 0.20; else if(adx < 20) score -= 0.30;
    
    return { score, trendUp: score >= 0.40 };
}

// STRATEJİ VE KAR KORUMA
function strategy() {
    // 1. ÇIKIŞ: Kâr Koruma ve Stop
    for (const b in S.positions) {
        const p = S.positions[b], px = prices[b];
        if (!px) continue;
        
        if (px > p.high) p.high = px; // Zirveyi güncelle
        
        const netPct = ((px * p.qty - p.cost - (p.cost * CFG.FEE * 2)) / p.cost) * 100;
        const peakPct = ((p.high * p.qty - p.cost - (p.cost * CFG.FEE * 2)) / p.cost) * 100;
        
        let why = null;
        if (peakPct >= 2.5 && netPct <= peakPct - 0.8) why = 'Zirveden Dönüş (Kar Kilitlendi)';
        else if (peakPct >= 1.2 && netPct <= 0.2) why = 'Başa Baş Koruması';
        else if (netPct <= -3.0) why = 'Zarar Kes'; // Güvenlik ağı

        if (why) {
            S.cash += px * p.qty * (1 - CFG.FEE);
            delete S.positions[b];
            cooldown[b] = Date.now() + (15 * 60 * 1000); // 15 Dk Cooldown
            S.log.unshift({action: 'SAT '+b, detail: why + ` (%${netPct.toFixed(2)})`});
        }
    }
    
    // 2. GİRİŞ: Dinamik Bütçe ve Trend
    let open = Object.keys(S.positions).length;
    const cand = universe.filter(u => !S.positions[u.base] && (!cooldown[u.base] || Date.now() > cooldown[u.base]));
    
    for (const c of cand) {
        if (open >= CFG.maxPositions) break;
        const a = analyze(c.base);
        
        if (a.trendUp) {
            // Kasayı bölerken işlem sayısına dikkat et, minimum $10 gir
            let alloc = Math.max(10, S.cash / (CFG.maxPositions - open + 1));
            if (alloc > S.cash * 0.95) alloc = S.cash * 0.95; 
            
            if (alloc >= 10 && prices[c.base] > 0) {
                S.positions[c.base] = { 
                    qty: (alloc * (1 - CFG.FEE)) / prices[c.base], 
                    cost: alloc,
                    entry: prices[c.base],
                    high: prices[c.base]
                };
                S.cash -= alloc;
                S.trades++;
                S.log.unshift({action: 'AL '+c.base, detail: 'VWAP+HMA Trend Onayı'});
                open++;
            }
        }
    }
}

// ARAYÜZ VERİ PAKETİ
function snapshot() {
    let eq = S.cash;
    let posArr = [];
    for (const b in S.positions) {
        const p = S.positions[b];
        const px = prices[b] || p.entry;
        const currentVal = p.qty * px;
        eq += currentVal;
        posArr.push({
            base: b,
            entry: p.entry,
            price: px,
            cost: p.cost,
            qty: p.qty,
            upnl: currentVal - p.cost - (p.cost * CFG.FEE * 2)
        });
    }
    let pnlPct = ((eq - CFG.startCash) / CFG.startCash) * 100;
    
    return {
        equity: eq,
        pnlPct: pnlPct,
        cash: S.cash,
        trades: S.trades,
        positions: posArr,
        log: S.log.slice(0, 50)
    };
}

// SUNUCU VE İLETİŞİM
app.use(express.static(__dirname)); 
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/api/state', (req, res) => res.json(snapshot()));

app.post('/api/pause', (req, res) => res.json({ok: true}));
app.post('/api/resume', (req, res) => res.json({ok: true}));
app.post('/api/close-all', (req, res) => {
    for (const b in S.positions) {
        S.cash += S.positions[b].qty * prices[b] * (1 - CFG.FEE);
        delete S.positions[b];
        S.log.unshift({action: 'SAT '+b, detail: 'Manuel Kapatıldı'});
    }
    res.json({ok: true});
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (socket, req) => {
    const u = new URL(req.url, 'http://localhost');
    if (u.searchParams.get('token') !== CFG.token) {
        socket.close(); return;
    }
    socket.send(JSON.stringify(snapshot()));
});

function broadcast() {
    const msg = JSON.stringify(snapshot());
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

server.listen(PORT, async () => {
    console.log('Bot başlatılıyor...');
    await buildUniverse();
    setInterval(async () => {
        await refreshPrices();
        strategy();
        broadcast(); 
    }, CFG.pollMs);
});
