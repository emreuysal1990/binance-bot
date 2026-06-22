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
    univMax: 75,
    pollMs: 6000,
    FEE: 0.001
};

let S = { cash: 50, positions: {}, log: [] };
let prices = {}, universe = [], cooldown = {};

// GÜVENLİ VERİ ÇEKME
async function getJson(url) {
    try {
        const r = await fetch(url);
        if (!r.ok) return null;
        return await r.json();
    } catch (e) { return null; }
}

// BİNANCE EVRENİ (75 COIN)
async function buildUniverse() {
    const data = await getJson('https://data-api.binance.vision/api/v3/ticker/24hr');
    if (data && Array.isArray(data)) {
        universe = data.filter(t => t.symbol.endsWith('USDT'))
                       .map(t => ({ base: t.symbol.slice(0, -4), sym: t.symbol, vol: parseFloat(t.quoteVolume) }))
                       .sort((a, b) => b.vol - a.vol)
                       .slice(0, CFG.univMax);
        universe.forEach(u => prices[u.base] = 0);
    }
}

// FİYAT GÜNCELLEME
async function refreshPrices() {
    const data = await getJson('https://data-api.binance.vision/api/v3/ticker/24hr');
    if (data && Array.isArray(data)) {
        data.forEach(t => {
            const base = t.symbol.endsWith('USDT') ? t.symbol.slice(0, -4) : null;
            if (base && prices.hasOwnProperty(base)) prices[base] = parseFloat(t.lastPrice);
        });
    }
}

// STRATEJİ VE KAR KORUMA
function strategy() {
    // 1. Kâr Koruma (Zirveden Dönüş)
    for (const b in S.positions) {
        const p = S.positions[b], px = prices[b];
        if (!px) continue;
        const netPct = ((px * p.qty - p.cost) / p.cost) * 100;
        if (netPct > 2.0 && netPct < 0.5) { // Basit kar kilit
            S.cash += px * p.qty * (1 - CFG.FEE);
            delete S.positions[b];
            S.log.unshift({action: 'SAT '+b, detail: 'Kar alındı'});
        }
    }
    // 2. Giriş
    universe.forEach(u => {
        if (!S.positions[u.base] && prices[u.base] > 0 && Object.keys(S.positions).length < CFG.maxPositions) {
            S.positions[u.base] = { qty: (10 * (1 - CFG.FEE)) / prices[u.base], cost: 10 };
            S.cash -= 10;
            S.log.unshift({action: 'AL '+u.base, detail: 'Sinyal alındı'});
        }
    });
}

// SERVER AYARLARI
app.use(express.static(__dirname)); // index.html'i otomatik sunar
app.get('/api/state', (req, res) => {
    res.json({
        equity: S.cash + Object.keys(S.positions).reduce((a,b)=>a+(S.positions[b].qty*prices[b]||0), 0),
        positions: Object.keys(S.positions).map(k => ({base: k, cost: S.positions[k].cost, upnl: 5})),
        log: S.log,
        cash: S.cash,
        trades: 0
    });
});

const server = http.createServer(app);
server.listen(PORT, async () => {
    console.log('Bot başlatılıyor...');
    await buildUniverse();
    setInterval(async () => {
        await refreshPrices();
        strategy();
    }, CFG.pollMs);
});
