name: Backtest Trend Takip
on:
  workflow_dispatch:
    inputs:
      days:
        description: "Kac gun"
        required: true
        default: "90"
      univ:
        description: "Kac coin (en hacimli)"
        required: true
        default: "40"
      interval:
        description: "Mum"
        required: true
        default: "1h"
      start:
        description: "Baslangic USDT"
        required: true
        default: "100"

jobs:
  run-backtest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Backtest calistir
        env:
          DAYS: ${{ github.event.inputs.days }}
          UNIV: ${{ github.event.inputs.univ }}
          INTERVAL: ${{ github.event.inputs.interval }}
          START: ${{ github.event.inputs.start }}
        run: |
          echo "==== GOMULU BACKTEST (KATI TREND TAKIP) ===="
          cat > bt_run.js <<'JSEOF'
          'use strict';
          console.log('>>> BACKTEST: KATI TREND (Kısmi kâr YOK, sadece trailing stop, ADX>25)');
          const fs = require('fs');
          
          const CFG = {
            days: +(process.env.DAYS || 90),
            univMax: +(process.env.UNIV || 40),
            interval: process.env.INTERVAL || '1h',
            start: +(process.env.START || 100),
            fee: 0.001,
            slip: 0.001,
            maxPos: 5,
            pumpMax: 25,
            cooldownTicks: 4, // 1h mumda 4 saat
            htf: true
          };

          const STABLES = ['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD'];
          let coins={}, universe=[], closed=[];
          
          async function fetchBinance(url){ const r=await fetch(url); return r.json(); }
          
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

          async function run(){
            console.log(`Veri cekiliyor: ${CFG.days} gun, ${CFG.interval} mum...`);
            const d24 = await fetchBinance('https://data-api.binance.vision/api/v3/ticker/24hr');
            const rows=[];
            for(const t of d24){ const s=t.symbol; if(!s.endsWith('USDT')||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
              const base=s.slice(0,-4); if(STABLES.includes(base)) continue;
              if(+t.lastPrice>0) rows.push({base,sym:s,vol:+t.quoteVolume}); }
            universe=rows.sort((a,b)=>b.vol-a.vol).slice(0,CFG.univMax);
            
            const limit = Math.ceil(CFG.days * 24 * (CFG.interval==='1h'?1:4));
            for(let i=0; i<universe.length; i++){
              const u = universe[i];
              const k = await fetchBinance(`https://data-api.binance.vision/api/v3/klines?symbol=${u.sym}&interval=${CFG.interval}&limit=${limit}`);
              if(Array.isArray(k)) coins[u.base] = { h: k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]})), cd:0 };
            }
            
            const BTC = coins['BTC']?.h; const L = BTC?.length || limit;
            let cash = CFG.start, pos = {}, peak = cash;
            
            console.log(`\nSİMÜLASYON BAŞLIYOR... (${L} mum)\n`);
            
            for(let i=50; i<L; i++){
              // CIKISLAR
              for(const b in pos){
                const p = pos[b], h = coins[b].h[i]; if(!h) continue;
                if(h.h > p.high) p.high = h.h;
                
                const ap = p.atrPct || 2.0;
                const stopPct = ap * 2.0;
                const trailAct = ap * 2.5;
                const trailDist = ap * 2.0;
                
                const netPct = ((h.c*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
                const peakPct = ((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
                
                let sell = false, why='';
                
                if (peakPct >= trailAct) {
                    if (netPct <= peakPct - trailDist) { sell=true; why='Trail'; }
                } else if (netPct <= -stopPct) { sell=true; why='Stop'; }
                else if (peakPct >= (ap*1.5) && netPct <= 0.2) { sell=true; why='BE'; }
                
                if(sell){
                  const fillPx = h.c*(1-CFG.slip), net = p.qty*fillPx*(1-CFG.fee);
                  cash += net; closed.push({b, pnl:net-p.cost, pct: (net-p.cost)/p.cost*100, why});
                  delete pos[b]; coins[b].cd = i + CFG.cooldownTicks;
                }
              }
              
              // GIRISLER
              let open = Object.keys(pos).length;
              if(open >= CFG.maxPos) continue;
              
              const cands = [];
              for(const u of universe){
                const b = u.base, coin = coins[b]; if(!coin || pos[b] || coin.cd > i) continue;
                const subH = coin.h.slice(0, i+1); if(subH.length<50) continue;
                
                const cArr = subH.map(x=>x.c), px = cArr[cArr.length-1];
                const adx = calcADX(subH), vwap = calcVWAP(subH, 24), hma = calcHMA(cArr, 21);
                const atr = calcATR(subH, 14), atrPct = clampN(atr/px*100, 1.0, 8.0);
                
                const aboveVwap = vwap ? px>vwap : false;
                const hmaUp = (hma&&hma.length>2) ? hma[hma.length-1]>hma[hma.length-2] : false;
                
                // HTF (4h sentetik olarak 1h uzerinden hesaplanir backtest icin)
                let htfUp = true;
                if(subH.length>100){
                   const h4 = []; for(let j=0; j<subH.length; j+=4) h4.push(subH[j].c);
                   htfUp = legUp(h4);
                }
                
                let score = 0;
                if(aboveVwap) score++; if(hmaUp) score++; if(htfUp) score++;
                
                const chg24 = subH.length>24 ? (px/subH[subH.length-24].c-1)*100 : 0;
                
                if(adx >= 25 && score >= 2 && chg24 < CFG.pumpMax) {
                   cands.push({b, px, atrPct, rank: adx});
                }
              }
              cands.sort((x,y)=>y.rank-x.rank);
              
              for(const c of cands){
                 if(open >= CFG.maxPos) break;
                 let alloc = (cash + Object.values(pos).reduce((s,p)=>s+p.cost,0)) * (0.90/CFG.maxPos);
                 alloc = Math.min(alloc, cash*0.95);
                 if(alloc<10) continue;
                 
                 const fillPx = c.px*(1+CFG.slip), cost = alloc, qty = (cost*(1-CFG.fee))/fillPx;
                 cash -= cost; pos[c.b] = {qty, cost, entry:fillPx, high:fillPx, atrPct:c.atrPct};
                 open++;
              }
            }
            
            // SONUC RAPORU
            let eq = cash; for(const b in pos) eq += pos[b].qty * coins[b].h[L-1].c;
            const netP = ((eq-CFG.start)/CFG.start)*100;
            
            let w=0,l=0,sw=0,sl=0;
            for(const c of closed){ if(c.pnl>=0){w++;sw+=c.pnl;}else{l++;sl+=Math.abs(c.pnl);} }
            const pf = sl>0 ? sw/sl : 99;
            const winRate = closed.length>0 ? (w/closed.length)*100 : 0;
            const avgW = w>0 ? sw/w : 0; const avgL = l>0 ? sl/l : 0;
            
            console.log("======================================");
            console.log(`NET KAZANÇ  : ${netP>=0?'+':''}${netP.toFixed(2)}%  ($${eq.toFixed(2)})`);
            console.log(`İşlem Sayısı: ${closed.length}`);
            console.log(`Kazanma Oranı: %${winRate.toFixed(2)}`);
            console.log(`Ort. Kâr / Ort. Zarar: +$${avgW.toFixed(2)} / -$${avgL.toFixed(2)}`);
            console.log(`Payoff (Risk/Ödül): ${(avgL>0 ? avgW/avgL : 99).toFixed(2)}`);
            console.log(`Profit Factor (PF): ${pf.toFixed(2)}`);
            console.log("======================================");
          }
          run().catch(console.error);
          JSEOF
          node bt_run.js
