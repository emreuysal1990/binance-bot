- name: Run Backtest
    run: |
      cat << 'EOF' > backtest.js
      const CFG = {
        days: 90,
        interval: '1h',
        univMax: 40,
        startCash: 100,
        fee: 0.001,
        slip: 0.001,
        maxPositions: 5,
        pumpMax: 25,
        cooldownMin: 240,
        investTarget: 0.90,
        maxFrac: 0.35,
        minNotional: 10
      };
      
      const STABLES = ['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD','PYUSD','EUR','TRY','GBP','AEUR','XUSD','RLUSD','USDE','USD1','GUSD','LUSD','FRAX','USTC','EURI'];
      let coins={}, universe=[], closed=[];
      
      async function fetchBinance(url){ 
        const r=await fetch(url); 
        const data=await r.json(); 
        if(data.code) throw new Error(`Binance API Error: ${data.msg}`);
        return data;
      }
      
      async function fetchKlines(sym, interval, limit) {
          let results = [];
          let endTime = Date.now();
          let rem = limit;
          while(rem > 0) {
              const batch = Math.min(rem, 1000);
              const url = `https://api.binance.com/api/v3/klines?symbol=${sym}&interval=${interval}&limit=${batch}&endTime=${endTime}`;
              try {
                const r = await fetchBinance(url);
                if(!Array.isArray(r) || r.length === 0) break;
                results = r.concat(results); // Eski veriyi listenin başına ekle
                endTime = r[0][0] - 1; // Bir sonraki sorgu için bitiş zamanını kaydır
                rem -= r.length;
                await new Promise(res => setTimeout(res, 30));
              } catch(e) {
                console.error(`  Hata (${sym}):`, e.message);
                break;
              }
          }
          return results;
      }
      
      function wmaSeries(a,len){ const n=0.5*len*(len+1); return a.map((_,i,ar)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=ar[i-j]*(len-j); return s/n; }); }
      function calcHMA(a,p=21){ if(a.length<p)return null; const wh=wmaSeries(a,Math.floor(p/2)), wf=wmaSeries(a,p); const diff=wh.map((x,i)=>(x!=null&&wf[i]!=null)?2*x-wf[i]:null).filter(x=>x!=null); return wmaSeries(diff,Math.max(2,Math.floor(Math.sqrt(p)))); }
      function calcVWAP(h,p=24){ if(h.length<10)return null; let sPV=0,sV=0; h.slice(-p).forEach(x=>{ const t=(x.h+x.l+x.c)/3; sPV+=t*x.v; sV+=x.v; }); return sV===0?h[h.length-1].c:sPV/sV; }
      function calcADX(h,p=14){ if(h.length<p*2)return 20; const tr=[],pdm=[],ndm=[]; for(let i=1;i<h.length;i++){ tr.push(Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c))); const up=h[i].h-h[i-1].h, dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); } const sm=arr=>{ const r=[arr.slice(0,p).reduce((a,b)=>a+b,0)]; for(let i=p;i<arr.length;i++) r.push(r[r.length-1]-r[r.length-1]/p+arr[i]); return r; }; const trS=sm(tr),pdmS=sm(pdm),ndmS=sm(ndm); const dx=trS.map((t,i)=>{ if(t===0)return 0; const pdi=100*pdmS[i]/t, ndi=100*ndmS[i]/t, sum=pdi+ndi; return sum===0?0:100*Math.abs(pdi-ndi)/sum; }); return dx.slice(-p).reduce((a,b)=>a+b,0)/p; }
      function ema(arr,p){ if(!arr||arr.length<p)return null; const k=2/(p+1); let e=arr.slice(0,p).reduce((a,b)=>a+b,0)/p; for(let i=p;i<arr.length;i++) e=arr[i]*k+e*(1-k); return e; }
      function legUp(closes){ if(!closes||closes.length<30) return null; const e=ema(closes,20); return e? closes[closes.length-1]>e : null; }
      function clampN(x,a,b){ return Math.max(a,Math.min(b,x)); }
      function calcATR(h,p=14){ if(!h||h.length<p+1)return null; let s=0; for(let i=h.length-p;i<h.length;i++) s+=Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c)); return s/p; }

      async function run(){
        console.log(`==== GOMULU BACKTEST (self-contained) ====`);
        console.log(`>>> BACKTEST SURUMU: v4 (1h Mum, ADX>25, HMA Up, VWAP Up, 4h HTF Onay)`);
        console.log(`Veri cekiliyor: ${CFG.days} gun, ${CFG.interval} mum...`);
        
        const d24 = await fetchBinance('https://api.binance.com/api/v3/ticker/24hr');
        const rows=[];
        for(const t of d24){ 
          const s=t.symbol; 
          if(!s.endsWith('USDT')||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
          const base=s.slice(0,-4); if(STABLES.includes(base)) continue;
          if(+t.lastPrice>0) rows.push({base,sym:s,vol:+t.quoteVolume}); 
        }
        universe=rows.sort((a,b)=>b.vol-a.vol).slice(0,CFG.univMax);
        
        if (universe.length === 0) {
          console.error("Coin verisi yok.");
          process.exit(1);
        }
        
        // 1 saatlik mumlar için limit (gün x 24)
        const limit = Math.ceil(CFG.days * 24);
        const limit4h = Math.ceil(CFG.days * 6); // 4 saatlik mumlar için limit (gün x 6)
        
        for(let i=0; i<universe.length; i++){
          const u = universe[i];
          const k = await fetchKlines(u.sym, CFG.interval, limit);
          const k4 = await fetchKlines(u.sym, '4h', limit4h);
          
          if(Array.isArray(k) && k.length > 50) {
            coins[u.base] = { 
              h: k.map(x=>({ts:+x[0], h:+x[2], l:+x[3], c:+x[4], v:+x[5]})),
              h4: k4.map(x=>({ts:+x[0], c:+x[4]})),
              cd:0 
            };
          }
        }
        
        const BTC = coins['BTC']?.h; const L = BTC?.length || limit;
        if(!BTC) { console.error('BTC verisi yok, iptal.'); return; }
        
        let cash = CFG.startCash;
        let peak = cash;
        let positions = {};
        let trades = 0;
        
        // Isinma ve dongu baslangici
        const WARMUP = 50; 
        for(let i=WARMUP; i<L; i++){
          const nowTs = BTC[i].ts;
          let equity = cash;
          for(const b in positions){ const p=positions[b]; equity+=p.qty*coins[b].h[i].c; }
          if(equity>peak) peak=equity;
          
          // 1. CIKISLARI KONTROL ET (Trailing Stop & Stop Loss)
          for(const b in positions){
            const p = positions[b];
            const coin = coins[b];
            const current = coin.h[i];
            if(!current) continue;
            
            const px = current.c;
            if(px > p.high) p.high = px;
            
            const ap = p.atrPct || 2.0;
            const stopPct = ap * 2.0;           // Zarar Kes (2x ATR)
            const trailAct = ap * 2.5;          // Trailing Stop Aktivasyonu (2.5x ATR)
            const trailDist = ap * 2.0;         // Trailing Stop Mesafesi (2x ATR)
            
            const netPct = ((px*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
            const peakPct = ((p.high*p.qty - p.cost - p.cost*CFG.fee*2)/p.cost)*100;
            
            // Ani düşüş kontrolü
            const prevPx = coin.h[i-1]?.c || px;
            const fastRet = (px/prevPx-1)*100;
            
            let sell = false;
            let why = '';
            
            if(fastRet <= -(ap * 1.5)) { sell = true; why = 'Crash'; }
            else if (peakPct >= trailAct) {
                const trailingStopLevel = peakPct - trailDist;
                if (netPct <= trailingStopLevel) { sell = true; why = 'Trail'; }
            } 
            else if (netPct <= -stopPct) {
                { sell = true; why = 'Stop'; }
            }
            else if (peakPct >= (ap*1.5) && netPct <= 0.2) {
                { sell = true; why = 'BE'; }
            }
            
            if(sell){
              const fillPx = px*(1-CFG.slip);
              const gross = p.qty*fillPx;
              const fee = gross*CFG.fee;
              const net = gross-fee;
              cash += net;
              const pnl = net - p.cost;
              const pct = pnl/p.cost*100;
              closed.push({base:b, pnl, pct, ts:nowTs});
              delete positions[b];
              // Cooldown 4 saat: 1h mumu oldugu icin index i + 4
              coin.cd = i + Math.floor(CFG.cooldownMin/60); 
            }
          }
          
          // 2. GIRISLERI KONTROL ET (Trend Tespiti ve Alim)
          let open = Object.keys(positions).length;
          let avail = cash;
          const cands = [];
          
          for(const u of universe){
            const b = u.base;
            const coin = coins[b];
            if(!coin || !coin.h[i] || positions[b] || coin.cd > i) continue;
            
            const h = coin.h.slice(0, i+1);
            if(h.length < 50) continue;
            
            const cArr = h.map(x=>x.c);
            const px = cArr[cArr.length-1];
            const chg = ((px / cArr[Math.max(0, cArr.length-24)]) - 1) * 100; // 24h degisim
            
            const vwap = calcVWAP(h, 24);
            const hma = calcHMA(cArr, 21);
            const adx = calcADX(h, 14);
            const atr = calcATR(h, 14);
            const atrPct = (atr&&px>0) ? clampN(atr/px*100, 1.0, 8.0) : 2.0;
            
            const aboveVwap = vwap ? px > vwap : false;
            const hmaUp = !!(hma&&hma.length>=2&&hma[hma.length-1]!=null&&hma[hma.length-2]!=null&&hma[hma.length-1]>hma[hma.length-2]);
            
            // HTF (4h) onay: Zaman damgasina gore 4h datasindan filtrele
            let htfUp = true;
            if(coin.h4) {
               const h4Slice = coin.h4.filter(x => x.ts <= nowTs).map(x=>x.c);
               htfUp = legUp(h4Slice);
            }
            
            const overbought = chg >= CFG.pumpMax;
            
            let score=0;
            if(aboveVwap) score+=1; 
            if(hmaUp)     score+=1; 
            if(htfUp)     score+=1;

            // KATI TREND SARTLARI: ADX >= 25, Indikator Score >= 2, Gunu Pummplamamis (overbought)
            const trendUp = (adx >= 25) && (score >= 2) && !overbought;
            
            if(trendUp) {
              cands.push({b, rank: adx + (score*10), atrPct});
            }
          }
          
          cands.sort((x,y)=>y.rank-x.rank);
          
          // Sepet Dağılımı ve İşleme Giriş
          for(const c of cands){
            if(open >= CFG.maxPositions) break;
            const perSlot = CFG.investTarget / CFG.maxPositions;
            let alloc = equity * perSlot;
            alloc = Math.min(alloc, equity*CFG.maxFrac, avail*0.95);
            
            if(alloc < CFG.minNotional) continue;
            
            const fillPx = coins[c.b].h[i].c * (1+CFG.slip);
            const fee = alloc * CFG.fee;
            const qty = (alloc-fee)/fillPx;
            
            cash -= alloc;
            positions[c.b] = {qty, cost: alloc, entry: fillPx, high: fillPx, atrPct: c.atrPct};
            trades++;
            open++;
            avail -= alloc;
          }
        }
        
        // Period sonu eldekileri kapat
        for(const b in positions){
            const p = positions[b];
            const px = coins[b].h[L-1].c;
            const fillPx = px*(1-CFG.slip);
            const gross = p.qty*fillPx;
            const fee = gross*CFG.fee;
            const net = gross-fee;
            cash += net;
            const pnl = net - p.cost;
            const pct = pnl/p.cost*100;
            closed.push({base:b, pnl, pct});
        }
        
        const pnl = cash - CFG.startCash;
        const ret = pnl/CFG.startCash*100;
        const w = closed.filter(x=>x.pnl>=0);
        const l = closed.filter(x=>x.pnl<0);
        const grossWin = w.reduce((a,b)=>a+b.pnl,0);
        const grossLoss = Math.abs(l.reduce((a,b)=>a+b.pnl,0));
        const pf = grossLoss > 0 ? grossWin/grossLoss : (grossWin>0 ? 99 : 0);
        const wr = closed.length ? w.length/closed.length*100 : 0;
        
        console.log(`\n================= BACKTEST SONUCU =================`);
        console.log(`Donem         : son ${CFG.days} gun · ${CFG.interval} mum`);
        console.log(`Baslangic     : $${CFG.startCash.toFixed(2)}`);
        console.log(`Bitis         : $${cash.toFixed(2)}`);
        console.log(`KAR/ZARAR     : ${ret.toFixed(2)}%  ($${pnl.toFixed(2)})`);
        console.log(`Islem sayisi  : ${closed.length}`);
        console.log(`Kazanma orani : ${wr.toFixed(2)}%  (${w.length}W / ${l.length}L)`);
        console.log(`Profit factor : ${pf.toFixed(2)}`);
        console.log(`===================================================`);
      }
      
      run().catch(console.error);
      EOF
      node backtest.js
