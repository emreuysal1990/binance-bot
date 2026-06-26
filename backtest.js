      const STABLES = ['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD'];
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
                results = r.concat(results); // Eski veriyi listenin başına ekle (kronolojik sıra)
                endTime = r[0][0] - 1; // Bir sonraki sorgu için bitiş zamanını kaydır
                rem -= r.length;
                await new Promise(res => setTimeout(res, 30)); // Binance rate-limit koruması
              } catch(e) {
                console.error(`  Hata (${sym}):`, e.message);
                break;
              }
          }
          return results;
      }
      
      function wmaSeries(a,len){ const n=0.5*len*(len+1); return a.map((_,i,ar)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=ar[i-j]*(len-j); return s/n; }); }


      async function run(){
        console.log(`Veri cekiliyor: ${CFG.days} gun, ${CFG.interval} mum...`);
        const d24 = await fetchBinance('https://api.binance.com/api/v3/ticker/24hr');
        const rows=[];
        for(const t of d24){ const s=t.symbol; if(!s.endsWith('USDT')||/(UP|DOWN|BULL|BEAR)USDT$/.test(s)) continue;
          const base=s.slice(0,-4); if(STABLES.includes(base)) continue;
          if(+t.lastPrice>0) rows.push({base,sym:s,vol:+t.quoteVolume}); }
        universe=rows.sort((a,b)=>b.vol-a.vol).slice(0,CFG.univMax);
        
        if (universe.length === 0) {
          console.error("Coin verisi yok.");
          process.exit(1);
        }
        
        // 1 saatlik mumlar için gün x 24 limit hesabı
        const limit = Math.ceil(CFG.days * 24);
        for(let i=0; i<universe.length; i++){
          const u = universe[i];
          process.stdout.write(`Veri cekiliyor ${u.sym}... `);
          const k = await fetchKlines(u.sym, CFG.interval, limit);
          if(Array.isArray(k) && k.length > 0) {
            coins[u.base] = { h: k.map(x=>({h:+x[2],l:+x[3],c:+x[4],v:+x[5]})), cd:0 };
            console.log(`${k.length} mum.`);
          } else {
            console.log("Veri yok veya hata.");
          }
        }
        
        const BTC = coins['BTC']?.h; const L = BTC?.length || limit;
