name: Backtest

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
        default: "20"
      interval:
        description: "Mum (15m/5m)"
        required: true
        default: "15m"
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
          echo "==== GOMULU BACKTEST (self-contained) ===="
          cat > bt_run.js <<'JSEOF'
          'use strict';
          console.log('>>> BACKTEST SURUMU: v4  (YATAY KAPALI · sadece trend · evren 20 · entryScore 0.40 · yari-bolme + GECTI Mi)');
          /* backtest.js — Survive & Grow stratejisini gecmis Binance verisinde test eder.
           * Canli bot (server.js) ile AYNI mantik: rejim (ADX/Choppiness) + trend/mean-reversion,
           * 1h+4h yon onayi, ATR'ye uyarlanan stop/kismi-kar/trailing, flash korumasi,
           * cop+vahsi coin filtresi, per-slot (hedef yatirim) boyutlandirma, komisyon+slipaj,
           * gun-ici kill-switch, cooldown.
           *
           * Kullanim (INTERNET gerekir; bu sandbox'ta Binance engelli — kendi makinende calistir):
           *   node backtest.js                          # son 90 gun, 15m, top 25 coin, $100
           *   DAYS=90 INTERVAL=15m UNIV=30 START=100 node backtest.js
           *   node backtest.js --demo                   # sahte veri (ANLAMSIZ, sadece format)
           *   Windows PowerShell: $env:DAYS="90"; node backtest.js
           */
          const BN='https://data-api.binance.vision';
          const DEMO=process.argv.slice(2).includes('--demo');
          const CFG={
            days:+(process.env.DAYS||90), interval:(process.env.INTERVAL||'15m'), univ:+(process.env.UNIV||20),
            tradeRange:(process.env.RANGE_TRADES||'0')!=='0',
            start:+(process.env.START||100), maxPos:+(process.env.MAX_POSITIONS||8),
            fee:+(process.env.FEE||0.001), slip:+(process.env.SLIP||0.0005),
            entryScore:+(process.env.ENTRY_SCORE||0.40), htf:(process.env.HTF||'1')!=='0',
            pumpMax:+(process.env.PUMP_MAX||40), tp1Frac:+(process.env.TP1_FRAC||0.34),
            dailyLossStop:+(process.env.DAILY_LOSS_STOP||0.15), minNotional:+(process.env.MIN_NOTIONAL||10),
            cooldownMin:+(process.env.COOLDOWN_MIN||60), maxTrade:+(process.env.MAX_TRADE_USDT||0),
            atrExits:(process.env.ATR_EXITS||'1')!=='0', atrStopK:+(process.env.ATR_STOP_K||1.3), atrTpK:+(process.env.ATR_TP_K||2.2),
            maxAtrPct:+(process.env.MAX_ATR_PCT||6), investTarget:+(process.env.INVEST_TARGET||85)/100, maxFrac:+(process.env.MAX_POS_PCT||35)/100,
            maxNewPerTick:+(process.env.MAX_NEW_PER_TICK||3), flashDropK:+(process.env.FLASH_DROP_K||1.2), flashSpikeK:+(process.env.FLASH_SPIKE_K||1.6),
            spikeEntryK:+(process.env.SPIKE_ENTRY_K||2.0),
          };
          const STABLES=['USDC','FDUSD','TUSD','DAI','USDP','BUSD','USDD','PYUSD','EUR','TRY','GBP','AEUR','XUSD','RLUSD','USDE','USD1','GUSD','LUSD','FRAX','USTC','EURI'];
          const intMin=({'1m':1,'3m':3,'5m':5,'15m':15,'30m':30,'1h':60,'2h':120,'4h':240})[CFG.interval]||15;
          const VWAP_LEN=Math.max(20,Math.round(1440/intMin));
          const clampN=(x,a,b)=>Math.max(a,Math.min(b,x));

          // ---------- veri ----------
          async function getJSON(u){ const r=await fetch(u,{headers:{'accept':'application/json','user-agent':'sg-bt/2.0'}}); if(!r.ok) throw new Error('HTTP '+r.status+' '+u); return r.json(); }
          async function klines(sym,interval,startMs,endMs){ let out=[],cur=startMs;
            while(cur<endMs){ const k=await getJSON(`${BN}/api/v3/klines?symbol=${sym}&interval=${interval}&startTime=${cur}&limit=1000`);
              if(!k.length) break; out=out.concat(k); cur=k[k.length-1][0]+1; if(k.length<1000) break; }
            return out.filter(x=>x[0]<endMs).map(x=>({t:x[0],h:+x[2],l:+x[3],c:+x[4],v:+x[5]})); }

          // ---------- indikator serileri (server.js ile ayni) ----------
          function wmaS(a,len){ const n=0.5*len*(len+1); return a.map((_,i)=>{ if(i<len-1)return null; let s=0; for(let j=0;j<len;j++) s+=a[i-j]*(len-j); return s/n; }); }
          function hmaS(a,p=55){ const wh=wmaS(a,Math.floor(p/2)),wf=wmaS(a,p),sq=Math.max(2,Math.floor(Math.sqrt(p)));
            const diff=a.map((_,i)=>(wh[i]!=null&&wf[i]!=null)?2*wh[i]-wf[i]:null), out=a.map(()=>null), nn=0.5*sq*(sq+1);
            for(let i=0;i<a.length;i++){ let s=0,ok=true; for(let j=0;j<sq;j++){ const v=diff[i-j]; if(v==null){ok=false;break;} s+=v*(sq-j);} if(ok)out[i]=s/nn; } return out; }
          function emaS(a,p){ const out=a.map(()=>null); if(a.length<p)return out; const k=2/(p+1); let e=a.slice(0,p).reduce((x,y)=>x+y,0)/p; out[p-1]=e; for(let i=p;i<a.length;i++){ e=a[i]*k+e*(1-k); out[i]=e; } return out; }
          function rsiS(a,p=14){ const out=a.map(()=>50); for(let i=p;i<a.length;i++){ let g=0,l=0; for(let k=i-p+1;k<=i;k++){ const d=a[k]-a[k-1]; if(d>=0)g+=d; else l-=d; } out[i]= l===0?100:100-100/(1+(g/p)/(l/p)); } return out; }
          function bbPctBS(a,p=20,k=2){ const out=a.map(()=>0.5); for(let i=p-1;i<a.length;i++){ const s=a.slice(i-p+1,i+1),m=s.reduce((x,y)=>x+y,0)/p; const sd=Math.sqrt(s.reduce((x,y)=>x+(y-m)*(y-m),0)/p),up=m+k*sd,lo=m-k*sd; out[i]=(up-lo)>0?(a[i]-lo)/(up-lo):0.5; } return out; }
          function vwapS(h,p){ const out=h.map(()=>null); for(let i=0;i<h.length;i++){ if(i<9)continue; const s=h.slice(Math.max(0,i-p+1),i+1); let pv=0,v=0; s.forEach(x=>{const t=(x.h+x.l+x.c)/3; pv+=t*x.v; v+=x.v;}); out[i]= v===0?h[i].c:pv/v; } return out; }
          function adxS(h,p=14){ const out=h.map(()=>20); if(h.length<p*2)return out; const tr=[0],pdm=[0],ndm=[0];
            for(let i=1;i<h.length;i++){ tr.push(Math.max(h[i].h-h[i].l,Math.abs(h[i].h-h[i-1].c),Math.abs(h[i].l-h[i-1].c))); const up=h[i].h-h[i-1].h,dn=h[i-1].l-h[i].l; pdm.push(up>dn&&up>0?up:0); ndm.push(dn>up&&dn>0?dn:0); }
            const dx=h.map(()=>0); let atr=0,ap=0,an=0;
            for(let i=1;i<h.length;i++){ if(i<=p){ atr+=tr[i]; ap+=pdm[i]; an+=ndm[i]; if(i===p){ const pdi=100*ap/atr,ndi=100*an/atr,su=pdi+ndi; dx[i]=su?100*Math.abs(pdi-ndi)/su:0; } continue; }
              atr=atr-atr/p+tr[i]; ap=ap-ap/p+pdm[i]; an=an-an/p+ndm[i]; const pdi=100*ap/atr,ndi=100*an/atr,su=pdi+ndi; dx[i]=su?100*Math.abs(pdi-ndi)/su:0; }
            for(let i=2*p;i<h.length;i++){ let s=0; for(let k=i-p+1;k<=i;k++) s+=dx[k]; out[i]=s/p; } return out; }
          function chopS(h,p=14){ const out=h.map(()=>50); for(let i=p;i<h.length;i++){ const seg=h.slice(i-p,i+1); let tr=0; for(let j=1;j<seg.length;j++) tr+=Math.max(seg[j].h-seg[j].l,Math.abs(seg[j].h-seg[j-1].c),Math.abs(seg[j].l-seg[j-1].c)); const hh=Math.max(...seg.slice(1).map(x=>x.h)),ll=Math.min(...seg.slice(1).map(x=>x.l)),rng=hh-ll; out[i]= (rng<=0||tr<=0)?50:Math.max(0,Math.min(100,100*Math.log10(tr/rng)/Math.log10(p))); } return out; }
          function atrPctS(h,p=14){ const out=h.map(()=>null); for(let i=p;i<h.length;i++){ let s=0; for(let k=i-p+1;k<=i;k++) s+=Math.max(h[k].h-h[k].l,Math.abs(h[k].h-h[k-1].c),Math.abs(h[k].l-h[k-1].c)); const atr=s/p, c=h[i].c; out[i]= c>0?clampN(atr/c*100,0.3,8):null; } return out; }

          function prep(h){ const c=h.map(x=>x.c);
            return { h, c, hma:hmaS(c,55), vwap:vwapS(h,VWAP_LEN), adx:adxS(h,14), chop:chopS(h,14), rsi:rsiS(c,14), pctB:bbPctBS(c,20,2), atrPct:atrPctS(h,14) }; }

          // server.js analyze() ile birebir
          function sigAt(P,i){ const c=P.c[i],hma=P.hma[i],hmaPrev=P.hma[i-1],vwap=P.vwap[i],adx=P.adx[i],chop=P.chop[i],rsi=P.rsi[i],pctB=P.pctB[i],atrPct=P.atrPct[i];
            if(hma==null||hmaPrev==null||vwap==null) return null;
            const aboveVwap=c>vwap, hmaUp=hma>hmaPrev;
            let regime='neutral'; if(adx>=23&&chop<=55)regime='trend'; else if(chop>=60||adx<15)regime='range';
            let score=0; if(aboveVwap)score+=0.20; else score-=0.20; if(hmaUp)score+=0.20; else score-=0.25; if(adx>25)score+=0.20; else if(adx<20)score-=0.30;
            return {c,regime,score,rsi,pctB,adx,chop,atrPct:atrPct==null?1.5:atrPct}; }

          // ---------- demo ----------
          function genDemo(){ const need=Math.floor(CFG.days*1440/intMin); const coins={};
            for(let n=0;n<CFG.univ;n++){ const sym='DEMO'+n+'USDT', base='DEMO'+n; const h=[]; let px=10+Math.random()*100; const t0=Date.now()-need*intMin*60000;
              for(let i=0;i<need;i++){ px*=1+(Math.random()-0.499)*0.01; const c=px; h.push({t:t0+i*intMin*60000,h:c*1.003,l:c*0.997,c,v:1000+Math.random()*1000}); }
              coins[base]={sym,h}; } return coins; }

          async function loadReal(){
            const end=Date.now(), start=end-CFG.days*86400000;
            console.log(`Binance'ten veri cekiliyor... (son ${CFG.days} gun, ${CFG.interval} + 1h + 4h yon)`);
            const t=await getJSON(`${BN}/api/v3/ticker/24hr`);
            const rows=t.filter(x=>x.symbol.endsWith('USDT')&&!/(UP|DOWN|BULL|BEAR)USDT$/.test(x.symbol)&&!STABLES.includes(x.symbol.slice(0,-4)))
              .map(x=>({sym:x.symbol,base:x.symbol.slice(0,-4),vol:+x.quoteVolume})).sort((a,b)=>b.vol-a.vol).slice(0,CFG.univ);
            const coins={};
            for(const r of rows){ try{ const h=await klines(r.sym,CFG.interval,start,end); if(h.length>320){ coins[r.base]={sym:r.sym,h};
                  if(CFG.htf){ coins[r.base].h1=await klines(r.sym,'1h',start,end); coins[r.base].h4=await klines(r.sym,'4h',start,end); } process.stdout.write('.'); } }catch(e){} }
            console.log(''); return coins;
          }

          (async function(){
            let coins; try{ coins = DEMO? genDemo() : await loadReal(); }
            catch(e){ console.error('VERI CEKILEMEDI:', e.message, '\nBu ortamda Binance engelli olabilir. Internet olan bir makinede calistir.'); process.exit(1); }
            const bases=Object.keys(coins); if(!bases.length){ console.error('Coin verisi yok.'); process.exit(1); }
            let refLen=0; for(const b of bases) refLen=Math.max(refLen, coins[b].h.length);
            const use=bases.filter(b=>Math.abs(coins[b].h.length-refLen)<=2);
            const L=Math.min(...use.map(b=>coins[b].h.length));
            const P={};
            for(const b of use){ const h=coins[b].h.slice(-L); coins[b].h=h; P[b]=prep(h);
              if(CFG.htf&&coins[b].h1){ const c1=coins[b].h1.map(x=>x.c); coins[b].ema1=emaS(c1,50); coins[b].c1=c1; coins[b].t1=coins[b].h1.map(x=>x.t); coins[b].p1=0; }
              if(CFG.htf&&coins[b].h4){ const c4=coins[b].h4.map(x=>x.c); coins[b].ema4=emaS(c4,50); coins[b].c4=c4; coins[b].t4=coins[b].h4.map(x=>x.t); coins[b].p4=0; } }
            const times=coins[use[0]].h.map(x=>x.t);
            const perDay=Math.floor(1440/intMin);

            let cash=CFG.start, peak=CFG.start, dayStart=CFG.start, curDay=new Date(times[0]).toISOString().slice(0,10);
            const pos={}, cooldown={}, closed=[]; const eqCurve=[]; let killedToday=false;
            const fee=CFG.fee, slip=CFG.slip;
            function equity(i){ let e=cash; for(const b in pos){ e+=pos[b].qty*P[b].c[i]; } return e; }
            // server legUp: HTF mumun KENDI kapanisi > kendi EMA50 (ikisi de). veri yetersizse null -> alma
            function legUpHTF(b,t,which){ const tk=which==='1'?coins[b].t1:coins[b].t4, ck=which==='1'?coins[b].c1:coins[b].c4, ek=which==='1'?coins[b].ema1:coins[b].ema4;
              if(!tk||!ek) return null; let p=which==='1'?coins[b].p1:coins[b].p4; while(p+1<tk.length&&tk[p+1]<=t)p++; if(which==='1')coins[b].p1=p; else coins[b].p4=p;
              const e=ek[p]; if(e==null||p<54) return null; return ck[p]>e; }
            function htfUp(b,t){ if(!CFG.htf) return true; const a=legUpHTF(b,t,'1'), c=legUpHTF(b,t,'4'); if(a===null||c===null) return false; return a&&c; }
            function chg24(b,i){ if(i<perDay)return 0; return (P[b].c[i]/P[b].c[i-perDay]-1)*100; }

            const warm=Math.max(300,perDay+5);
            for(let i=warm;i<L;i++){
              const t=times[i], day=new Date(t).toISOString().slice(0,10);
              if(day!==curDay){ curDay=day; dayStart=equity(i); killedToday=false; }
              const eqNow=equity(i); if(eqNow>peak)peak=eqNow;
              if(!killedToday && eqNow<=dayStart*(1-CFG.dailyLossStop)){ killedToday=true; for(const b of Object.keys(pos)) sell(b,i,'kill-switch',1); }
              // CIKISLAR (server.js ile ayni)
              for(const b of Object.keys(pos)){ const p=pos[b], px=P[b].c[i]; if(px>p.high)p.high=px;
                const s=sigAt(P[b],i)||{}; const ap=p.atrPct||1.5;
                let sd,tp,actT,give,beT;
                if(CFG.atrExits){ sd=clampN(CFG.atrStopK*ap,1.2,4.5); tp=Math.max(1.2,CFG.atrTpK*ap); actT=Math.max(1.8,1.8*ap); give=Math.max(1.2,1.6*ap); beT=Math.max(1.2,1.4*ap); }
                else { sd=3; tp=1.5; actT=2.5; give=0.8; beT=1.2; }
                const netPct=((px*p.qty-p.cost-p.cost*fee*2)/p.cost)*100, peakPct=((p.high*p.qty-p.cost-p.cost*fee*2)/p.cost)*100;
                const ref=P[b].c[i-1], fastRet=(ref&&ref>0)?(px/ref-1)*100:0;
                const fDrop=Math.max(1.5,CFG.flashDropK*ap), fSpike=Math.max(2.0,CFG.flashSpikeK*ap);
                if(fastRet<=-fDrop){ sell(b,i,'ani dusus',1); continue; }
                if(fastRet>=fSpike && netPct>0 && !p.tp1done){ p.tp1done=true; sell(b,i,'ani yukselis-kismi',CFG.tp1Frac); continue; }
                if(!p.tp1done && netPct>=tp){ p.tp1done=true; sell(b,i,'kismi',CFG.tp1Frac); continue; }
                let why=null;
                if(p.mode==='range'){ if(s.pctB!=null&&s.pctB>=0.5)why='mean'; else if(s.rsi!=null&&s.rsi>=65)why='rsi'; else if(netPct<=-sd)why='stop'; }
                else { if(peakPct>=actT&&netPct<=peakPct-give)why='trail'; else if(peakPct>=beT&&netPct<=0.1)why='basabas'; else if(netPct<=-sd)why='stop'; }
                if(why) sell(b,i,why,1);
              }
              // GIRISLER (server.js ile ayni: filtre + per-slot boyut + maxNewPerTick)
              if(!killedToday){ let open=Object.keys(pos).length, avail=cash; const cands=[];
                for(const b of use){ if(pos[b]||(cooldown[b]&&t<=cooldown[b]))continue; const s=sigAt(P[b],i); if(!s)continue;
                  if(CFG.htf){ const okData=(coins[b].ema1&&coins[b].ema4); if(!okData)continue; }
                  if((s.atrPct||0)>CFG.maxAtrPct)continue;
                  const hu=htfUp(b,t), ob=chg24(b,i)>=CFG.pumpMax;
                  const ap=s.atrPct||1.5, ref=P[b].c[i-1], fr=(ref&&ref>0)?(P[b].c[i]/ref-1)*100:0;
                  if(fr>=Math.max(2.0,CFG.spikeEntryK*ap))continue;
                  const trendUp=s.regime==='trend'&&s.score>=CFG.entryScore&&hu&&!ob;
                  const meanRev=s.regime==='range'&&s.rsi<40&&s.pctB<0.20&&hu&&!ob;
                  if(trendUp)cands.push({b,mode:'trend',rank:s.score,s}); else if(CFG.tradeRange && meanRev)cands.push({b,mode:'range',rank:0.5+(40-s.rsi)/70,s}); }
                cands.sort((x,y)=>y.rank-x.rank);
                let placed=0;
                for(const c of cands){ if(open>=CFG.maxPos||placed>=CFG.maxNewPerTick)break; const s=c.s, ap=s.atrPct||1.5;
                  const volF=clampN(1.5/ap,0.5,1.5);
                  const convF=(c.mode==='trend')?clampN(0.7+(s.score-CFG.entryScore)*1.8,0.7,1.5):clampN(0.7+(40-(s.rsi||35))/50,0.7,1.4);
                  const perSlot=CFG.investTarget/CFG.maxPos, qf=clampN(volF*convF,0.8,1.4);
                  let alloc=equity(i)*perSlot*qf; alloc=Math.min(alloc,equity(i)*CFG.maxFrac,avail*0.95);
                  if(CFG.maxTrade>0)alloc=Math.min(alloc,CFG.maxTrade);
                  if(alloc<CFG.minNotional){ if(avail>=CFG.minNotional)alloc=CFG.minNotional; else continue; }
                  alloc=Math.round(alloc*100)/100; buy(c.b,i,alloc,c.mode,ap); open++; placed++; avail-=alloc; } }
              if(i%Math.max(1,Math.floor(perDay/4))===0) eqCurve.push(equity(i));
            }
            for(const b of Object.keys(pos)) sell(b,L-1,'son',1);

            function buy(b,i,cost,mode,ap){ const px=P[b].c[i]*(1+slip); if(cost>cash)cost=cash; if(cost<1)return; const f=cost*fee, qty=(cost-f)/px; cash-=cost; pos[b]={qty,cost,entry:px,high:px,tp1done:false,mode,atrPct:ap}; }
            function sell(b,i,why,frac){ const p=pos[b]; if(!p)return; frac=Math.min(1,frac||1); const px=P[b].c[i]*(1-slip); const sq=p.qty*frac,cp=p.cost*frac; const gross=sq*px, net=gross-gross*fee; cash+=net; const pnl=net-cp; closed.push({b,pnl,pct:cp?pnl/cp*100:0,why,mode:p.mode,i});
              if(frac<0.999){ p.qty-=sq; p.cost-=cp; p.tp1done=true; } else { delete pos[b]; cooldown[b]=times[i]+CFG.cooldownMin*60000; } }

            // ----- rapor -----
            const endEq=cash; const ret=(endEq-CFG.start)/CFG.start*100;
            let wins=0,losses=0,sw=0,sl=0; const byCoin={};
            for(const c of closed){ if(c.pnl>=0){wins++;sw+=c.pnl;} else {losses++;sl+=Math.abs(c.pnl);} byCoin[c.b]=(byCoin[c.b]||0)+c.pnl; }
            let mdd=0,pk=eqCurve[0]||CFG.start; for(const e of eqCurve){ if(e>pk)pk=e; const dd=(pk-e)/pk*100; if(dd>mdd)mdd=dd; }
            const btc = coins.BTC? (coins.BTC.h[L-1].c/coins.BTC.h[warm].c-1)*100 : null;
            const totalPnl=endEq-CFG.start;
            const coinArr=Object.entries(byCoin).sort((a,b)=>b[1]-a[1]);
            const best=coinArr[0], worst=coinArr[coinArr.length-1];
            const exBest = best? totalPnl-best[1] : totalPnl;
            const fmt=n=>n.toFixed(2);
            const days=(times[L-1]-times[warm])/86400000;
            console.log('\n================ BACKTEST SONUCU ================');
            if(DEMO) console.log('*** DEMO (SAHTE VERI) — sonuc ANLAMSIZ, sadece format ***');
            console.log(`Donem            : son ${CFG.days} gun (gercek ~${days.toFixed(0)}g) · ${CFG.interval} mum · ${use.length} coin`);
            console.log(`Baslangic        : $${fmt(CFG.start)}`);
            console.log(`Bitis            : $${fmt(endEq)}`);
            console.log(`KAR/ZARAR        : ${ret>=0?'+':''}${fmt(ret)}%  (${ret>=0?'+':''}$${fmt(totalPnl)})`);
            console.log(`Gunluk ortalama  : ${days>0?(ret/days>=0?'+':'')+fmt(ret/days)+'%/gun':'-'}`);
            console.log(`Islem sayisi     : ${closed.length}  (~${days>0?(closed.length/days).toFixed(1):'-'}/gun)`);
            console.log(`Kazanma orani    : ${closed.length?fmt(wins/closed.length*100):'0'}%  (${wins}W / ${losses}L)`);
            console.log(`Profit factor    : ${sl>0?fmt(sw/sl):'-'}  (1 alti = kaybeden)`);
            const aw=wins?sw/wins:0, alo=losses?sl/losses:0;
            console.log(`Ort kazanc/kayip : +${fmt(aw)} / -${fmt(alo)}  payoff ${alo>0?fmt(aw/alo):'-'}`);
            console.log(`Max dusus (DD)   : -${fmt(mdd)}%`);
            if(btc!=null) console.log(`BTC al-tut       : ${btc>=0?'+':''}${fmt(btc)}%  (ayni donem, kiyas)`);
            console.log('-------------------------------------------------');
            if(best){ console.log(`En cok kazandiran: ${best[0]} ${best[1]>=0?'+':''}$${fmt(best[1])}`);
                      console.log(`En cok kaybettiren: ${worst[0]} ${worst[1]>=0?'+':''}$${fmt(worst[1])}`);
                      console.log(`>> En iyi coin HARIC net: ${exBest>=0?'+':''}$${fmt(exBest)}  (${(exBest/CFG.start*100).toFixed(2)}%)`);
                      console.log(`   (Eger bu deger eksiyse, kar tek bir sansli coine bagli demektir — guvenilmez.)`); }
            console.log('-------------------------------------------------');
            // TUTARLILIK: donemi ikiye bol, her yariyi ayri olc (curve-fit yakalamak icin)
            const mid=Math.floor((warm+(L-1))/2);
            function half(lo,hi){ let w=0,l=0,sw=0,sl=0; for(const c of closed){ if(c.i>=lo&&c.i<hi){ if(c.pnl>=0){w++;sw+=c.pnl;}else{l++;sl+=Math.abs(c.pnl);} } } const pf=sl>0?sw/sl:(sw>0?99:0); return {n:w+l,net:sw-sl,pf}; }
            const h1=half(warm,mid), h2=half(mid,L);
            const btc1=coins.BTC?(coins.BTC.h[mid].c/coins.BTC.h[warm].c-1)*100:null;
            const btc2=coins.BTC?(coins.BTC.h[L-1].c/coins.BTC.h[mid].c-1)*100:null;
            const r1=h1.net/CFG.start*100, r2=h2.net/CFG.start*100;
            const pff=p=>p>=99?'∞':fmt(p);
            console.log('TUTARLILIK (donem ikiye bolundu):');
            console.log(`  Ilk yari    : bot ${r1>=0?'+':''}${fmt(r1)}%  PF ${pff(h1.pf)}  (${h1.n} islem)  vs BTC ${btc1!=null?(btc1>=0?'+':'')+fmt(btc1)+'%':'-'}`);
            console.log(`  Ikinci yari : bot ${r2>=0?'+':''}${fmt(r2)}%  PF ${pff(h2.pf)}  (${h2.n} islem)  vs BTC ${btc2!=null?(btc2>=0?'+':'')+fmt(btc2)+'%':'-'}`);
            const pass = h1.pf>1 && h2.pf>1 && btc1!=null && r1>btc1 && btc2!=null && r2>btc2;
            console.log(`  GECTI Mi?   : ${pass?'EVET — iki yarida da PF>1 ve BTC ustu':'HAYIR — kural saglanmadi'}`);
            console.log('  KURAL: iki yarida da (PF>1 VE bot getirisi > BTC) ise devam; degilse strateji emekli.');
            console.log('=================================================');
            console.log('Not: gecmis performans gelecegi GARANTI ETMEZ. Komisyon+slipaj dahil. Bu yatirim tavsiyesi degildir.');
          })();
          JSEOF
          node bt_run.js
