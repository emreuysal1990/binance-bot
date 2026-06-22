# Survive & Grow — Binance Hesap Yöneticisi

Binance hesabını yöneten, **7/24 sunucuda çalışan**, hem otomatik hem **manuel müdahaleli** (al / sat / hepsini kapat / panik / duraklat) bir işlem botu + kontrol paneli.
Amaç: ayakta kalmak ve parayı katlamaya çalışmak. Strateji: trend filtresi + RSI/MACD/StochRSI/Bollinger skoru, ATR'ye göre **trailing stop**, sermaye dilimleme.

> ⚠️ **Bu yatırım tavsiyesi değildir. Kayıplar gerçektir.** Çoğu retail bot komisyon/spread yüzünden para kaybeder. Sıra: **paper → testnet → çok küçük live.** Karar ve sorumluluk sana aittir. Binance API ticaretinin yerel mevzuata ve Binance şartlarına uygunluğundan sen sorumlusun.

## 0) Güvenlik — önce bunu oku
- API anahtarı **yalnızca sunucuda** (`.env`) durur, panele/tarayıcıya asla girmez.
- Binance'te anahtar oluştururken **sadece "Enable Spot & Margin Trading"** aç. **"Enable Withdrawals" ASLA açma** → anahtar çalınsa bile paran çekilemez.
- **IP kısıtlaması** koy: sunucunun çıkış IP'sini gir. (Render/Railway'de statik IP yoksa kendi VPS'ini veya evdeki Raspberry Pi'yi tercih et.)
- `DASH_TOKEN`'ı uzun ve rastgele yap; paneli o korur.

## 1) Kurulum (lokal deneme — paper mod, anahtar gerekmez)
```bash
cd binance-bot
cp .env.example .env        # DASH_TOKEN'i degistir, MODE=paper kalsin
npm install
npm start
```
Tarayıcıda `http://localhost:8080` → DASH_TOKEN ile gir. Gerçek fiyatlarla **sahte** işlem başlar; bütün davranışı izle, manuel butonları dene.

## 2) Testnet (sahte para, gerçek Binance API)
1. https://testnet.binance.vision adresinden test API anahtarı al.
2. `.env`: `MODE=testnet`, `BINANCE_KEY=...`, `BINANCE_SECRET=...`
3. `npm start`. Artık gerçek emir akışını (sahte bakiyeyle) test edersin.

## 3) Live (GERÇEK PARA — küçük başla)
1. Binance > API Management > Create API. Sadece Spot Trading, Withdrawals KAPALI, IP kısıtlı.
2. `.env`: `MODE=live`, anahtarları gir. `START_CASH` önemsiz (gerçek bakiye okunur).
3. `MAX_TRADE_USDT`, `MAX_POSITIONS`, `DAILY_LOSS_STOP` ile sınırlarını ayarla. **Az parayla başla.**
4. `npm start`. Panelde mod **LIVE** (kırmızı) görünür.

## 4) 7/24 yayın (telefonu kapatınca da çalışsın)
Aşağıdakilerden biri yeterli (sunucu sürekli açık kalır, telefon sadece panele bağlanır):
- **Kendi VPS** (Hetzner/DigitalOcean ~5$/ay): `git clone` → `npm i` → `pm2 start server.js` → `pm2 save`. Statik IP olur, Binance IP kısıtı koyabilirsin (en güvenlisi).
- **Evdeki Raspberry Pi / eski bilgisayar**: aynı şekilde `pm2` ile. Ev IP'sini Binance'e tanımla.
- **Railway / Render**: repoyu bağla, env değişkenlerini gir. (Not: bunların IP'si değişken olabilir; Binance IP kısıtı koymak zorlaşır.)

Panele telefondan erişmek için sunucunun adresini Safari'de aç (VPS'te `http://IP:8080` veya bir alan adı + HTTPS).

## Manuel müdahale
Panelden her an: **Duraklat/Devam**, bir coini **Sat**, **Hepsini Kapat**, **PANİK** (hepsini kapat + durdur), elle **AL** (coin sembolü gir). Bot otomatik çalışırken sen de istediğin an araya girebilirsin.

## Güvenlik otomatiği
- **Kill-switch:** gün içi kayıp `DAILY_LOSS_STOP`'u (varsayılan %15) aşarsa tüm pozisyonlar kapatılır ve bot durur.
- **MAX_TRADE_USDT:** tek işlemde harcanabilecek mutlak tavan.
- Durum `state.json`'a yazılır; sunucu yeniden başlasa da kaldığı yerden devam eder.

## Ayarlar (.env)
`MODE, QUOTE, START_CASH, UNIVERSE (≤60), MAX_POSITIONS, RISK_MODE (safe/normal/degen), POLL_MS, MIN_NOTIONAL, DAILY_LOSS_STOP, MAX_TRADE_USDT, DASH_TOKEN, BINANCE_KEY, BINANCE_SECRET`

## "Bot gibi değil, insan gibi" hakkında dürüst not
Tamamen insan gibi sezgisel karar veren bir sistem otomatikleştirilemez; bu, kurallarla çalışır. Ama daha az ama nitelikli işlem (yüksek eşik), trende uyma, kazananı bırakıp trailing stop ile koruma, konviksiyona göre boyutlandırma ve belirsizde nakitte durma ile **mekanik bir bottan çok daha temkinli/iradeli** davranır. Yine de kâr garantisi yoktur.
