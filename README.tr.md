# STRATUM — Veri Güvenilirliği Platformu

Cenker Sule tarafından geliştirilen, ham veriden doğrulanmış analiz tablolarına uzanan yerel veri mühendisliği projesi.

[English](README.md) · [Deutsch](README.de.md)

## VS Code ve Docker ile aç

ZIP'i açıp `compose.yaml` bulunan **stratum** klasörünü VS Code'da aç. Docker Desktop açıkken terminalde:

```bash
docker compose up --build
```

**http://localhost:8080** adresine gir. İlk kurulum bağımlılıkları indirir; uygulama ilk açılışta paketli gerçek 50 bin kaydı otomatik işler. API anahtarı gerekmez. AFTERLIGHT 8080'i kullanıyorsa onu durdur veya Compose içindeki ana makine portunu 8081 yap.

Veriler `stratum-data` Docker volume içinde saklanır. `docker compose down` bunları silmez; `down -v` siler. Tam ay denemeleri için Docker'a en az 3 GB bellek ve birkaç GB boş disk ayır. Kaynak kodun dışında Python/Node kurmak zorunda değilsin.

## Çalışan özellikler

- Resmî NYC TLC verisinden 50.000 kayıtlık gerçek örnek; tam Ocak-Mart 2025 dosyalarını seçerek indirme.
- SHA-256 kaynak kontrolü, sürümlü doğrulama politikası ve kalıcı çalışma geçmişi.
- Eksik zorunlu sütun kontrolü, altı kayıt kuralı, kabul/karantina ayrımı ve neden incelemesi.
- Aynı dosya ve aynı politika tekrar çalıştırıldığında kayıt çoğaltmama.
- dbt ile SQL dönüşümleri, günlük/bölgesel analiz tabloları ve 10 veri testi.
- Dagster ile veri bölümü çalıştırma; başarısız kapılarda son sağlam veri kümesini koruma.
- Üç dilli panel, açık/koyu tema, veri gezgini, çalışma ayrıntıları, JSON ve dbt kayıtları.

## İnceleme sırası

1. İlk çalışma bitince 50.000 ham, 46.797 kabul ve 3.203 karantina kaydını gör.
2. Standart örneği tekrar çalıştır: **Değişiklik yok** durumunu doğrula.
3. **Eksik sütun denemesi** seç: ayrı kopyadan `total_amount` silinir, yayımlama engellenir.
4. **Geçersiz mesafe denemesi** seç: ayrı kopyada mesafeler -999 olur; kalite kapısı engeller.
5. Sağlam kaynağı tekrar seç: mevcut yayımla aynıysa işlem güvenli biçimde atlanır.
6. **Tam ay** seçeneği ile gerçek aylık dosyayı indir ve işle. Aynı ay için tam dosya örneğin yerine geçer; üstüne eklenmez. Diğer ayları tek tek geçmişe dönük işleyebilirsin.

## Veriyi doğru yorumla

Ocak kaynak dosyası 3.475.226 kayıt içerir. Paketli örnek her 69. kayıt seçilerek hazırlanmıştır; rastgele veya temsili örnek olduğu iddia edilmez. Kaynak, seçim ve özetler `data/seed/provenance.json` içindedir.

Karantina her zaman kaynak hatası demek değildir. Altı proje kuralı; süre, ay, mesafe, bölge, ücret ve ödeme kodunu denetler. Geçerli iadeler ve uzun yolculuklar da politikaya takılabilir. Kayıt parmak izleri resmî yolculuk kimliği değildir; aynı görünen meşru kayıtlar da mükerrer adayı sayılabilir. Ham kaynak korunur. Ücret toplamları denetlenmiş gelir değildir. Tarihler sağlayıcının yerel saatinde tutulur.

Dagster tek bir veri bölümü varlığını çalıştırır; alt aşamalar kontrol defterinde, SQL bağımlılıkları dbt çıktısındadır. Canlı veri akışı, CDC, dağıtık kuyruk, otomatik çok aylı zamanlayıcı ve üretim SLA taahhüdü yoktur. Kesilen işlem yeniden başlatmada işaretlenir; aşama ortasından otomatik devam etmez. Tam teknik açıklama ve sınırlamalar [English README](README.md) içindedir.

## Test ve lisans

Native testler ve TypeScript/Vite derlemesi için `README.md` komutlarını kullan. Docker burada mevcut olmadığından konteyner derlemesi yerel makinede/CI'da ayrıca doğrulanmalıdır. Doğrulama sonuçları `docs/VALIDATION.md` içinde.

Uygulama kodu MIT; üçüncü taraf NYC verisi MIT olarak yeniden lisanslanmaz. Atıf ve kullanım bilgisi `data/LICENSE.md` içinde. Proje yerel ve tek kullanıcılıdır; kimlik doğrulaması yoktur, internete açık sunma.
