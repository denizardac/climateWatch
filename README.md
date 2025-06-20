# ClimateWatch: Küresel İklim Haberleri ve Çevresel Veriler için Tam Otomatik Büyük Veri Analiz Platformu

## Proje Amacı
Küresel iklim haberleri ve çevresel verileri büyük veri teknolojileriyle analiz ederek, medya ilgisi ile gerçek iklim göstergeleri (sıcaklık, CO₂, afetler, politikalar, zirveler) arasındaki ilişkiyi ortaya koymak.

---

## Özellikler ve Pipeline Akışı

1. **Veri Toplama (Ingestion)**
   - Her kaynak için modüler ingestion scriptleri (`ingest_gdelt.py`, `ingest_climate.py`, ...).
   - `run_ingestion.py` ile tüm kaynaklardan veri otomatik ve izlenebilir şekilde toplanır.
   - Her adımda çıktı dosyası, satır sayısı, boyut ve hata loglanır.

2. **Veri Temizleme & İşleme (Processing)**
   - Her kaynak için modüler temizlik scriptleri (`clean_gdelt.py`, `clean_climate.py`, ...).
   - `run_processing.py` ile tüm temizlik adımları otomatik ve izlenebilir şekilde çalışır.

3. **Scraping + NLP**
   - Tüm işlenmiş CSV'lerden URL'ler çekilir, scraping ve sentiment analizi otomatik uygulanır.
   - Sonuçlar tek bir CSV'de toplanır, loglanır.

4. **Otomatik Raporlama & Görselleştirme**
   - `data_analysis/reporting.py` ile özet tablolar ve histogramlar otomatik üretilir.
   - Sonuçlar `analysis_results/` altında saklanır, loglanır.

5. **Tam Otomatik Pipeline**
   - `run_all_in_docker.sh` ile ingestion → processing → scraping/NLP → raporlama adımları tek komutla, modüler ve esnek şekilde çalışır.
   - Her adımda ayrıntılı loglama, çıktı dosyası ve satır/boyut kontrolü var.
   - Hata olursa pipeline durmaz, loglanır ve bir sonraki adıma geçilir.

---

## Hızlı Başlangıç

### 1. Ortamı Kurun

```bash
docker-compose up -d
```

### 2. Tüm Pipeline'ı Tek Komutla Çalıştırın

```bash
bash run_all_in_docker.sh
```

- Tüm veri toplama, işleme, scraping/NLP ve raporlama adımları otomatik başlar.
- Her adımda logs/ altında ayrıntılı loglar oluşur.
- Sonuçlar data_storage/processed/ ve analysis_results/ altında bulunur.

---

## Klasör Yapısı

- **data_ingestion/**: Her kaynak için ingestion scriptleri.
- **data_processing/**: Her kaynak için temizlik/işleme scriptleri, scraping ve NLP pipeline'ı.
- **data_analysis/**: Otomatik raporlama ve görselleştirme modülleri.
- **logs/**: Her adım için ayrıntılı log dosyaları.
- **data_storage/**: Ham ve işlenmiş veri dosyaları.
- **analysis_results/**: Otomatik üretilen özetler ve görseller.
- **run_all_in_docker.sh**: Tüm pipeline'ı başlatan ana script.
- **requirements.txt**: Tüm pip bağımlılıkları.
- **Dockerfile**: Ortamı otomatik kurar.

---

## Loglama ve İzlenebilirlik

- Her adımda (ingestion, processing, scraping/NLP, raporlama) başarı/başarısızlık, çıktı dosyası, satır sayısı ve boyut loglanır.
- Hatalar pipeline'ı durdurmaz, loglanır ve bir sonraki adıma geçilir.

---

## Geliştirici Notları

- Tüm parametreler ve yollar `config.yaml` ile merkezi olarak yönetilir.
- Yeni veri kaynağı eklemek için ilgili ingestion ve temizlik scriptini ekleyin.
- Otomatik raporlama fonksiyonları kolayca genişletilebilir.

---

## Bağımlılıklar

- Tüm pip paketleri `requirements.txt` ile otomatik kurulur.
- Docker ortamında ek kurulum gerekmez.

---

## Lisans ve Katkı

- Proje açık kaynak olarak paylaşılmıştır.
- Katkı ve sorularınız için issue açabilir veya pull request gönderebilirsiniz.

---

**ClimateWatch ile büyük veri tabanlı, tam otomatik ve izlenebilir iklim-medya analizi!**
