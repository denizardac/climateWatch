# ClimateWatch: Büyük Veri ile Küresel İklim Haberleri ve Çevresel Verilerin Analizi

## Proje Amacı
Küresel iklim değişikliğiyle ilgili haber ve çevresel verileri büyük veri teknolojileriyle analiz etmek, medya ilgisi ile bilimsel gerçekler arasındaki ilişkiyi ortaya koymak.

## Proje Durumu (Mayıs 2024)
- Farklı kaynaklardan (GDELT, Google Trends, NOAA, climate-laws.org, COP zirveleri, afetler) veri çekme scriptleri hazır.
- Büyük veri depolama ve işleme için Docker ile MongoDB, HDFS, Spark, Kafka altyapısı kurulabiliyor.
- Ham veri dosyaları repoya eklenmiyor; veri indirme scriptleriyle gerçek veriler indiriliyor.
- Veri işleme, analiz ve modelleme için temel notebook altyapısı mevcut, ancak analiz pipeline'ı ve görselleştirme adımları geliştirilmeye açıktır.

## Son Yapılanlar

- Tüm veri temizleme scriptleri (`clean_climate.py`, `clean_disasters.py`, `clean_policies.py`, `clean_gdelt.py`, `clean_trends.py`) PySpark ile yeniden yazıldı.
- Tüm veri işleme adımları artık Spark DataFrame üzerinde, büyük veri ölçeğinde çalışacak şekilde optimize edildi.
- Çıktılar CSV yerine Parquet formatında kaydediliyor (büyük veri için daha verimli).
- Kodlar, Docker ortamında veya yerel Spark ile çalışacak şekilde güncellendi.
- Her scriptte logging ve hata yönetimi geliştirildi.
- Veri kalitesi ve temel analiz metrikleri eklendi.

## Big Data Projesine Tam Dönüşüm İçin Yapılması Gerekenler

1. **HDFS Entegrasyonu**
   - Tüm veri okuma/yazma işlemlerini HDFS üzerinde yapacak şekilde güncelle.
   - Örnek:  
     ```python
     df = spark.read.csv("hdfs:///data/raw/climate/*.csv")
     df.write.parquet("hdfs:///data/processed/climate")
     ```

2. **Kafka ile Gerçek Zamanlı Veri Akışı**
   - Gerekli ise, veri ingestion adımlarında Kafka topic'lerinden veri oku/yaz.
   - Örnek:
     ```python
     df = spark.readStream.format("kafka")...
     ```

3. **Airflow ile Otomasyon**
   - Tüm veri pipeline adımlarını Airflow DAG'leri ile otomatikleştir.
   - Her veri kaynağı için bir DAG oluştur.

4. **Monitoring ve Logging**
   - Prometheus ve Grafana ile Spark, Kafka, HDFS ve pipeline metriklerini izle.
   - Logları merkezi bir yerde topla.

5. **Veri Kalitesi ve Test**
   - Her veri seti için null, tutarlılık ve anomali kontrolleri ekle.
   - Otomatik test scriptleri yaz.

6. **Hive/Spark SQL ile SQL Analizleri**
   - Temizlenmiş verileri Hive tablolarına kaydet.
   - SQL ile analiz ve raporlama yap.

7. **Pipeline ve Kod Standartları**
   - Tüm scriptlerde modülerlik, hata yönetimi ve logging standartlarını uygula.
   - Kodun her adımında açıklayıcı docstring ve yorumlar ekle.

8. **Docker Compose ile Tüm Servisleri Yönet**
   - Hadoop, Spark, Kafka, Airflow, Prometheus, Grafana gibi servisleri tek komutla başlatacak bir `docker-compose.yml` hazırla.

### Yapılanlar
- [x] Spark ile veri temizleme scriptleri
- [x] Parquet formatına geçiş
- [x] Logging ve veri kalitesi metrikleri
- [x] Docker ortamı ile test

### Yapılacaklar
- [ ] HDFS entegrasyonu
- [ ] Kafka ile stream processing
- [ ] Airflow DAG'leri ile otomasyon
- [ ] Prometheus & Grafana ile monitoring
- [ ] Hive/Spark SQL ile analiz
- [ ] Otomatik test ve veri kalitesi kontrolleri

## Gereksinimler
- Docker ve Docker Compose
- Python 3.8+
- Gerekli Python paketleri (örn. pandas, requests, pytrends, pymongo, pyspark, kafka-python, pdfplumber, tesseract vb.)
- (Opsiyonel) Tesseract OCR (PDF extraction için)

### Python Paketlerini Kurmak için:
```bash
pip install -r requirements.txt
```
> Not: requirements.txt dosyasını kendiniz oluşturmalısınız. Her scriptin başında gerekli paketler belirtilmiştir.

## Klasör Yapısı ve Veri Yönetimi
- **data_storage/**: Ham ve işlenmiş veri dosyaları (CSV, JSON, Parquet). Bu klasör .gitignore ile repoya eklenmez.
- **data_ingestion/**: Tüm veri indirme ve ön işleme scriptleri burada.
- **logs/**: Script logları burada tutulur.
- **notebooks/**: Analiz ve modelleme için Jupyter notebookları.
- **docker-compose.yml**: Tüm büyük veri servislerini başlatmak için.

## Çalıştırma Sırası

### 1. Docker Servislerini Başlat
```bash
docker-compose up -d
```
- MongoDB, HDFS, Spark, Jupyter, Kafka gibi servisler başlatılır.

### 2. Gerekli Klasörleri Oluştur
```bash
mkdir -p data_storage/hdfs/namenode data_storage/hdfs/datanode data_storage/mongodb data_storage/spark data_storage/climate data_storage/gdelt notebooks logs
```

### 3. Gerçek Verileri İndir
Aşağıdaki scriptleri çalıştırarak ilgili veri kaynaklarından gerçek verileri indirin:

#### a) İklim Verisi
```bash
python data_ingestion/fetch_climate_data.py --target_dir data_storage/climate --log_path logs/data_ingestion.log
```

#### b) GDELT Haber Verisi
```bash
python data_ingestion/fetch_gdelt_news.py --start_date 20240501 --end_date 20240507 --keywords CLIMATE ENVIRONMENT WEATHER --target_dir data_storage/gdelt --log_path logs/data_ingestion.log
```

#### c) Doğal Afet, Zirve, Politika Değişikliği, Google Trends
```bash
python data_ingestion/fetch_disaster_events.py --start_date 20230101 --end_date 20231231 --target_dir data_storage/disasters
python data_ingestion/fetch_climate_summits.py --target_dir data_storage/summits
python data_ingestion/fetch_policy_changes.py --target_dir data_storage/policies
python data_ingestion/fetch_google_trends.py --keyword "climate change" --start_date 2023-01-01 --end_date 2023-01-31 --target_dir data_storage/trends
```
> Her scriptin başında gerekli parametreler ve opsiyonlar açıklanmıştır.

### 4. Jupyter Notebook ile Analiz
- [http://localhost:8888](http://localhost:8888) adresinden Jupyter'a erişin.
- Analiz ve modelleme için `notebooks/` klasöründe yeni notebooklar oluşturun.
- Spark ve MongoDB bağlantısı için örnek kodlar ve açıklamalar eklenmiştir.

### 5. Log ve Hata Yönetimi
- Tüm veri çekme işlemleri ve hatalar `logs/data_ingestion.log` dosyasına kaydedilir.

## Notlar ve İpuçları
- **Ham veri dosyaları repoya eklenmez.** Gerçek veriyi scriptlerle indirmeniz gerekir.
- **Büyük veriyle çalışırken** Spark ve HDFS kullanımı önerilir.
- **Veri işleme ve analiz pipeline'ı** için örnek notebooklar ve scriptler geliştirilmeye açıktır.
- **Ek veri kaynakları** veya yeni analizler eklemek için `data_ingestion/` altına yeni scriptler ekleyebilirsiniz.

## Geliştirici Notları
- Kodunuzu ve veri pipeline'ınızı periyodik çalıştırmak için cron veya benzeri zamanlayıcılar kullanabilirsiniz.
- Her scriptin başında gerekli Python paketleri ve kullanım örnekleri açıklanmıştır.
- Proje büyüdükçe, analiz ve modelleme adımlarını modüler hale getirmek için notebook ve script yapısını sade ve açıklayıcı tutun.

## Lisans ve Atıf
- Proje açık kaynak olarak paylaşılacaktır.
- UNFCCC katılımcı verisi için [bagozzib/UNFCCC-Attendance-Data](https://github.com/bagozzib/UNFCCC-Attendance-Data) projesinden alınan kodlar CC-BY-4.0 lisansı ile kullanılmıştır.

---

**Herhangi bir sorunda veya katkı yapmak için lütfen issue açın veya pull request gönderin!**

## Mimari Diyagram (Text Tablo)

```
+-------------------+      +----------------+      +-------------------+
|  Veri Kaynakları  | ---> |  Veri Toplama  | ---> |  Veri Depolama    |
| (GDELT, NOAA,     |      |  (Python       |      |  (HDFS, MongoDB,  |
|  NewsAPI,         |      |  Scriptleri,   |      |   HBase, Hive)    |
|  Google Trends,   |      |  Kafka)        |      +-------------------+
|  Twitter,         |      +----------------+               |
|  Afet/Etkinlik)   |                                   +---+---+
+-------------------+                                   |       |
                                                        v       v
                                                +-------------------+
                                                |  Veri İşleme      |
                                                |  (Spark, PySpark, |
                                                |   ETL, NLP)       |
                                                +-------------------+
                                                        |
                                                        v
                                                +-------------------+
                                                |  Analiz & ML      |
                                                |  (Spark MLlib,    |
                                                |   scikit-learn,   |
                                                |   EDA, Modelleme) |
                                                +-------------------+
                                                        |
                                                        v
                                                +-------------------+
                                                |  Görselleştirme   |
                                                |  (Jupyter,        |
                                                |   Streamlit,      |
                                                |   Dashboard)      |
                                                +-------------------+
```

## Sistem Bileşenleri
- **Jupyter Notebook**: Analiz ve modelleme ortamı (PySpark destekli)
- **MongoDB**: Yarı-yapılandırılmış veri (haber, iklim) için NoSQL veritabanı
- **Hadoop HDFS**: Büyük veri depolama
- **Spark**: Dağıtık veri işleme
- **Kafka (Opsiyonel)**: Gerçek zamanlı veri akışı ve streaming ingestion
- **Hive/Spark SQL (Opsiyonel)**: SQL tabanlı veri ambarı ve analiz
- **HBase (Opsiyonel)**: Dağıtık tablo veritabanı, büyük ölçekli zaman serisi verisi için

## Proje Akış Şeması
1. **Veri Toplama**: Haber, iklim, sosyal medya ve ek veri kaynaklarından veri çekilir.
2. **Veri Depolama**: Toplanan veriler HDFS, MongoDB, HBase gibi sistemlerde saklanır.
3. **Veri İşleme & Temizleme**: Spark ve Python ile veri temizliği, NLP, ETL işlemleri yapılır.
4. **Analiz & Modelleme**: EDA, istatistiksel analiz, NLP, zaman serisi ve makine öğrenimi modelleri uygulanır.
5. **Görselleştirme & Sunum**: Sonuçlar Jupyter, Streamlit veya dashboard ile sunulur.

## Kullanılan/Kullanılabilecek Veri Kaynakları
- **GDELT**: Küresel haber verisi
- **NOAA/NASA**: İklim ve çevresel göstergeler
- **NewsAPI**: Alternatif haber kaynağı
- **Google Trends**: Medya ilgisi
- **Twitter API**: Sosyal medya verisi
- **Afet/Etkinlik API'ları**: Doğal afet, zirve, politika değişikliği verisi

## Ek Veri Kaynakları ve Alternatif Medya Scriptleri
Aşağıdaki scriptler, ek veri kaynaklarından veri çekmek için iskelet olarak eklenmiştir:

- `fetch_disaster_events.py`: Doğal afet (yangın, sel, kasırga, deprem vb.) verisi çekmek için. Artık ReliefWeb API ile gerçek afet verisi çeker.
- `fetch_climate_summits.py`: Uluslararası iklim zirveleri ve önemli çevre etkinlikleri verisi çekmek için. Artık Wikipedia'dan COP zirveleri listesini çeker.
- `fetch_policy_changes.py`: Ülkelerin iklim politikası değişiklikleri, anlaşmalar ve yasalar için veri çekmek için. Artık climate-laws.org sitesinden CSV indirir.
- `fetch_google_trends.py`: Google Trends API veya pytrends ile arama ilgisi verisi çekmek için. Artık pytrends ile gerçek veri çeker.

> **Not:** Twitter API scripti kaldırıldı. Twitter API artık çoğu kullanım için paralı ve ücretsiz erişim çok kısıtlı olduğu için bu script projeden çıkarıldı.

Her script, ilgili API veya veri kaynağına bağlanarak gerçek veri çeker ve çıktıyı CSV/JSON olarak kaydeder. Gerçek veri çekimi için ek Python paketleri gerekebilir (ör. `requests`, `pandas`, `pytrends`).

### Örnek Kullanımlar

```bash
# Doğal afet verisi çekme (ReliefWeb API)
python data_ingestion/fetch_disaster_events.py --start_date 20230101 --end_date 20231231 --target_dir data_storage/disasters

# İklim zirvesi verisi çekme (Wikipedia)
python data_ingestion/fetch_climate_summits.py --target_dir data_storage/summits

# Politika değişikliği verisi çekme (climate-laws.org)
python data_ingestion/fetch_policy_changes.py --target_dir data_storage/policies

# Google Trends verisi çekme (pytrends)
python data_ingestion/fetch_google_trends.py --keyword "climate change" --start_date 2023-01-01 --end_date 2023-01-31 --target_dir data_storage/trends
```

## UNFCCC Katılımcı Verisi (COP Attendance) - Otomatik PDF'den Çekme

Bu projede, UNFCCC (COP) katılımcı verisini otomatik olarak PDF dosyalarından çıkarmak ve işlemek için [bagozzib/UNFCCC-Attendance-Data](https://github.com/bagozzib/UNFCCC-Attendance-Data) projesinin açık kaynak kod altyapısı entegre edilmiştir.

### Kullanım Adımları

1. **Gerekli Bağımlılıkları Kurun:**
   - Python için: `pip install -r requirements.txt` (pdfplumber, pytesseract, pillow, pdf2image vb.)
   - Tesseract OCR kurulumu gereklidir. (Windows için: https://github.com/tesseract-ocr/tesseract)

2. **PDF'den Veri Çıkarımı:**
   - `extract_pdf_data.py` scripti ile PDF dosyasının yapısına göre (tek/sütun, metin/görsel) uygun extraction class'ı seçin.
   - Çıkan veriyi `inputs_file.py` içinde `extracted_input_data` değişkenine kaydedin.

3. **Veri Sınıflandırma:**
   - `classify_data.py` scripti ile veriyi; Grup Tipi, Delegasyon, Unvan, İsim, Görev, Bölüm, Kurum gibi alanlara otomatik ayırın.

4. **Veri Temizleme ve Sonuç:**
   - R scriptleri (`FinalDataCleaning_COP.R` vb.) ile son temizlik ve CSV üretimi yapılabilir.

> Detaylı adımlar ve örnekler için: [UNFCCC Project Code Execution Steps](https://github.com/bagozzib/UNFCCC-Attendance-Data/wiki/UNFCCC-Project-Code--Execution-Steps)

### Kredilendirme
Bu sistemin kod altyapısı [bagozzib/UNFCCC-Attendance-Data](https://github.com/bagozzib/UNFCCC-Attendance-Data) projesinden alınmış ve CC-BY-4.0 lisansı ile kullanılmıştır. Orijinal geliştiricilere atıf yapılmıştır. 