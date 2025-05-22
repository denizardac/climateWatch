# ClimateWatch: Büyük Veri ile Küresel İklim Haberleri ve Çevresel Verilerin Analizi

## Proje Amacı
Küresel iklim değişikliğiyle ilgili haber ve çevresel verileri büyük veri teknolojileriyle analiz etmek, medya ilgisi ile bilimsel gerçekler arasındaki ilişkiyi ortaya koymak.

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

- `fetch_disaster_events.py`: Doğal afet (yangın, sel, kasırga, deprem vb.) verisi çekmek için.
- `fetch_climate_summits.py`: Uluslararası iklim zirveleri ve önemli çevre etkinlikleri verisi çekmek için.
- `fetch_policy_changes.py`: Ülkelerin iklim politikası değişiklikleri, anlaşmalar ve yasalar için veri çekmek için.
- `fetch_twitter_data.py`: Twitter API ile iklim değişikliğiyle ilgili tweetleri çekmek için.
- `fetch_google_trends.py`: Google Trends API veya pytrends ile arama ilgisi verisi çekmek için.

Her script, ilgili API veya veri kaynağına bağlanmak için temel iskelet ve örnek argümanlar içerir. Gerçek veri çekimi için API anahtarı ve endpoint entegrasyonu gereklidir.

### Örnek Kullanımlar

```bash
# Doğal afet verisi çekme (örnek)
python data_ingestion/fetch_disaster_events.py --api_url <API_URL> --start_date 20230101 --end_date 20231231 --target_dir data_storage/disasters

# İklim zirvesi verisi çekme (örnek)
python data_ingestion/fetch_climate_summits.py --api_url <API_URL> --target_dir data_storage/summits

# Politika değişikliği verisi çekme (örnek)
python data_ingestion/fetch_policy_changes.py --api_url <API_URL> --target_dir data_storage/policies

# Twitter verisi çekme (örnek)
python data_ingestion/fetch_twitter_data.py --api_key <API_KEY> --query "climate change" --start_date 20230101 --end_date 20230131 --target_dir data_storage/twitter

# Google Trends verisi çekme (örnek)
python data_ingestion/fetch_google_trends.py --keyword "climate change" --start_date 20230101 --end_date 20230131 --target_dir data_storage/trends
```

## Kurulum ve Çalıştırma

### 1. Gerekli Klasörleri Oluştur
```bash
mkdir -p data_storage/hdfs/namenode data_storage/hdfs/datanode data_storage/mongodb data_storage/spark data_storage/climate data_storage/gdelt notebooks logs
```

### 2. Docker Servislerini Başlat
```bash
docker-compose up -d
```
- **Tüm servisler (Jupyter, MongoDB, HDFS, Spark) otomatik başlar.**
- Servislerin durumunu kontrol etmek için:
  ```bash
  docker ps
  ```

### 3. Jupyter Notebook'a Erişim
- Tarayıcıda [http://localhost:8888](http://localhost:8888) adresine git.
- Gerekirse token'ı görmek için:
  ```bash
  docker logs climatewatch-jupyter-1
  ```
- Jupyter'ı veri toplama script'lerinden önce veya sonra açabilirsin.

### 4. Otomatik Veri Toplama Script'leri
#### a) İklim Verisi
```bash
python data_ingestion/fetch_climate_data.py --target_dir data_storage/climate --log_path logs/data_ingestion.log
```
- `--no_mongo` parametresi ile MongoDB kaydını kapatabilirsin.

#### b) GDELT Haber Verisi (Tarih aralığı ve anahtar kelime ile)
```bash
python data_ingestion/fetch_gdelt_news.py --start_date 20240501 --end_date 20240507 --keywords CLIMATE ENVIRONMENT WEATHER --target_dir data_storage/gdelt --log_path logs/data_ingestion.log
```
- `--no_mongo` parametresi ile MongoDB kaydını kapatabilirsin.
- `--keywords` ile istediğin anahtar kelimeleri belirtebilirsin.
- Başlangıç ve bitiş tarihi verilmezse son 7 gün otomatik indirilir.

#### Log ve Hata Yönetimi
- Tüm işlemler ve hatalar `logs/data_ingestion.log` dosyasına kaydedilir.
- Hatalar terminalde de gösterilir.

### 5. Analiz ve Modelleme
- Jupyter Notebook'ta Spark ve MongoDB ile analiz yapabilirsin.
- Örnek notebooklar `notebooks/` klasöründe bulunur.

## Sık Sorulanlar
- **Jupyter'ı ne zaman açmalıyım?**  
  Veri toplama script'lerinden önce veya sonra açabilirsin. Analiz için Jupyter'ı kullanacaksan, Docker servisleri açık olmalı.
- **Docker servisleri otomatik mi başlar?**  
  `docker-compose up -d` komutuyla hepsi başlar. Kapatmak için `docker-compose down`.
- **Veri script'leri tekrar çalıştırılırsa ne olur?**  
  Aynı veriyi tekrar eklememek için script'ler önce eski veriyi siler, sonra yenisini ekler.
- **Log dosyası nerede?**  
  Tüm işlemler ve hatalar `logs/data_ingestion.log` dosyasına kaydedilir.

## Geliştirici Notları
- Yeni veri kaynakları eklemek için `data_ingestion/` altına yeni scriptler ekleyin.
- Pipeline'ı periyodik çalıştırmak için cron veya task scheduler kullanılabilir.
- Tüm kodlar ve scriptler açık kaynak olarak paylaşılacaktır.

## Opsiyonel Büyük Veri Servisleri

- **Kafka**: Gerçek zamanlı veri akışı ve streaming ingestion için kullanılır. Haber, sosyal medya veya sensör verisi gibi sürekli akan verileri toplamak ve işlemek için idealdir.
- **Hive**: HDFS üzerinde SQL tabanlı veri ambarı sağlar. Büyük veri üzerinde SQL sorguları ile analiz yapmak için kullanılır.
- **HBase**: Dağıtık tablo veritabanı. Büyük ölçekli zaman serisi veya anahtar-değer tabanlı veri için uygundur.

### Docker ile Servisleri Başlatma

```bash
docker-compose up -d
```
Tüm servisler (Jupyter, MongoDB, HDFS, Spark, Kafka, Hive, HBase) otomatik başlar.

### Servislere Bağlantı ve Kullanım

- **Kafka**: Bağlantı noktası: `localhost:9092`
  - Örnek Python kütüphanesi: `kafka-python`
- **Hive**: Metastore: `localhost:9083`, Server: `localhost:10000`
  - Örnek Python kütüphanesi: `pyhive`
- **HBase**: Web arayüzü: [http://localhost:16010](http://localhost:16010)
  - Örnek Python kütüphanesi: `happybase`

Her servisin detaylı kullanımı için ilgili Python kütüphanelerinin dökümantasyonuna bakınız. 