import pandas as pd
import pycountry
import logging
from typing import Dict, List, Optional

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CountryCodeMapper:
    def __init__(self):
        # Ülke isimlerini ve kodlarını içeren sözlük
        self.country_dict = {country.name: country.alpha_2 for country in pycountry.countries}
        # Alternatif isimler için ek eşleştirmeler
        self.alternative_names = {
            'UNITED STATES': 'US',
            'UNITED KINGDOM': 'GB',
            'RUSSIA': 'RU',
            'CHINA': 'CN',
            'INDIA': 'IN',
            'BRAZIL': 'BR',
            'SOUTH AFRICA': 'ZA',
            'SOUTH KOREA': 'KR',
            'NORTH KOREA': 'KP',
            'VIETNAM': 'VN',
            'PHILIPPINES': 'PH',
            'SINGAPORE': 'SG',
            'MALAYSIA': 'MY',
            'INDONESIA': 'ID',
            'THAILAND': 'TH',
            'AUSTRALIA': 'AU',
            'NEW ZEALAND': 'NZ',
            'CANADA': 'CA',
            'MEXICO': 'MX',
            'ARGENTINA': 'AR',
            'CHILE': 'CL',
            'COLOMBIA': 'CO',
            'PERU': 'PE',
            'VENEZUELA': 'VE',
            'EGYPT': 'EG',
            'NIGERIA': 'NG',
            'KENYA': 'KE',
            'ETHIOPIA': 'ET',
            'TANZANIA': 'TZ',
            'UGANDA': 'UG',
            'GHANA': 'GH',
            'MOROCCO': 'MA',
            'ALGERIA': 'DZ',
            'TUNISIA': 'TN',
            'SAUDI ARABIA': 'SA',
            'UAE': 'AE',
            'QATAR': 'QA',
            'KUWAIT': 'KW',
            'BAHRAIN': 'BH',
            'OMAN': 'OM',
            'JORDAN': 'JO',
            'LEBANON': 'LB',
            'SYRIA': 'SY',
            'IRAQ': 'IQ',
            'IRAN': 'IR',
            'PAKISTAN': 'PK',
            'BANGLADESH': 'BD',
            'SRI LANKA': 'LK',
            'NEPAL': 'NP',
            'BHUTAN': 'BT',
            'MYANMAR': 'MM',
            'CAMBODIA': 'KH',
            'LAOS': 'LA',
            'MONGOLIA': 'MN',
            'TAIWAN': 'TW',
            'HONG KONG': 'HK',
            'MACAU': 'MO',
            'JAPAN': 'JP',
            'NEW DELHI': 'IN',  # Hindistan'ın başkenti
            'NAIROBI': 'KE',    # Kenya'nın başkenti
            'ROME': 'IT',       # İtalya'nın başkenti
            'VICTORIA': 'SC',   # Seyşeller'in başkenti
            'CAPE TOWN': 'ZA',  # Güney Afrika'nın şehri
            'ORLANDO': 'US',    # ABD'nin şehri
            'QUEENSLAND': 'AU', # Avustralya'nın eyaleti
            'SARAWAK': 'MY',    # Malezya'nın eyaleti
            'MOUNTAIN STATE': 'US', # ABD'nin eyaleti
            'COSTA RICA': 'CR', # Ülke
            'NICARAGUA': 'NI',  # Ülke
            'UZBEKISTAN': 'UZ', # Ülke
            'NIUE': 'NU',       # Ülke
            'COOK ISLANDS': 'CK', # Ülke
            'MONGOLIAN': 'MN',  # Ülke
            'AFRICA': None,     # Kıta
            'EUROPE': None,     # Kıta
            'ASIA': None,       # Kıta
            'AMERICA': None,    # Kıta
            'GOVERNMENT': None, # Genel terim
            'INDUSTRY': None,   # Genel terim
            'COMMUNITY': None,  # Genel terim
            'COMPANY': None,    # Genel terim
            'EMPLOYEE': None,   # Genel terim
            'PRESIDENT': None,  # Genel terim
            'ECONOMIST': None,  # Genel terim
            'JUDGE': None,      # Genel terim
            'POLICE': None,     # Genel terim
            'AIR FORCE': None,  # Genel terim
            'AIRLINE': None,    # Genel terim
            'BANK': None,       # Genel terim
            'BUSINESS': None,   # Genel terim
            'NEWSPAPER': None,  # Genel terim
            'STUDENT': None,    # Genel terim
            'SCHOOL': None,     # Genel terim
            'FARMER': None,     # Genel terim
            'ENTREPRENEUR': None, # Genel terim
            'POPULATION': None,  # Genel terim
            'MINIST': None,     # Kısaltma
            'JINKOSOLAR': None, # Şirket
            'AFRICAN DEVELOPMENT BANK': None, # Kurum
        }
        
    def get_country_code(self, country_name: str) -> Optional[str]:
        """
        Ülke isminden ülke kodunu döndürür
        
        Args:
            country_name (str): Ülke ismi
            
        Returns:
            Optional[str]: Ülke kodu veya None
        """
        if not country_name or pd.isna(country_name):
            return None
            
        # Önce alternatif isimlerden kontrol et
        if country_name in self.alternative_names:
            return self.alternative_names[country_name]
            
        # Sonra pycountry'den kontrol et
        if country_name in self.country_dict:
            return self.country_dict[country_name]
            
        # Eşleşme bulunamadı
        return None
        
    def process_gdelt_file(self, file_path: str) -> pd.DataFrame:
        """
        GDELT dosyasını işler ve ülke kodlarını ekler
        
        Args:
            file_path (str): GDELT dosyasının yolu
            
        Returns:
            pd.DataFrame: İşlenmiş veri
        """
        try:
            # Dosyayı oku
            df = pd.read_csv(file_path, low_memory=False)
            
            # Ülke kodlarını ekle
            df['Actor1CountryCode'] = df['Actor1Name'].apply(self.get_country_code)
            df['Actor2CountryCode'] = df['Actor2Name'].apply(self.get_country_code)
            
            # ActionGeo_CountryCode için Actor1CountryCode'u kullan
            df['ActionGeo_CountryCode'] = df['Actor1CountryCode']
            
            return df
            
        except Exception as e:
            logger.error(f"Dosya işlenirken hata oluştu: {str(e)}")
            return pd.DataFrame()

def main():
    # Mapper'ı başlat
    mapper = CountryCodeMapper()
    
    # Test için bir dosya işle
    test_file = "data_storage/gdelt/20250515.csv"
    df = mapper.process_gdelt_file(test_file)
    
    if not df.empty:
        # Sonuçları göster
        print("\nİşlenmiş veri örneği:")
        print(df[['Actor1Name', 'Actor1CountryCode', 'Actor2Name', 'Actor2CountryCode']].head())
        
        # İstatistikleri göster
        print("\nÜlke kodları istatistikleri:")
        print(f"Toplam kayıt sayısı: {len(df)}")
        print(f"Ülke kodu olan Actor1 sayısı: {df['Actor1CountryCode'].notna().sum()}")
        print(f"Ülke kodu olan Actor2 sayısı: {df['Actor2CountryCode'].notna().sum()}")
        
        # Eşleşmeyen ülke isimlerini göster
        unmatched = df[df['Actor1CountryCode'].isna()]['Actor1Name'].unique()
        print("\nEşleşmeyen ülke isimleri:")
        for name in unmatched:
            print(f"- {name}")

if __name__ == "__main__":
    main() 