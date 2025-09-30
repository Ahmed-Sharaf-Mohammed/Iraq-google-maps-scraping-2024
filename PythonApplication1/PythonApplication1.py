import re
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, LabelEncoder, StandardScaler # OrdinalEncoder for multiple columns, LabelEncoder for single column ex: "yes" with ordinal will has 1 value but with label will value in each column
import pandas as pd
import pyodbc
from math import radians, sin, cos, sqrt, atan2
import folium
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import ast




# Display all columns in the DataFrame
pd.set_option('display.max_columns', None)

# Display all rows in the DataFrame
pd.set_option('display.max_rows', None)


conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=(localdb)\MSSQLLocalDB;"
    "Database=pr1;"
)

cursor = conn.cursor()

df = pd.read_sql("SELECT * FROM [All]", conn)
gov_df = pd.read_table("governorates_centers.txt")


######################## 1. Dataset Overview ##################
print("\n\n\n=================Dataset Overview=================")
print(df.shape,"\n\n\n================info=================")
print(df.info(),"\n\n\n==================isnull.sum===============") 
print(df.isnull().sum(),"\n\n\n=================dublicate.sum================")
print(df['state'].value_counts().head(100))
print(df.duplicated().sum(),"\n\n\n==================head(5)===============")
print(df.describe())


# 2. drop unnecessary columns
df = df.drop(columns=['plus_code', 'order_links'])
df = df.dropna(subset=['name', 'latitude', 'longitude', 'business_status'])


# 3. Fill missing values
df['reviews'] = df['reviews'].fillna(0)
df['photos_count'] = df['photos_count'].fillna(0)
columns_to_fill_with_zero = ['owner_id', 'postal_code', 'reviews', 'rating', 'photos_count', 'reviews_per_score_1', 'reviews_per_score_2', 'reviews_per_score_3', 'reviews_per_score_4', 'reviews_per_score_5']
df[columns_to_fill_with_zero] = df[columns_to_fill_with_zero].fillna(0)
df['verified'] = df['verified'].fillna(False).astype(bool)
columns_to_fill_with_unknown = ['subtypes', 'type', 'city', 'state', 'full_address', 'site', 'phone', 'phone_1', 'linkedin', 'email_1']
df[columns_to_fill_with_unknown] = df[columns_to_fill_with_unknown].fillna("NOT_PROVIDE")
df['working_hours'] = df['working_hours'].fillna('{}')
print(df.isnull().sum())
#print(df['type'].unique())


# 4. Strip leading/trailing whitespace from text columns
text_columns = ['subtypes', 'business_status', 'email_1', 'type', 'city', 'full_address', 'name', 'state']
for col in text_columns:
    df[col] = df[col].str.strip()
df['name'] = df['name'].str.strip()


#5. Convert 'working_hours' from string representation of dictionary to actual dictionary
try:
    df['working_hours_dict'] = df['working_hours'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
except Exception as e:
    print("step 5 error:", e)


#6.1. Clean and standardize 'state' column using mapping and geolocation
state_mapping = {
    'Basra Governorate': 'Al Basrah',
    'Al Basrah محافظة': 'Al Basrah',
    'Basra Province': 'Al Basrah',
    'بصرة': 'Al Basrah',
    'البصرة العراق': 'Al Basrah',
    'البصرة محافظة': 'Al Basrah',
    'البصرة': 'Al Basrah',    
    'Baghdad Governorate': 'Baghdad',
    'بغداد': 'Baghdad',
    'Erbil Governorate': 'Arbil',
    'أربيل': 'Arbil',
    'Sulaymaniyah Governorate': 'As Sulaymaniyah',
    'Duhok Governorate': 'Dihok',
    'دهوك': 'Dihok',
    'Dhi Qar Governorate': 'Dhi Qar',
    'ذي قار': 'Dhi Qar',
    'Al Anbar Governorate': 'Al Anbar',
    'الأنبار': 'Al Anbar',
    'Najaf Governorate': 'An Najaf',
    'النجف': 'An Najaf',
    'Nineveh Governorate': 'Ninawa',
    'نينوى': 'Ninawa',
    'Saladin Governorate': 'Sala Ad Din',
    'صلاح الدين': 'Sala Ad Din',
    'Kirkuk Governorate': 'Kirkuk',
    'كركوك': 'Kirkuk',
    'Diyala Governorate': 'Diyala',
    'ديالى': 'Diyala',
    'Wasit Governorate': 'Wasit',
    'واسط': 'Wasit',
    'Karbala Governorate': 'Karbala',
    'كربلاء': 'Karbala',
    'Al-Qādisiyyah Governorate': 'Al Qadisiyah',
    'القادسية': 'Al Qadisiyah',
    'Babylon Governorate': 'Babil',
    'بابل': 'Babil',
    'Al Muthanna Governorate': 'Al Muthannia',
    'المثنى': 'Al Muthannia',
    'Maysan Governorate': 'Maysan',
    'ميسان': 'Maysan',
    'Ohio': 'Baghdad',
    'Malta': 'Dihok',
    'Kurdistan': 'unknown',
    'Kurdistan Region': 'unknown',
    'إقليم كوردستان': 'unknown',
    'KRG': 'unknown',
    'KRI': 'unknown',
    '1': 'unknown',
    '0': 'unknown',
    'A': 'unknown',
    'None': 'unknown',
    'NOT_PROVIDED': 'unknown',
    'لايوجد': 'unknown',
    'لا': 'unknown',
    'Almakall': 'unknown',
    'hader': 'unknown',
    'Dasy2': 'unknown',
    'North': 'unknown',
    'Iraq': 'unknown',
    'Farahidi': 'unknown',
    'mega_1230': 'unknown',
    'شارعنه': 'unknown',
    'ZoneF': 'unknown',
    'Delaware': 'unknown',
    'Alaska': 'unknown',
    'Vital': 'unknown',
    'Nazo': 'unknown',
    'Modon': 'unknown',
    'Tawarak': 'unknown',
    'كسنزان': 'unknown'
}


#6.2. Identify unique states not in the mapping and assign them to 'unknown'
unique_states = df['state'].unique()
for state in unique_states:
    if state not in state_mapping and pd.notna(state):
        state_mapping[state] = 'unknown'


df['state_clean'] = df['state'].map(state_mapping).fillna('unknown')


#6.3. Calculate distance using Haversine geolocation formula to fill in missing or 'unknown' states
def haversine(lat1, lon1, lat2, lon2):
    R = 6371 
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c


def find_nearest_governorate(lat, lon, gov_df):
    if pd.isna(lat) or pd.isna(lon):
        return "unknown"
    
    min_distance = float('inf')
    nearest_gov = "unknown"
    
    for _, row in gov_df.iterrows():
        distance = haversine(lat, lon, row['Latitude'], row['Longitude'])
        if distance < min_distance:
            min_distance = distance
            nearest_gov = row['States']
    
    # إذا كانت المسافة كبيرة جداً (أكثر من 150 كم)، نعتبرها "unknown"
    return nearest_gov if min_distance <= 130 else "unknown"


for idx, row in df.iterrows():
    if row['state_clean'] == 'unknown' and pd.notna(row['latitude']) and pd.notna(row['longitude']):
        nearest_gov = find_nearest_governorate(row['latitude'], row['longitude'], gov_df)
        df.at[idx, 'state_clean'] = nearest_gov


df = df[df["state_clean"] != "unknown"]


# 7. Visualize the distribution of companies across Iraq using Folium
"""iraq_map = folium.Map(location=[33.2232, 43.6793], zoom_start=6)

# إضافة نقاط للشركات مع ترميز لوني حسب المحافظة
colors = {
    'Al Basrah': 'red',
    'Baghdad': 'blue',
    'Arbil': 'green',
    'As Sulaymaniyah': 'purple',
    'Dihok': 'orange',
    'Al Anbar': 'darkred',
    'An Najaf': 'lightred',
    'Ninawa': 'beige',
    'Sala Ad Din': 'darkblue',
    'Kirkuk': 'darkgreen',
    'Diyala': 'lightgreen',
    'Wasit': 'cadetblue',
    'Karbala': 'pink',
    'Al Qadisiyah': 'lightblue',
    'Babil': 'darkpurple',
    'Al Muthannia': 'lightgray',
    'Maysan': 'black',
    'Dhi Qar': 'lightorange',
    'unknown': 'gray'
}

for idx, row in df.iterrows():
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        color = colors.get(row['state_clean'], 'gray')
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=3,
            color=color,
            fill=True,
            fill_color=color,
            #popup=f"{row['name']} - {row['state_clean']}"
            popup=f"{row['state_clean']}"
        ).add_to(iraq_map)

# حفظ الخريطة
iraq_map.save('iraq_companies_distribution.html')"""


type_counts = df['type'].value_counts().reset_index()
type_counts.columns = ['original_type', 'count']


# Function to detect if a string contains Arabic characters
def contains_arabic(text):
    arabic_range = re.compile('[\u0600-\u06FF]')
    return bool(arabic_range.search(str(text)))


"""# Separate Arabic and English types
type_counts['is_arabic'] = type_counts['original_type'].apply(contains_arabic)
arabic_types = type_counts[type_counts['is_arabic']]
english_types = type_counts[~type_counts['is_arabic']]
print(f"Arabic types: {len(arabic_types)}")
print(f"English types: {len(english_types)}")"""


# First, let's create a simple function to translate Arabic types to English
def translate_arabic_type(arabic_type):
    arabic_to_english_map = {
        'شركة إنشاء': 'construction company',
        'مقاول عام': 'general contractor',
        'خدمة التركيبات الكهربائية': 'electrical installation service',
        'شركة هندسة مدنية': 'civil engineering company',
        'محطة كهرباء فرعية': 'electrical substation',
        'مقاول أجهزة ميكانيكية': 'mechanical equipment contractor',
        'مستشفى خاصة': 'private hospital',
        'سبّاك': 'plumber',
        'مورد خرسانة جاهزة': 'ready-mix concrete supplier',
        'محطة معالجة مياه': 'water treatment plant',
        'بناء منازل': 'home builder',
        'مكتب الشركات': 'corporate office',
        'ورشة إصلاح سيارات': 'auto repair shop',
        'تصليح سيارات': 'car repair',
        'متجر قطع غيار سيارات': 'auto parts store',
        'مركز صيانة سيارات': 'auto maintenance center',
        'خدمات تدفئة وتكييف هواء وتبريد': 'hvac services',
        'التنمية السكنية': 'housing development',
        'مقاول بناء': 'building contractor',
        'شركة هندسة معمارية': 'architecture company',
        'متجر مواد بناء': 'building materials store',
        'ميكانيكي': 'mechanic',
        'مقاول': 'contractor',
        'كهربائي': 'electrician',
        'خدمة إصلاح تكييف الهواء': 'air conditioning repair service',
        'خدمة تغيير الزيت': 'oil change service',
        'مهندس استشاري': 'consulting engineer',
        'محطة فحص سيارات': 'vehicle inspection station',
        'ميكانيكي هياكل السيارات': 'auto body mechanic',
        'محل فرامل': 'brake shop',
        'خدمة ضبط السيارات': 'car tuning service',
        'متجر أجهزة إلكترونية': 'electronics store',
        'مورد معدات البناء': 'construction equipment supplier',
        'مقاول أسفلت': 'asphalt contractor',
        'حلول ومعدات الطاقة': 'energy solutions and equipment',
        'خدمة التركيبات الكهربائية': 'electrical installation service',
        'مسجد': 'mosque',
        'مصنع': 'factory',
        'مبنى سكني': 'residential building',
        'مقاول عام': 'general contractor',
        'وكيل شحن المركبات': 'Vehicle shipping agent',
        'وكالة عقارات': 'Real estate agency',
        'وكالة تأجير معدات': 'Equipment rental agency',
        'ورشة ميكانيكا سيارات': 'Auto mechanics workshop',
        'ورشة إصلاح هياكل سيارات': 'Auto body repair shop',
        'ورشة إصلاح دراجات': 'Bicycle repair shop',
        'هندسة إلكترونيات': 'Electronics engineering',
        'ميناء بحري': 'Seaport / Maritime port',
        'ميناء': 'Port',
        'مورد معدات صناعية': 'Industrial equipment supplier',
        'مورد معدات حقول النفط': 'Oil field equipment supplier',
        'مورد طعام وشراب': 'Food and beverage supplier',
        'مورد سيارات هيونداي': 'Hyundai car supplier/dealer',
        'مورد سيارات تويوتا': 'Toyota car supplier/dealer',
        'مورد أنابيب': 'Pipe supplier',
        'مهندس ميكانيكي': 'Mechanical engineer',
        'مهندس معماري': 'Architect',
        'مهندس مدني': 'Civil engineer',
        'مكتب مسح الأراضي': 'Land survey office',
        'مكتب حكومي': 'Government office',
        'مكتب الجمارك الرئيسية': 'Main customs office',
        'مقاول حفر': 'Excavation contractor',
        'مقاول بناء داخلي': 'Interior building contractor',
        'مقاول بناء تركيبات المنازل الجاهزة': 'Prefabricated home installation contractor',
        'مقاول بناء المنازل المخصّصة': 'Custom home builder',
        'مطوّر عقارات': 'Real estate developer',
        'مصنع خرسانة': 'Concrete plant',
        'مصنع أسمنت': 'Cement plant',
        'مصمم معماري': 'Architectural designer',
        'مصمم داخلي': 'Interior designer',
        'مصفاة نفط': 'Oil refinery',
        'مشتل للنباتات': 'Plant nursery',
        'مشتل': 'Nursery',
        'مسح بحري': 'Marine surveying',
        'مستشفى': 'Hospital',
        'مزود منتجات خرسانية': 'Concrete products supplier',
        'مزوّد خدمة إنترنت': 'Internet service provider (ISP)',
        'مزاد السيارات': 'Car auction',
        'مدينة الملاهي': 'Amusement park',
        'مختبر': 'Laboratory (Lab)',
        'محل ياي سيارات': 'Car accessories shop / Car audio shop',
        'محل أدوات كتم صوت': 'Soundproofing materials shop',
        'محل إصلاح محركات كهربائية': 'Electric motor repair shop',
        'محطة وقود': 'Gas station / Fuel station',
        'محطة غسل سيارات': 'Car wash station',
        'محطة حاويات': 'Container terminal',
        'محطة حافلات': 'Bus station',
        'مجمع الأنشطة التجارية': 'Commercial activities complex',
        'متنزه': 'Park',
        'متجر منتجات إضاءة بالجملة': 'Wholesale lighting products store',
        'متجر لإصلاح الدراجات الرباعية': 'ATV repair shop',
        'متجر زهور': 'Flower shop / Florist',
        'متجر زجاج سيارات': 'Car glass shop',
        'متجر دراجات': 'Bicycle shop',
        'متجر خردوات': 'Hardware store',
        'متجر تصليح الإطارات': 'Tire repair shop',
        'متجر تحف': 'Antique shop',
        'متجر بطاريات سيارات': 'Car battery store',
        'متجر أجهزة تسجيل سيارات': 'Car dashcam store',
        'متجر إصلاح هواتف الجوّال': 'Mobile phone repair shop',
        'ماسح الأراضي': 'Land surveyor',
        'قسم شرطة': 'Police department',
        'عناية بالحدائق': 'Garden care / Landscaping service',
        'صانع نماذج معمارية وهندسية': 'Architectural and engineering model maker',
        'شركة نقل': 'Transport company',
        'شركة عزل مائي': 'Waterproofing company',
        'شركة طاقة شمسية': 'Solar energy company',
        'شركة تشغيل الميناء': 'Port operating company',
        'شركة بترول': 'Petroleum company',
        'شركة أتمتة المنازل': 'Home automation company',
        'شركة إلكترونيات': 'Electronics company',
        'شركة إدارة عقارات': 'Property management company',
        'شركة البناء': 'Construction company',
        'شركة الاستيراد والتصدير': 'Import and export company',
        'شركة استثمار': 'Investment company',
        'سيارات': 'Cars / Automotive',
        'سوق مواد البناء': 'Building materials market',
        'سوق قطع غيار السيارات': 'Auto parts market',
        'سكن مدرسين/ات': 'Teachers housing',
        'سكن': 'Housing / Accommodation',
        'سجل تجاري': 'Commercial registry',
        'دورة مياه عامة': 'Public restroom',
        'دهان السيارات': 'Auto painting',
        'خدمة نباتات داخلية': 'Indoor plants service',
        'خدمة كهرباء سيارات': 'Auto electrical service',
        'خدمة ضبط العجلات ومحاذاتها': 'Wheel alignment service',
        'خدمة تجديد سيارات': 'Car refurbishment service',
        'خدمة أمنية': 'Security service',
        'خدمة إصلاح زجاج': 'Glass repair service',
        'خدمة استكشاف الغاز والبترول': 'Oil and gas exploration service',
        'حقل نفط': 'Oil field',
        'حضانة أطفال': 'Childrens nursery / Daycare',
        'حديقة': 'Garden',
        'جمعية الإسكان': 'Housing association',
        'تصليح دراجات نارية': 'Motorcycle repair',
        'تاجر سيارات': 'Car dealer',
        'أخصائي الأعمال التجميلية': 'Aesthetic specialist',
        'المقاول': 'The Contractor',
        'المساعدة المنزلية': 'Home help',
        'الخدمات اللوجستية': 'Logistics services',
        'استشاري البناء': 'Construction consultant'
    }
    return arabic_to_english_map.get(arabic_type, arabic_type)


# Function to clean and standardize the type column
def clean_type_column(type_series):
    # First, translate Arabic types to English
    translated_types = type_series.apply(
        lambda x: translate_arabic_type(x) if contains_arabic(x) else x
    )
    
    # Then convert everything to lowercase
    cleaned_types = translated_types.str.lower().str.strip()
    
    return cleaned_types


# Apply the cleaning function to the type column
df['type_clean'] = clean_type_column(df['type'])


# Comprehensive categorization function with more specific categories
def categorize_business(business_type):
    business_lower = business_type.lower()
    
    # Comprehensive category mapping
    categories = {
        'Construction & Engineering': [
            'construction', 'contractor', 'builder', 'engineering', 'مقاول', 'هندس', 'بناء', 
            'عزل', 'خرسانة', 'اسمنت', 'roofing', 'masonry', 'carpenter', 'blacksmith',
            'steel', 'concrete', 'excavating', 'demolition', 'plasterer', 'bricklayer'
        ],
        'Automotive Services': [
            'auto', 'car', 'vehicle', 'motor', 'tire', 'mechanic', 'brake',
            'radiator', 'transmission', 'detailing', 'tuning', 'alignment',
            'muffler', 'glass repair'
        ],
        'Retail & Shopping': [
            'store', 'shop', 'market', 'mall', 'متجر', 'سوق', 'تسوق', 'بيع', 'شراء',
            'outlet', 'wholesaler', 'تاجر', 'سوبرماركت', 'supermarket', 'grocery',
            'clothing', 'furniture', 'electronics', 'hardware', 'pharmacy', 'بقالة'
        ],
        'Professional Services': [
            'consultant', 'lawyer', 'accountant', 'engineer', 'designer', 'استشاري', 
            'محامي', 'محاسب', 'مصمم', 'notary', 'surveyor', 'appraiser', 'architect',
            'consulting', 'advisory', 'planning', 'management', 'broker', 'agent'
        ],
        'Healthcare & Medical': [
            'hospital', 'clinic', 'medical', 'dental', 'doctor', 'health',
            'nursing', 'therapy','eye'
        ],
        'Food & Hospitality': [
            'restaurant', 'cafe', 'food', 'beverage', 'مطعم', 'مقهى', 'طعام', 'شراب',
            'مأكولات', 'hotel', 'catering', 'bakery', 'butcher', 'coffee', 'bar',
            'pub', 'grill', 'buffet', 'pizza', 'fast food', 'ice cream', 'confectionery'
        ],
        'Real Estate & Property': [
            'real estate', 'property', 'عقارات', 'وكالة عقارات', 'تطوير عقاري',
            'housing', 'apartment', 'condominium', 'villa', 'land', 'estate',
            'developer', 'management', 'rental', 'lease', 'mortgage', 'title'
        ],
        'Technology & IT Services': [
            'software', 'computer', 'internet', 'technology', 'IT', 'برمجة', 'حاسوب',
            'انترنت', 'تكنولوجيا', 'electronic', 'digital', 'web', 'programming',
            'coding', 'data', 'network', 'security', 'hosting', 'e-commerce'
        ],
        'Education & Training': [
            'school', 'university', 'college', 'education', 'training', 'مدرسة', 'جامعة',
            'تعليم', 'تدريب', 'institute', 'academy', 'learning', 'tuition', 'course',
            'kindergarten', 'preschool', 'nursery', 'vocational', 'language'
        ],
        'Government & Public Services': [
            'government', 'public', 'police', 'court', 'حكومي', 'شرطة', 'محكمة', 'بلدية',
            'municipal', 'administration', 'registry', 'customs', 'embassy', 'consulate',
            'ministry', 'department', 'agency', 'office', 'service', 'مركز خدمة'
        ],
        'Manufacturing & Industry': [
            'manufacturer', 'factory', 'industry', 'industrial', 'مصنع', 'صناعي', 'تصنيع',
            'production', 'plant', 'mill', 'workshop', 'fabrication', 'assembly', 'processing'
        ],
        'Logistics & Transportation': [
            'logistics', 'shipping', 'transport', 'نقل', 'شحن', 'لوجستي', 'مواصلات',
            'delivery', 'freight', 'forwarding', 'courier', 'moving', 'haulage', 'distribution',
            'port', 'marine', 'airport', 'terminal', 'warehouse', 'storage'
        ],
        'Energy & Utilities': [
            'energy', 'power', 'oil', 'gas', 'طاقة', 'نفط', 'غاز', 'كهرباء', 'solar',
            'petroleum', 'refinery', 'utility', 'water', 'electric', 'renewable', 'fuel',
            'generator', 'transmission', 'distribution'
        ],
        'Financial Services': [
            'bank', 'insurance', 'financial', 'investment', 'بنك', 'تأمين', 'مالي', 'استثمار',
            'credit', 'loan', 'mortgage', 'exchange', 'brokerage', 'fund', 'asset', 'wealth',
            'advisory', 'planning', 'accounting', 'audit', 'tax'
        ],
        'Personal Services': [
            'beauty', 'salon', 'spa', 'hair', 'جمال', 'صالون', 'تجميل', 'شعر', 'nails',
            'barber', 'cosmetic', 'aesthetic', 'wellness', 'massage', 'skin care', 'makeup',
            'esthetics', 'piercing', 'tattoo', 'laser', 'waxing'
        ],
        'Entertainment & Recreation': [
            'entertainment', 'recreation', 'park', 'club', 'ترفيه', 'منتزه', 'نادي', 'ملاهي',
            'sports', 'gym', 'fitness', 'cinema', 'theater', 'museum', 'gallery', 'art',
            'music', 'dance', 'bowling', 'pool', 'billiards', 'amusement', 'adventure'
        ],
        'Home Services': [
            'cleaning', 'maintenance', 'repair', 'installation', 'خدمة منزلية', 'تنظيف',
            'صيانة', 'إصلاح', 'تركيب', 'plumbing', 'electrical', 'painting', 'carpentry',
            'gardening', 'landscaping', 'pest control', 'security', 'alarm', 'locksmith'
        ],
        'Communications & Media': [
            'telecommunication', 'communication', 'media', 'broadcast', 'اتصال', 'اتصالات',
            'إعلام', 'بث', 'radio', 'television', 'newspaper', 'publishing', 'printing',
            'advertising', 'marketing', 'public relations', 'design', 'creative', 'agency'
        ],
        'Environmental Services': [
            'environmental', 'recycling', 'waste', 'نفايات', 'بيئة', 'إعادة تدوير',
            'conservation', 'sustainability', 'green', 'eco', 'pollution', 'treatment',
            'cleanup', 'remidation', 'conservation', 'protection'
        ],
        'Child Care Services': [
            'child care', 'day care', 'nursery', 'kindergarten', 'preschool'
        ],
        'Religious Institutions': [
            'church', 'mosque', 'temple', 'synagogue', 'religious', 'place of worship'
        ],
        'Agriculture & Farming': [
            'farm', 'agriculture', 'farming', 'زراعة', 'مزرعة', 'حقل', 'livestock',
            'dairy', 'orchard', 'vineyard', 'greenhouse', 'aquaculture', 'horticulture'
        ]
    }
    
    # Check for matches
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in business_lower:
            #if re.search(rf'\b{re.escape(keyword)}\b', business_lower):
                return category
    
    # If no match found, use text similarity with existing categories
    return 'Specialized Services'

# Apply categorization
df['category'] = df['type_clean'].apply(categorize_business)


# Use K-means clustering for the remaining uncategorized items
uncategorized_mask = df['category'] == 'Specialized Services'
if uncategorized_mask.sum() > 0:
    # Vectorize the text
    vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
    X = vectorizer.fit_transform(df[uncategorized_mask]['type_clean'])
    
    # Cluster into 5 groups
    kmeans = KMeans(n_clusters=5, random_state=42)
    clusters = kmeans.fit_predict(X)
    
    # Assign cluster-based categories
    cluster_categories = {
        0: 'Business Services',
        1: 'Specialty Retail',
        2: 'Technical Services',
        3: 'Community Services',
        4: 'Specialized Manufacturing'
    }
    
    df.loc[uncategorized_mask, 'category'] = [cluster_categories.get(c, 'Specialized Services') for c in clusters]


cursor.execute("""
if OBJECT_ID('dbo.NewAll', 'U') IS NOT NULL
    DROP TABLE dbo.NewAll;

CREATE TABLE NewAll (
    RecordID INT PRIMARY KEY,
    rating FLOAT,
    subtypes NVARCHAR(MAX),
    business_status NVARCHAR(255),
    email_1 NVARCHAR(255),
    working_hours NVARCHAR(MAX),
    verified BIT,
    type_clean NVARCHAR(255),
    city NVARCHAR(255),
    linkedin NVARCHAR(255),
    reviews INT,
    postal_code NVARCHAR(255),
    reviews_per_score_1 INT,
    photos_count INT,
    about NVARCHAR(MAX),
    reviews_per_score_2 INT,
    full_address NVARCHAR(MAX),
    reviews_per_score_4 INT,
    reviews_per_score_3 INT,
    latitude FLOAT,
    location_link NVARCHAR(MAX),
    location_reviews_link NVARCHAR(MAX),
    longitude FLOAT,
    name NVARCHAR(255),
    phone NVARCHAR(255),
    phone_1 NVARCHAR(255),
    site NVARCHAR(548),
    state_clean NVARCHAR(255),
    reviews_per_score_5 INT,
    owner_id NVARCHAR(255),
    category NVARCHAR(255)
)""")


for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO NewAll (
            RecordID, rating, subtypes, business_status, email_1, working_hours,
            verified, type_clean, city, linkedin, reviews, postal_code,
            reviews_per_score_1, photos_count, about, reviews_per_score_2, full_address,
            reviews_per_score_4, reviews_per_score_3, latitude, location_link,
            location_reviews_link, longitude, name, phone, phone_1,
            site, state_clean, reviews_per_score_5, owner_id, category
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, 
    row["RecordID"], row["rating"], row["subtypes"], row["business_status"], row["email_1"],
    row["working_hours"], row["verified"], row["type_clean"], row["city"], row["linkedin"],
    row["reviews"], row["postal_code"], row["reviews_per_score_1"], row["photos_count"], row["about"],
    row["reviews_per_score_2"], row["full_address"], row["reviews_per_score_4"], row["reviews_per_score_3"],
    row["latitude"], row["location_link"], row["location_reviews_link"], row["longitude"], row["name"],
    row["phone"], row["phone_1"], row["site"], row["state_clean"], row["reviews_per_score_5"],
    row["owner_id"], row["category"])


conn.commit()
cursor.close()
conn.close()


# ######################### 4. remove unnecessary outliers#############
# category_counts = df['category'].value_counts()
# rare_categories = category_counts[category_counts < 5] 
# if len(rare_categories) > 0:
#     df['category'] = df['category'].apply(
#         lambda x: 'أخرى' if x in rare_categories.index else x
#     )
# # Example: Remove outliers in SalePrice using IQR
# #Q1 = df['Impact_on_Grades'].quantile(0.25)
# #Q3 = df['Impact_on_Grades'].quantile(0.75)
# #IQR = Q3 - Q1
# #df = df[~((df['Impact_on_Grades'] < (Q1 - 1.5 * IQR)) | (df['Impact_on_Grades'] > (Q3 + 1.5 * IQR)))]


# print("\n\n\n=================Final DataFrame after Normalization==============")
# print(df.head(5))

# # Save the processed DataFrame to a new CSV file
# df.to_csv("encoded_data.csv", index=False)