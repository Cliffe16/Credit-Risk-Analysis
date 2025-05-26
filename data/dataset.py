import pyodbc
import random
import datetime
import math
from faker import Faker
import sys
from contextlib import contextmanager
import configparser
import os
import decimal 
from decimal import Decimal, ROUND_HALF_UP

def create_config_if_not_exists():
    if not os.path.exists('config.ini'):
        config = configparser.ConfigParser()
        
        
        # Database section
        config['database'] = {
            'server': '192.168.100.30',
            'database': 'QuickPesaDB',
            'driver': 'ODBC Driver 18 for SQL Server',
            'trusted_connection': 'no',
            'uid' : 'sa',
            'pwd' : 'BLOOMberg411**',
            'trust_server_certificate': 'yes' 
        }
        
        # Generation section
        config['generation'] = {
            'customer_count': '2500',
            'batch_size': '1500',
            'transaction_months': '15',
            'loan_apps_per_day': '3500',
            'max_active_customers': '1900',
            'test_mode': 'False',
            'tier_upgrade_threshold' : '1',
            'max_tier' : '5',
            'tier_amount_multiplier' : '1.5',
            'initial_max_eligible_amount' : '1000',
            'tier_amount_increment' : '1000',
            'absolute_max_loan_amount' : '100000.00'            
        }
        
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        print("Created new config.ini file with default values")
        
# Call this function at the start of your script
create_config_if_not_exists()

# Configuration Management
def load_config(config_file ='config.ini'):
    """Load configuration from file"""
    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        db_settings = {
            'server': config.get('database', 'server'),
            'database': config.get('database', 'database'),
            'driver': config.get('database', 'driver'),
            'trusted_connection': config.getboolean('database', 'trusted_connection', fallback=False)
        }
        # Only add uid and pwd if trusted_connection is not true
        if not db_settings['trusted_connection']:
            db_settings['uid'] = config.get('database', 'uid', fallback=None)
            db_settings['pwd'] = config.get('database', 'pwd', fallback=None)

        # Add trust_server_certificate, defaulting to False if not found for safety,
        # but for your case, you need it to be True in the config file.
        db_settings['trust_server_certificate'] = config.getboolean('database', 'trust_server_certificate', fallback=False)

        return {
            'db_connection': db_settings,
            'generation': {
                'customer_count': config.getint('generation', 'customer_count'),
                'batch_size': config.getint('generation', 'batch_size'),
                'transaction_months': config.getint('generation', 'transaction_months'),
                'loan_apps_per_day': config.getint('generation', 'loan_apps_per_day'),
                'max_active_customers': config.getint('generation', 'max_active_customers'),
                'test_mode': config.getboolean('generation', 'test_mode'),
                'initial_max_eligible_amount': config.getfloat('generation', 'initial_max_eligible_amount', fallback=500.00),
                'tier_upgrade_threshold': config.getint('generation', 'tier_upgrade_threshold', fallback=1),
                'max_tier': config.getint('generation', 'max_tier', fallback=5),
                'tier_amount_multiplier': config.getfloat('generation', 'tier_amount_multiplier', fallback=1.5),
                'tier_amount_increment': config.getfloat('generation', 'tier_amount_increment', fallback=1000.00),
                'absolute_max_loan_amount': config.getfloat('generation', 'absolute_max_loan_amount', fallback=100000.00)
            }
        }
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        raise ValueError(f"Invalid config file structure or missing option: {str(e)}")


# Initialize Faker and random seed
fake = Faker()
random.seed(42)

# Connection Management
@contextmanager

def db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        config_data = load_config() # Renamed to avoid conflict with 'configparser.ConfigParser()' instance
        db_config = config_data['db_connection']

        conn_str_parts = [
            f"Driver={{{db_config['driver']}}}",
            f"Server={db_config['server']}",
            f"Database={db_config['database']}"
        ]

        if db_config.get('trusted_connection'): # Check if the key exists and is True
            conn_str_parts.append("Trusted_Connection=yes")
        elif db_config.get('uid') and db_config.get('pwd'): # Check if UID/PWD are provided
            conn_str_parts.append(f"UID={db_config['uid']}")
            conn_str_parts.append(f"PWD={db_config['pwd']}")
        else:
            # If trusted_connection is false, and no uid/pwd, it's an issue.
            raise ValueError(
                "Database configuration in config.ini requires UID/PWD when Trusted_Connection is 'no'."
            )

        # Add TrustServerCertificate if set to True in config
        if db_config.get('trust_server_certificate'):
            conn_str_parts.append("TrustServerCertificate=yes")
        
        # Some drivers/setups might also require Encrypt=yes for TrustServerCertificate=yes to work as expected
        # or if the server enforces encryption. You can try adding this if TrustServerCertificate=yes alone doesn't resolve it.
        # conn_str_parts.append("Encrypt=yes")


        conn_str = ";".join(conn_str_parts)

        log_conn_str = conn_str
        if db_config.get('pwd'): # Mask password for logging
            log_conn_str = conn_str.replace(f"PWD={db_config['pwd']}", "PWD=********")
        print(f"Attempting to connect with: {log_conn_str}")

        conn = pyodbc.connect(conn_str, autocommit=False)
        yield conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database connection error (pyodbc): {sqlstate} - {str(ex)}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Generic database connection error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
# Progress Reporting
def show_progress(current, total, start_time=None, prefix=""):
    """Enhanced progress reporting with prefix"""
    percent = (current / total) * 100
    elapsed = (datetime.datetime.now() - start_time).seconds if start_time else 0
    
    if current > 0 and elapsed > 0:
        rate = current / elapsed
        remaining = (total - current) / rate
        eta = f"ETA: {remaining:.0f}s"
    else:
        eta = ""
    
    sys.stdout.write(f"\r{prefix}Progress: {percent:.1f}% | {current}/{total} | {eta}")
    sys.stdout.flush()
    
    if current >= total:
        print()  # New line when complete
        
# Error Handling
class DataGenerationError(Exception):
    """Base exception for data generation errors"""
    pass

class DatabaseError(DataGenerationError):
    """Database-related errors"""
    pass

class GenerationError(DataGenerationError):
    """Data generation logic errors"""
    pass

# Kenyan-specific data (unchanged from your original)
kenyan_counties = { 
    'Mombasa': {
        'weight': 12,
        'urban_ratio': 0.85,
        'subcounties': ['Changamwe', 'Jomvu', 'Kisauni', 'Likoni', 'Mvita', 'Nyali'],
        'towns': ['Mombasa', 'Miritini', 'Mikindani', 'Bamburi', 'Shanzu', 'Kongowea']
    },
    'Nairobi': {
        'weight': 35,
        'urban_ratio': 0.95,
        'subcounties': ['Dagoretti North', 'Dagoretti South', 'Embakasi Central', 'Embakasi East', 
                       'Embakasi North', 'Embakasi South', 'Embakasi West', 'Kasarani', 'Kibra', 
                       'Langata', 'Makadara', 'Mathare', 'Starehe', 'Westlands', 'Roysambu', 'Ruaraka'],
        'towns': ['Nairobi', 'Eastleigh', 'Karen', 'Runda']
    },
    'Kwale': {
        'weight': 1,
        'urban_ratio': 0.43,
        'subcounties': ['Matuga', 'Msambweni', 'Lunga Lunga', 'Kinango'],
        'towns': ['Kwale', 'Ukunda', 'Diani', 'Msambweni', 'Shelly Beach']
    },
    'Kilifi': {
        'weight': 8,
        'urban_ratio': 0.55,
        'subcounties': ['Kilifi North', 'Kilifi South', 'Kaloleni', 'Rabai', 'Ganze', 'Malindi', 'Magarini'],
        'towns': ['Kilifi', 'Malindi', 'Mariakani', 'Mavueni', 'Bamba']
    },
    'Tana River': {
        'weight': 2,
        'urban_ratio': 0.44,
        'subcounties': ['Bura', 'Galole', 'Garsen'],
        'towns': ['Hola', 'Garsen', 'Bura', 'Kipini']
    },
    'Lamu': {
        'weight': 1,
        'urban_ratio': 0.54,
        'subcounties': ['Lamu East', 'Lamu West'],
        'towns': ['Lamu', 'Mpeketoni', 'Faza', 'Kiunga']
    },
    'Taita-Taveta': {
        'weight': 2,
        'urban_ratio': 0.54,
        'subcounties': ['Taveta', 'Wundanyi', 'Mwatate', 'Voi'],
        'towns': ['Voi', 'Taveta', 'Wundanyi', 'Mwatate']
    },
    'Garissa': {
        'weight': 1,
        'urban_ratio': 0.53,
        'subcounties': ['Garissa Township', 'Balambala', 'Lagdera', 'Dadaab', 'Fafi', 'Ijara'],
        'towns': ['Garissa', 'Dadaab', 'Hulugho', 'Modogashe']
    },
    'Wajir': {
        'weight': 0.5,
        'urban_ratio': 0.40,
        'subcounties': ['Wajir North', 'Wajir East', 'Wajir South', 'Wajir West', 'Eldas', 'Tarbaj'],
        'towns': ['Wajir', 'Habaswein', 'Buna', 'Eldas', 'Tarbaj']
    },
    'Mandera': {
        'weight': 0.5,
        'urban_ratio': 0.58,
        'subcounties': ['Mandera East', 'Mandera North', 'Mandera South', 'Mandera West', 'Lafey', 'Banissa'],
        'towns': ['Mandera', 'Takaba', 'Elwak', 'Rhamu']
    },
    'Marsabit': {
        'weight': 1,
        'urban_ratio': 0.42,
        'subcounties': ['Moyale', 'Saku', 'Laisamis', 'North Horr'],
        'towns': ['Marsabit', 'Moyale', 'Loiyangalani']
    },
    'Isiolo': {
        'weight': 2,
        'urban_ratio': 0.65,
        'subcounties': ['Isiolo', 'Meru North', 'Garbatulla'],
        'towns': ['Isiolo', 'Garba Tulla', 'Merti']
    },
    'Meru': {
        'weight': 8,
        'urban_ratio': 0.20,
        'subcounties': ['Buuri', 'Imenti North', 'Imenti South', 'Tigania East', 'Tigania West', 'Igembe Central', 'Igembe North', 'Igembe South'],
        'towns': ['Meru', 'Maua', 'Kangeta', 'Mikinduri']
    },
    'Tharaka-Nithi': {
        'weight': 2,
        'urban_ratio': 0.15,
        'subcounties': ['Tharaka North', 'Tharaka South', 'Maara', 'Chuka/Igambang\'ombe'],
        'towns': ['Chuka', 'Kathwana', 'Marimanti']
    },
    'Embu': {
        'weight': 3,
        'urban_ratio': 0.30,
        'subcounties': ['Embu North', 'Embu East', 'Embu West', 'Manyatta', 'Runyenjes'],
        'towns': ['Embu', 'Runyenjes', 'Siakago']
    },
    'Kitui': {
        'weight': 2,
        'urban_ratio': 0.10,
        'subcounties': ['Kitui West', 'Kitui Central', 'Kitui Rural', 'Mwingi North', 'Mwingi West', 'Mwingi Central'],
        'towns': ['Kitui', 'Mwingi', 'Mutomo']
    },
    'Machakos': {
        'weight': 7,
        'urban_ratio': 0.50,
        'subcounties': ['Machakos Town', 'Masinga', 'Kangundo', 'Matungulu', 'Kathiani', 'Mavoko', 'Mwala', 'Yatta'],
        'towns': ['Machakos', 'Athi River', 'Kangundo', 'Tala']
    },
    'Makueni': {
        'weight': 2,
        'urban_ratio': 0.12,
        'subcounties': ['Makueni', 'Kibwezi East', 'Kibwezi West', 'Kilome', 'Mbooni'],
        'towns': ['Wote', 'Kibwezi', 'Sultan Hamud']
    },
    'Nyandarua': {
        'weight': 3,
        'urban_ratio': 0.32,
        'subcounties': ['Kinangop', 'Kipipiri', 'Ol Kalou', 'Ol Jorok', 'Ndaragwa'],
        'towns': ['Ol Kalou', 'Engineer', 'Njabini']
    },
    'Nyeri': {
        'weight': 5,
        'urban_ratio': 0.43,
        'subcounties': ['Tetu', 'Kieni', 'Mathira', 'Othaya', 'Mukurweini', 'Nyeri Town'],
        'towns': ['Nyeri', 'Karatina', 'Othaya']
    },
    'Kirinyaga': {
        'weight': 4,
        'urban_ratio': 0.43,
        'subcounties': ['Kirinyaga Central', 'Kirinyaga East', 'Kirinyaga West', 'Mwea East', 'Mwea West'],
        'towns': ['Kerugoya', 'Sagana', 'Wanguru']
    },
    'Murang\'a': {
        'weight': 4,
        'urban_ratio': 0.30,
        'subcounties': ['Kandara', 'Gatanga', 'Kiharu', 'Kangema', 'Mathioya', 'Kigumo', 'Maragua'],
        'towns': ['Murang\'a', 'Kenol', 'Kangari']
    },
    'Kiambu': {
        'weight': 27,
        'urban_ratio': 0.85,
        'subcounties': ['Gatundu North', 'Gatundu South', 'Juja', 'Thika Town', 'Ruiru', 'Githunguri', 'Kiambu', 'Kiambaa', 'Kabete', 'Lari', 'Limuru'],
        'towns': ['Thika', 'Kiambu', 'Ruiru', 'Gatundu']
    },
    'Turkana': {
        'weight': 1,
        'urban_ratio': 0.38,
        'subcounties': ['Turkana North', 'Turkana West', 'Turkana Central', 'Turkana East', 'Turkana South', 'Loima'],
        'towns': ['Lodwar', 'Kakuma', 'Lokichogio']
    },
    'West Pokot': {
        'weight': 0.5,
        'urban_ratio': 0.07,
        'subcounties': ['Pokot North', 'Pokot South', 'Pokot Central', 'Pokot East'],
        'towns': ['Kapenguria', 'Alale', 'Ortum']
    },
    'Samburu': {
        'weight': 1,
        'urban_ratio': 0.35,
        'subcounties': ['Samburu North', 'Samburu East', 'Samburu West'],
        'towns': ['Maralal', 'Barsaloi', 'Wamba']
    },
    'Trans-Nzoia': {
        'weight': 4,
        'urban_ratio': 0.37,
        'subcounties': ['Cherangany', 'Kwanza', 'Endebess', 'Saboti', 'Kiminini'],
        'towns': ['Kitale', 'Endebess']
    },
    'Uasin Gishu': {
        'weight':6,
        'urban_ratio': 0.75,
        'subcounties': ['Ainabkoi', 'Kesses', 'Moiben', 'Soy', 'Turbo', 'Kapseret'],
        'towns': ['Eldoret', 'Burnt Forest']
    },
    'Elgeyo-Marakwet': {
        'weight': 2,
        'urban_ratio': 0.05,
        'subcounties': ['Keiyo North', 'Keiyo South', 'Marakwet East', 'Marakwet West'],
        'towns': ['Iten', 'Kapcherop', 'Tambach']
    },
    'Nandi': {
        'weight': 2,
        'urban_ratio': 0.08,
        'subcounties': ['Chesumei', 'Nandi Hills', 'Tinderet', 'Aldai', 'Emgwen', 'Mosop'],
        'towns': ['Kapsabet', 'Nandi Hills']
    },
    'Baringo': {
        'weight': 1,
        'urban_ratio': 0.32,
        'subcounties': ['Baringo Central', 'Baringo North', 'Baringo South', 'Eldama Ravine', 'Mogotio', 'Tiaty'],
        'towns': ['Kabarnet', 'Eldama Ravine', 'Marigat']
    },
    'Laikipia': {
        'weight': 5,
        'urban_ratio': 0.39,
        'subcounties': ['Laikipia Central', 'Laikipia East', 'Laikipia North', 'Laikipia West', 'Nyandarua South'],
        'towns': ['Nanyuki', 'Rumuruti', 'Nyahururu']
    },
    'Nakuru': {
        'weight': 15,
        'urban_ratio': 0.65,
        'subcounties': ['Nakuru Town East', 'Nakuru Town West', 'Njoro', 'Molo', 'Gilgil', 'Naivasha', 'Kuresoi North', 'Kuresoi South', 'Subukia', 'Rongai', 'Bahati'],
        'towns': ['Nakuru', 'Naivasha', 'Molo', 'Gilgil']
    },
    'Narok': {
        'weight': 5,
        'urban_ratio': 0.74,
        'subcounties': ['Narok North', 'Narok South', 'Narok East', 'Narok West', 'Transmara East', 'Transmara West'],
        'towns': ['Narok', 'Kilgoris']
    },
    'Kajiado': {
        'weight': 30,
        'urban_ratio': 0.80,
        'subcounties': ['Kajiado Central', 'Kajiado East', 'Kajiado North', 'Kajiado West', 'Kajiado South'],
        'towns': ['Kajiado', 'Ongata Rongai', 'Kitengela']
    },
    'Kericho': {
        'weight': 5,
        'urban_ratio': 0.40,
        'subcounties': ['Ainamoi', 'Bureti', 'Belgut', 'Kipkelion East', 'Kipkelion West', 'Soin/Sigowet'],
        'towns': ['Kericho', 'Litein', 'Kipkelion']
    },
    'Bomet': {
        'weight': 3,
        'urban_ratio': 0.05,
        'subcounties': ['Bomet Central', 'Bomet East', 'Chepalungu', 'Konoin', 'Sotik'],
        'towns': ['Bomet', 'Sotik', 'Longisa']
    },
    'Kakamega': {
        'weight': 14,
        'urban_ratio': 0.38,
        'subcounties': ['Butere', 'Mumias East', 'Mumias West', 'Matungu', 'Khwisero', 'Shinyalu', 'Lugari', 'Malava', 'Navakholo', 'Ikolomani'],
        'towns': ['Kakamega', 'Mumias', 'Malava']
    },
    'Vihiga': {
        'weight': 3,
        'urban_ratio': 0.30,
        'subcounties': ['Emuhaya', 'Luanda', 'Vihiga', 'Sabatia', 'Hamisi'],
        'towns': ['Vihiga', 'Mbale', 'Luanda']
    },
    'Bungoma': {
        'weight': 8,
        'urban_ratio': 0.37,
        'subcounties': ['Bungoma North', 'Bungoma South', 'Bungoma East', 'Bungoma West', 'Chetambe', 'Tongaren', 'Kimilili', 'Webuye East', 'Webuye West'],
        'towns': ['Bungoma', 'Webuye', 'Kimilili']
    },
    'Busia': {
        'weight': 3,
        'urban_ratio': 0.39,
        'subcounties': ['Bunyala', 'Butula', 'Nambale', 'Teso North', 'Teso South'],
        'towns': ['Busia', 'Malaba', 'Amagoro']
    },
    'Siaya': {
        'weight': 2,
        'urban_ratio': 0.25,
        'subcounties': ['Alego Usonga', 'Bondo', 'Rarieda', 'Gem', 'Ugunja', 'Ugenya'],
        'towns': ['Siaya', 'Bondo', 'Ugunja']
    },
    'Kisumu': {
        'weight': 14,
        'urban_ratio': 0.55,
        'subcounties': ['Kisumu East', 'Kisumu West', 'Kisumu Central', 'Seme', 'Muhoroni', 'Nyando', 'Nyakach'],
        'towns': ['Kisumu', 'Ahero', 'Muhoroni']
    },
    'Homa Bay': {
        'weight': 4,
        'urban_ratio': 0.39,
        'subcounties': ['Homa Bay Town', 'Ndhiwa', 'Rangwe', 'Karachuonyo', 'Kabondo Kasipul', 'Suba North', 'Suba South'],
        'towns': ['Homa Bay', 'Rod Kopany']
    },
    'Migori': {
        'weight': 6,
        'urban_ratio': 0.44,
        'subcounties': ['Migori', 'Uriri', 'Awendo', 'Nyatike', 'Rongo', 'Kuria East', 'Kuria West'],
        'towns': ['Migori', 'Rongo', 'Kehancha']
    },
    'Kisii': {
        'weight': 7,
        'urban_ratio': 0.43,
        'subcounties': ['Kitutu Chache North', 'Kitutu Chache South', 'Nyaribari Chache', 'Nyaribari Masaba', 'Bomachoge Borabu', 'Bomachoge Chache', 'Bobasi', 'Bomachoge Borabu'],
        'towns': ['Kisii', 'Ogembo', 'Keroka']
    },
    'Nyamira': {
        'weight': 4,
        'urban_ratio': 0.25,
        'subcounties': ['Borabu', 'Kitutu Masaba', 'North Mugirango', 'West Mugirango'],
        'towns': ['Nyamira', 'Keroka']
    } }  # Keep your original county data
male_first_names = [ ('John', 'Common'), ('James', 'Common'), ('David', 'Common'), ('Joseph', 'Common'), 
    ('Peter', 'Common'), ('Paul', 'Common'), ('Michael', 'Common'), ('William', 'Common'),
    ('Mark', 'Common'), ('Benjamin', 'Common'), ('Abel', 'Common'), ('Joseph', 'Common'), 
    ('Elisaphan', 'Common'), ('Wycliffe', 'Common'), ('Seth', 'Common'), ('Zachariah', 'Common'),
    ('Cliff', 'Common'), ('Emmanuel', 'Common'), ('Joe', 'Common'), ('Victor', 'Common'), 
    ('Clifford', 'Common'), ('Asa', 'Common'), ('Steve', 'Common'), ('Sospeter', 'Common'),
    ('Brian', 'Common'), ('Alvin', 'Common'), ('Kevin', 'Common'), ('Dennis', 'Common'), 
    ('Ryan', 'Common'), ('Kelvin', 'Common'), ('Robert', 'Common'), ('Jeff', 'Common'),
    ('Japheth', 'Kikuyu'),('Ochieng', 'Luo'), ('Odhiambo', 'Luo'), ('Omondi', 'Luo'), 
    ('Otieno', 'Luo'),('Mohamed', 'Muslim'), ('Abdullahi', 'Muslim'), ('Ali', 'Muslim'),
    ('Omar', 'Muslim'),('Hassan', 'Muslim'), ('Ibrahim', 'Muslim'), ('Yusuf', 'Muslim'),
    ('Abdi', 'Muslim'),('Mwirigi', 'Meru'), ('Kithinji', 'Meru'), ('Lekutan', 'Samburu'),
    ('Lentir', 'Samburu'),('Lokwawi', 'Pokot'), ('Lomuria', 'Pokot'), ('Mugambi', 'Tharaka'),
    ('Gitari', 'Tharaka'),('Farah', 'Somali'), ('Aden', 'Somali'), ('Warsame', 'Somali'), 
    ('Ismail', 'Somali') ]  # Keep your original names
female_first_names = [ ('Mary', 'Common'), ('Elizabeth', 'Common'), ('Susan', 'Common'), ('Margaret', 'Common'),
    ('Joyce', 'Common'), ('Ann', 'Common'), ('Grace', 'Common'), ('Jane', 'Common'),
    ('Mercy', 'Common'), ('Esther', 'Common'), ('Chloe', 'Common'), ('Sarah', 'Common'),
    ('Monicah', 'Common'), ('Maureen', 'Common'), ('Angela', 'Common'), ('Emma', 'Common'),
    ('Hazel', 'Common'), ('Ruby', 'Common'), ('Kelsey', 'Common'), ('Charlotte', 'Common'),
    ('June', 'Common'), ('Nelly', 'Common'), ('Leah', 'Common'), ('Caroline', 'Common'),
    ('Chrsitine', 'Common'), ('Winnie', 'Common'), ('Phoebe', 'Common'), ('Faith', 'Common'),
    ('Nasimiyu', 'Luhya'), ('Nekesa', 'Luhya'),('Chebet', 'Kalenjin'), ('Mwikali', 'Kamba'), 
    ('Amina', 'Muslim'), ('Fatuma', 'Muslim'), ('Khadija', 'Muslim'), ('Hawa', 'Muslim'),
    ('Mariam', 'Muslim'), ('Asha', 'Muslim'), ('Halima', 'Muslim'), ('Zainab', 'Muslim'),
    ('Kanana', 'Meru'),('Nakiru', 'Turkana'), ('Nangiro', 'Turkana'),
    ('Naserian', 'Samburu'), ('Nakoyo', 'Pokot'), ('Nacheke', 'Pokot'), ('Nang''iro', 'Pokot'),
    ('Nakarin', 'Pokot'),('Naserian', 'Maasai'),('Mwadime', 'Taita'), ('Mwakio', 'Taita'), 
    ('Kanini', 'Embu'),('Habiba', 'Somali'), ('Sahra', 'Somali'), ('Asli', 'Somali') ]  # Keep your original names
last_names = [ ('Mwangi', 'Kikuyu'), ('Maina', 'Kikuyu'), ('Kamau', 'Kikuyu'), ('Njoroge', 'Kikuyu'),
    ('Gachau', 'Kikuyu'),
    ('Ochieng', 'Luo'), ('Odhiambo', 'Luo'), ('Omondi', 'Luo'), ('Otieno', 'Luo'),
    ('Wamalwa', 'Luhya'), ('Wetangula', 'Luhya'), ('Mudavadi', 'Luhya'), ('Wafula', 'Luhya'),
    ('Kipchoge', 'Kalenjin'), ('Korir', 'Kalenjin'), ('Kiplagat', 'Kalenjin'), ('Kiprop', 'Kalenjin'),
    ('Mutua', 'Kamba'), ('Musyoka', 'Kamba'), ('Kilonzo', 'Kamba'), ('Nzioka', 'Kamba'),
    ('Mohamed', 'Muslim'), ('Abdullahi', 'Muslim'), ('Hassan', 'Muslim'), ('Abdi', 'Muslim'),
    ('Mwirigi', 'Meru'), ('Kithinji', 'Meru'), ('Mugambi', 'Meru'), ('Kiraitu', 'Meru'),
    ('Kahindi', 'Mijikenda'), ('Katana', 'Mijikenda'), ('Karisa', 'Mijikenda'), ('Baya', 'Mijikenda'),
    ('Ekai', 'Turkana'), ('Lokale', 'Turkana'), ('Ewoi', 'Turkana'), ('Lowasa', 'Turkana'),
    ('Lenasalia', 'Samburu'), ('Leleina', 'Samburu'), ('Lekutan', 'Samburu'), ('Lentir', 'Samburu'),
    ('Lokwawi', 'Pokot'), ('Lomuria', 'Pokot'), ('Lokitoe', 'Pokot'), ('Lopet', 'Pokot'),
    ('Saitoti', 'Maasai'), ('Nampaso', 'Maasai'), ('Sironka', 'Maasai'), ('Nkoitoi', 'Maasai'),
    ('Mwakio', 'Taita'), ('Mwasaru', 'Taita'), ('Mwamburi', 'Taita'), ('Mwang''ombe', 'Taita'),
    ('Muthuri', 'Embu'), ('Murithi', 'Embu'), ('Gicovi', 'Embu'), ('Kithinji', 'Embu'),
    ('Mugambi', 'Tharaka'), ('Gitari', 'Tharaka'), ('Gikundi', 'Tharaka'), ('Mugwika', 'Tharaka'),
    ('Wanjiru', 'Kikuyu'), ('Nyambura', 'Kikuyu'), ('Wambui', 'Kikuyu'), ('Njeri', 'Kikuyu'),
    ('Achieng', 'Luo'), ('Atieno', 'Luo'), ('Adhiambo', 'Luo'), ('Akinyi', 'Luo'),
    ('Nasimiyu', 'Luhya'), ('Nekesa', 'Luhya'), ('Namukhula', 'Luhya'), ('Nabwire', 'Luhya'),
    ('Chebet', 'Kalenjin'), ('Jepkosgei', 'Kalenjin'), ('Chepng''etich', 'Kalenjin'), ('Jepchirchir', 'Kalenjin'),
    ('Mutheu', 'Kamba'), ('Mwikali', 'Kamba'), ('Nduku', 'Kamba'), ('Kasyoka', 'Kamba'),
    ('Karegi', 'Meru'), ('Kinya', 'Meru'), ('Karimi', 'Meru'), ('Kanana', 'Meru'),
    ('Mwanasha', 'Mijikenda'), ('Charo', 'Mijikenda'), ('Kahindi', 'Mijikenda'), ('Mwakio', 'Mijikenda'),
    ('Nakiru', 'Turkana'), ('Nangiro', 'Turkana'), ('Nalimo', 'Turkana'), ('Nakure', 'Turkana'),
    ('Naserian', 'Samburu'), ('Nalangu', 'Samburu'), ('Nalotu', 'Samburu'), ('Nalepo', 'Samburu'),
    ('Nakoyo', 'Pokot'), ('Nacheke', 'Pokot'), ('Nang''iro', 'Pokot'), ('Nakarin', 'Pokot'),
    ('Naserian', 'Maasai'), ('Nalangu', 'Maasai'), ('Naini', 'Maasai'), ('Noolteti', 'Maasai'),
    ('Mwadime', 'Taita'), ('Mwakio', 'Taita'), ('Mwamburi', 'Taita'), ('Mwang''ombe', 'Taita'),
    ('Karimi', 'Embu'), ('Kanini', 'Embu'), ('Karwitha', 'Embu'), ('Karegi', 'Embu'),
    ('Gakii', 'Tharaka'), ('Gitari', 'Tharaka'), ('Gikundi', 'Tharaka'), ('Kagendo', 'Tharaka'),
    ('Fardowsa', 'Somali'), ('Habiba', 'Somali'), ('Sahra', 'Somali'), ('Asli', 'Somali'),
    ('Mwangi', 'Kikuyu'), ('Kamau', 'Kikuyu'), ('Njoroge', 'Kikuyu'), ('Maina', 'Kikuyu'),
    ('Ochieng', 'Luo'), ('Odhiambo', 'Luo'), ('Omondi', 'Luo'), ('Otieno', 'Luo'),
    ('Wekesa', 'Luhya'), ('Wafula', 'Luhya'), ('Shikuku', 'Luhya'), ('Namachanja', 'Luhya'),
    ('Kipchoge', 'Kalenjin'), ('Korir', 'Kalenjin'), ('Kiplagat', 'Kalenjin'), ('Kiprop', 'Kalenjin'),
    ('Musyoka', 'Kamba'), ('Mutua', 'Kamba'), ('Nzioka', 'Kamba'), ('Muthini', 'Kamba'),
    ('Mohamed', 'Muslim'), ('Abdullahi', 'Muslim'), ('Ali', 'Muslim'), ('Omar', 'Muslim'),
    ('Hassan', 'Muslim'), ('Ibrahim', 'Muslim'), ('Yusuf', 'Muslim'), ('Abdi', 'Muslim'),
    ('Mwirigi', 'Meru'), ('Kithinji', 'Meru'), ('Mugambi', 'Meru'), ('Kiraitu', 'Meru'),
    ('Kahindi', 'Mijikenda'), ('Katana', 'Mijikenda'), ('Karisa', 'Mijikenda'), ('Baya', 'Mijikenda'),
    ('Ekai', 'Turkana'), ('Lokale', 'Turkana'), ('Ewoi', 'Turkana'), ('Lowasa', 'Turkana'),
    ('Lenasalia', 'Samburu'), ('Leleina', 'Samburu'), ('Lekutan', 'Samburu'), ('Lentir', 'Samburu'),
    ('Lokwawi', 'Pokot'), ('Lomuria', 'Pokot'), ('Lokitoe', 'Pokot'), ('Lopet', 'Pokot'),
    ('Saitoti', 'Maasai'), ('Nampaso', 'Maasai'), ('Sironka', 'Maasai'), ('Nkoitoi', 'Maasai'),
    ('Mwakio', 'Taita'), ('Mwasaru', 'Taita'), ('Mwang''ombe', 'Taita'), ('Mwamburi', 'Taita'),
    ('Muthuri', 'Embu'), ('Murithi', 'Embu'), ('Gicovi', 'Embu'), ('Kithinji', 'Embu'),
    ('Mugambi', 'Tharaka'), ('Gitari', 'Tharaka'), ('Gikundi', 'Tharaka'), ('Mugwika', 'Tharaka'),
    ('Farah', 'Somali'), ('Aden', 'Somali'), ('Warsame', 'Somali'), ('Ismail', 'Somali') ]  # Keep your original names
loan_purposes = [ 'School Fees', 'Medical Expenses', 'Business Capital', 'Family Emergency',
    'Household Expenses', 'Rent', 'Debt Repayment', 'Holiday Spending', 'Other' ]  # Keep your original purposes
employment_statuses = [ 'Employed', 'Self-Employed', 'Unemployed', 'Student', 
    'Casual Worker', 'Business Owner', 'Other' ]  # Keep your original statuses
device_models = [ 'Samsung Galaxy A03', 'Tecno Spark 8', 'Infinix Hot 12', 'Nokia C21', 'Itel A58',
    'Samsung Galaxy A15', 'Tecno Camon 19', 'Infinix Note 12', 'Oppo A16', 'Redmi A3x' ]  # Keep your original models
os_versions = [ 'Android 10', 'Android 11', 'Android 12', 'Android 13', 
    'iOS 14', 'iOS 15', 'iOS 16' ]  # Keep your original versions
app_versions = [ '1.0.0', '1.1.0', '1.2.1', '1.3.0', 
    '2.0.0', '2.1.0', '2.2.0' ]  # Keep your original versions
payment_methods = [     'M-Pesa', 'Airtel Money', 'T-Kash', 'Equitel', 'Bank Transfer'
 ]  # Keep your original methods
age_distribution = [
    (18, 25, 0.32),   # 18-25 years: 32%
    (26, 35, 0.41),   # 26-35 years: 41%
    (36, 45, 0.18),   # 36-45 years: 18%
    (46, 65, 0.09)    # 46-65 years: 9%
]
employment_by_age = {
    (18, 25): [('Student', 40), ('Casual Worker', 30), ('Employed', 20), ('Unemployed', 10)],
    (26, 35): [('Employed', 45), ('Self-Employed', 30), ('Business Owner', 15), ('Unemployed', 10)],
    (36, 45): [('Employed', 50), ('Business Owner', 25), ('Self-Employed', 20), ('Unemployed', 5)],
    (46, 65): [('Self-Employed', 50), ('Business Owner', 30), ('Employed', 15), ('Unemployed', 5)]
}

# Helper functions (unchanged)
def weighted_choice(choices):
    total = sum(w for c, w in choices)
    r = random.uniform(0, total)
    upto = 0
    for c, w in choices:
        if upto + w >= r:
            return c
        upto += w
    return choices[-1][0]# Keep your original

def random_date(start_date, end_date): 
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    time_between = end_date - start_date
    if time_between.days <= 0:  # Same day or invalid range
        return start_date 
    random_days = random.randrange(time_between.days)
    return start_date + datetime.timedelta(days=random_days)# Keep your original

def random_time_on_date(date): 
    return date + datetime.timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )# Keep your original

def initialize_database():
    """Create tables and insert initial data"""
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            # Insert loan products if none exist (batch insert)
            cursor.execute("SELECT COUNT(*) FROM LoanProducts")
            if cursor.fetchone()[0] == 0:
                loan_products = [
                    ('First Time Loan', 'Personal', 100, 1000, 35.0, 5.0, 7, 7, 1, 1, 'Short-term loan for first-time borrowers'),
                    ('Quick Cash', 'Personal', 1000, 20000, 35.0, 2.5, 7, 14, 0, 1, 'Fast loan for existing customers'),
                    ('Emergency Loan', 'Emergency', 5000, 50000, 30.0, 2.0, 7, 30, 0, 1, 'Larger amounts for emergencies'),
                    ('Jipange Loan', 'Business', 10000, 100000, 25.0, 1.5, 7, 30, 0, 1, 'Flexible loan for financial planning')
                ]
                cursor.executemany("""
                    INSERT INTO LoanProducts (
                        ProductName, Category, MinAmount, MaxAmount, InterestRate, ProcessingFee, 
                        MinTermDays, MaxTermDays, IsFirstTime, CRBReporting, Description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, loan_products)
            
            
            conn.commit()
            print("Database initialized successfully")
        except Exception as e:
            conn.rollback()
            raise DatabaseError(f"Database initialization failed: {str(e)}")

def generate_customers(count, batch_size=1000):
    print(f"Starting to generate {count} customers with batch size {batch_size}")  
    """Generate customers with batch processing"""
    start_time = datetime.datetime.now()
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            # Verify Customers table exists
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers')
            BEGIN
                RAISERROR('Customers table does not exist', 16, 1)
            END
            """)
            
            # Get existing phone numbers and ID numbers to avoid duplicates
            cursor.execute("SELECT PhoneNumber, IDNumber FROM Customers")
            existing_data = cursor.fetchall()
            existing_phones = {row[0] for row in existing_data if row[0]}
            existing_ids = {row[1] for row in existing_data if row[1]}# Create weighted county list
            weighted_counties = [(county, data['weight']) for county, data in kenyan_counties.items()]
            
            customers = []
            credit_infos = []
            
            # Define initial values for new customers
            initial_loan_tier = 0
            initial_max_eligible_amount = 1000.00 
            initial_consecutive_repayments = 0
            
            for i in range(1, count + 1):
                # Basic demographics
                gender = random.choice(['M', 'F'])
                county = weighted_choice(weighted_counties)
                county_data = kenyan_counties[county]
                subcounty = random.choice(county_data['subcounties'])
                town = random.choice(county_data['towns'])
                is_urban = random.random() < kenyan_counties[county]['urban_ratio']

                
                # Name selection based on gender and origin
                if county in ['Mombasa', 'Kwale', 'Kilifi', 'Lamu', 'Garissa', 'Wajir', 'Mandera']:
                    # Higher probability of Muslim names in coastal and northeastern counties
                    if gender == 'M':
                        first_name = weighted_choice([
                            (name, 3 if origin == 'Muslim' else 1) 
                            for name, origin in male_first_names 
                            if origin in ['Common', 'Muslim']
                        ])
                    else:
                        first_name = weighted_choice([
                            (name, 3 if origin == 'Muslim' else 1) 
                            for name, origin in female_first_names 
                            if origin in ['Common', 'Muslim']
                        ])
                else:
                    if gender == 'M':
                        first_name = random.choice(male_first_names)[0]
                    else:
                        first_name = random.choice(female_first_names)[0]
                
                # Last name based on county/tribe
                if county in ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru']:
                    last_name = random.choice(last_names)[0]
                elif county in ['Kiambu', 'Murang\'a', 'Nyeri', 'Kirinyaga']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Kikuyu'])
                elif county in ['Siaya', 'Kisumu', 'Homa Bay', 'Migori']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Luo'])
                elif county in ['Mombasa', 'Kwale', 'Kilifi', 'Lamu', 'Garissa', 'Wajir', 'Mandera']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Muslim'])
                elif county in ['Mombasa', 'Kwale', 'Kilifi', 'Lamu', 'Garissa', 'Wajir', 'Mandera']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Somali'])
                elif county in ['Kakamega', 'Vihiga', 'Bungoma', 'Busia']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Luhya'])
                elif county in ['Uasin Gishu', 'Nandi', 'Elgeyo-Marakwet', 'Trans-Nzoia']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Kalenjin'])
                elif county in ['Kitui', 'Machakos', 'Makueni']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Kamba'])
                elif county in ['Mombasa', 'Kwale', 'Kilifi', 'Lamu']:
                    last_name = weighted_choice([(name, 1) for name, origin in last_names if origin == 'Mijikenda'])
                else:
                    last_name = random.choice(last_names)[0]
                
                # 1. Select age group using weighted choice
                selected_age_group = weighted_choice([((min_age, max_age), weight) for (min_age, max_age, weight) in age_distribution])
                min_age, max_age = selected_age_group  # Unpack the age range tuple

                # 2. Generate exact age (skewed younger within groups)
                age = min_age + int(random.triangular(0, max_age-min_age, (max_age-min_age)*0.3))
                dob = datetime.date.today() - datetime.timedelta(days=age*365 + random.randint(0, 364))

                # 3. Get employment status based on age group
                employment_options = employment_by_age[(min_age, max_age)]
                employment = weighted_choice(employment_options)

                # 4. Income distribution by age (in KES)
                if min_age < 25:  # 18-25 group
                    if employment in ['Employed', 'Business Owner']:
                        monthly_income = 15000 + random.random() * 35000  # Max ~50k
                    elif employment == 'Student':
                        monthly_income = 2000 + random.random() * 10000
                    else:
                        monthly_income = 5000 + random.random() * 15000
                elif min_age < 35:  # 26-35 group
                    if employment in ['Employed', 'Business Owner']:
                        # 80% below 50k, 20% above
                        if random.random() < 0.8:
                            monthly_income = 20000 + random.random() * 30000
                        else:
                            monthly_income = 50000 + random.random() * 100000
                    else:
                        monthly_income = 10000 + random.random() * 40000
                else:  # 36+ groups
                    if employment in ['Employed', 'Business Owner']:
                        # 70% below 50k, 30% above
                        if random.random() < 0.7:
                            monthly_income = 25000 + random.random() * 25000
                        else:
                            monthly_income = 50000 + random.random() * 150000
                    else:
                        monthly_income = 15000 + random.random() * 60000
                        
                # Mobile money usage intensity
                mobile_volume_multiplier = {
                    18: 0.8,   # Younger users transact more frequently
                    25: 1.0,
                    35: 0.9,
                    45: 0.7,
                    65: 0.5
                }
                age_key = min([k for k in mobile_volume_multiplier.keys() if k >= age])
                mobile_volume = monthly_income * (0.2 + random.random() * 0.4) * mobile_volume_multiplier[age_key]
                
                # Adjust income and loan behavior based on urban/rural
                if is_urban:
                    if employment in ['Employed', 'Business Owner']:
                        monthly_income = 30000 + random.random() * 200000  # Higher urban incomes
                else:
                    if employment in ['Employed', 'Business Owner']:
                        monthly_income = 15000 + random.random() * 80000  # Lower rural incomes
                
                # Education level
                education = weighted_choice([
                    ('Primary', 15),
                    ('Secondary', 30),
                    ('College', 30),
                    ('University', 25)
                ])
                
                # Marital status based on age
                if age < 20:
                    marital_status = 'Single'
                elif age < 30:
                    marital_status = 'Single' if random.random() < 0.6 else 'Married'
                elif age < 50:
                    r = random.random()
                    marital_status = 'Single' if r < 0.3 else ('Married' if r < 0.9 else 'Divorced')
                else:
                    r = random.random()
                    marital_status = 'Married' if r < 0.7 else ('Widowed' if r < 0.9 else 'Divorced')
                
                # Mobile provider by region
                if county in ['Nairobi', 'Central Kenya']:
                    mobile_provider = weighted_choice([
                        ('M-Pesa', 85),  # Safaricom dominance
                        ('Airtel Money', 12),
                        ('T-Kash', 3)
                    ])
                elif county in ['Western', 'Nyanza']:
                    mobile_provider = weighted_choice([
                        ('M-Pesa', 80),
                        ('Airtel Money', 18),  # Slightly higher Airtel penetration
                        ('T-Kash', 2)
                    ])
                elif county in ['Coastal', 'North Eastern']:
                    mobile_provider = weighted_choice([
                        ('M-Pesa', 75),
                        ('Airtel Money', 20),  # Higher Airtel presence
                        ('T-Kash', 5)
                    ])
                else:
                    mobile_provider = weighted_choice([
                        ('M-Pesa', 80),
                        ('Airtel Money', 15),
                        ('T-Kash', 5)
                    ])
                
                mobile_volume = 0 if monthly_income == 0 else monthly_income * (0.1 + random.random() * 0.5)
                
                # Phone number and ID
                while True:
                    phone_number = '07' + ''.join([str(random.randint(0, 9)) for _ in range(8)])
                    if phone_number not in existing_phones:
                        existing_phones.add(phone_number)
                        break
                # Generate unique ID number (for adults)
                id_number = None
                if age >= 18:
                    while True:
                        id_number = str(random.randint(2, 4)) + ''.join([str(random.randint(0, 9)) for _ in range(6)]) + str(random.randint(0, 9))
                        if id_number not in existing_ids:
                            existing_ids.add(id_number)
                            break
                
                # Registration date (last 3 years)
                registration_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 1095))
                last_active = registration_date + datetime.timedelta(days=random.randint(0, 30))
                is_active = random.random() > 0.2  # 80% active
                
                # Convert data types for SQL Server
                is_active_int = 1 if is_active else 0
                dob_str = dob.strftime('%Y-%m-%d')  # Convert date to string
                registration_date_str = registration_date.strftime('%Y-%m-%d %H:%M:%S')
                last_active_str = last_active.strftime('%Y-%m-%d %H:%M:%S')
                
                # Add to batch
                customers.append((
                    first_name, last_name, phone_number, id_number, dob_str, gender,
                    county, subcounty, town, employment, float(monthly_income),
                    education, marital_status, mobile_provider,
                    float(mobile_volume), registration_date_str, last_active_str, is_active_int
                ))
                
                # Prepare credit info
                credit_score = 300 + int(random.triangular(0, 400, 180))
                if monthly_income > 50000:
                    credit_score = min(850, credit_score + 100)
                    
                credit_infos.append((
                    None,
                    credit_score,                                       # 2
                    70 + random.randint(0, 30),                         # 3: PaymentHistoryScore
                    Decimal(str(round(random.uniform(0.05, 0.6), 2))),  # 4: CreditUtilization
                    0,                                                  # 5: CreditHistoryLength (starts at 0)
                    50 + random.randint(0, 50),                         # 6: CreditMixScore
                    0,                                                  # 7: RecentInquiries (starts at 0)
                    0,                                                  # 8: TotalLoansTaken
                    Decimal('0.00'),                                    # 9: TotalAmountBorrowed
                    Decimal('0.00'),                                    # 10: TotalAmountRepaid
                    0,                                                  # 11: ActiveLoans
                    Decimal('0.00'),                                    # 12: ActiveLoanAmount
                    0,                                                  # 13: TimesDefaulted
                    None,                                               # 14: LastDefaultDate
                    None,                                               # 15: DaysSinceLastDefault
                    0,                                                  # 16: CRBListed (False)
                    None,                                               # 17: CRBListingDate
                    None,                                               # 18: CRBListingType
                    random.randint(50, 90),                             # 19: MobileMoneyRepaymentHistory (example score)
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),# 20: LastUpdated
                    0,                                                  # 21: TimesOverdrafted
                    Decimal('0.00'),                                    # 22: TotalOverdraftFees
                    Decimal(str(round(max(500.0, 5000.0 * (credit_score / 700.0)), 2))), # 23: OverdraftLimit
                    # --- New Tiering Fields ---
                    initial_loan_tier,                                  # 24: CurrentLoanTier
                    initial_max_eligible_amount,                        # 25: MaxEligibleLoanAmount (already Decimal)
                    initial_consecutive_repayments                      # 26: ConsecutiveOnTimeRepayments
                ))
                
                # Commit in batches
                if i % batch_size == 0 or i == count:
                    # Insert customers
                    cursor.executemany("""
                        INSERT INTO Customers (
                            FirstName, LastName, PhoneNumber, IDNumber, DateOfBirth, Gender,
                            County, SubCounty, Town, EmploymentStatus, MonthlyIncome,
                            EducationLevel, MaritalStatus, MobileMoneyProvider,
                            MonthlyMobileMoneyVolume, RegistrationDate, LastActiveDate, IsActive
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, customers)
                    
                    # Get the generated customer IDs
                    cursor.execute("SELECT CustomerID FROM Customers WHERE CustomerID > (SELECT ISNULL(MAX(CustomerID), 0) FROM Customers) - ?", (len(customers),))
                    customer_ids = [row[0] for row in cursor.fetchall()]
                    
                    # Update credit info with customer IDs
                    for j, customer_id in enumerate(customer_ids):
                        credit_infos[len(credit_infos) - len(customers) + j] = (
                            customer_id, *credit_infos[len(credit_infos) - len(customers) + j][1:]
                        )
                    
                    # Insert credit info
                    cursor.executemany("""
                        INSERT INTO CustomerCreditInfo (
                            CustomerID, CreditScore, PaymentHistoryScore, CreditUtilization,
                            CreditHistoryLength, CreditMixScore, RecentInquiries,
                            TotalLoansTaken, TotalAmountBorrowed, TotalAmountRepaid,
                            ActiveLoans, ActiveLoanAmount, TimesDefaulted, LastDefaultDate, DaysSinceLastDefault,
                            CRBListed, CRBListingDate, CRBListingType, MobileMoneyRepaymentHistory, LastUpdated,
                            TimesOverdrafted, TotalOverdraftFees, OverdraftLimit,
                            CurrentLoanTier, MaxEligibleLoanAmount, ConsecutiveOnTimeRepayments
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, credit_infos[len(credit_infos) - len(customers):])
                    
                    conn.commit()
                    show_progress(i, count, start_time)
                    customers = []
            
            print(f"\nSuccessfully generated {count} customers")
        except Exception as e:
            conn.rollback()
            raise GenerationError(f"Error generating customers: {str(e)}")

def generate_device_info():
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(CustomerID) FROM Customers")
            total_customers = cursor.fetchone()[0]
            
            cursor.execute("SELECT CustomerID, RegistrationDate FROM Customers")
            customers = cursor.fetchall()
            
            start_time = datetime.datetime.now()
            print(f"Generating device info for {total_customers} customers...")
            
            for i, (customer_id, reg_date) in enumerate(customers, 1):
                device_model = random.choice(device_models)
                
                if 'iPhone' in device_model:
                    os_version = random.choice([v for v in os_versions if 'iOS' in v])
                else:
                    os_version = random.choice([v for v in os_versions if 'Android' in v])
                
                app_version = random.choice(app_versions)
                last_seen = reg_date + datetime.timedelta(days=random.randint(0, 30))
                
                cursor.execute("""
                    INSERT INTO CustomerDeviceInfo (
                        CustomerID, DeviceModel, OSVersion, AppVersion,
                        FirstSeenDate, LastSeenDate
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (customer_id, device_model, os_version, app_version, reg_date, last_seen))
                
                # Show progress every 100 records or at the end
                if i % 100 == 0 or i == total_customers:
                    show_progress(i, total_customers, start_time, "Devices: ")
            
            conn.commit()
            print(f"Generated device info for {total_customers} customers")
        except Exception as e:
            conn.rollback()
            raise GenerationError(f"Error generating device info: {str(e)}")

def generate_mobile_money_transactions(customer_id, months_back=24, transaction_intensity=3):
    conn = None
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
        
            start_date = datetime.datetime.now() - datetime.timedelta(days=730)  # 2 years back
            end_date = datetime.datetime.now() - datetime.timedelta(days=365)    # 1 year back
            current_date = start_date
            
            # Get customer details
            cursor.execute("""
                SELECT MobileMoneyProvider, MonthlyMobileMoneyVolume, County, EmploymentStatus
                FROM Customers WHERE CustomerID = ?
            """, customer_id)
            provider, monthly_volume, county, employment = cursor.fetchone()
            
            cursor.execute("SELECT CreditScore, OverdraftLimit FROM CustomerCreditInfo WHERE CustomerID = ?", customer_id)
            credit_score, overdraft_limit = cursor.fetchone()
            
            # Convert decimal values to float for calculations
            monthly_volume = float(monthly_volume) if monthly_volume else 0.0
            overdraft_limit = float(overdraft_limit) if overdraft_limit else 0.0
            
            # Adjust overdraft limit based on credit score
            overdraft_limit = max(0, min(
                overdraft_limit * (credit_score / 700),
                20000  # Max overdraft limit
            ))
            
            # Determine if urban
            is_urban = county in ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru'] or (
                county in ['Kiambu', 'Machakos', 'Uasin Gishu'] and random.random() > 0.3
            )
            
            # Base transaction values
            base_daily = transaction_intensity
            base_amount = monthly_volume / (100 * base_daily) if monthly_volume > 0 else 0
            
            # Track overdraft usage
            total_overdraft_fees = 0.0
            times_overdrafted = 0
            
            # Generate daily transactions
            while current_date <= end_date:
                # Adjust for day of week
                daily_multiplier = 0.7 if current_date.weekday() in [5, 6] else 1.0
                
                # More activity at month end
                if current_date.day >= 25:
                    daily_multiplier *= 1.5
                
                # Calculate transactions for this day
                daily_transactions = math.ceil(base_daily * daily_multiplier * (0.8 + random.random() * 0.4))
                
                # Starting balance
                balance = 500.0 + random.random() * 5000.0
                daily_overdraft_fees = 0.0
                
                while daily_transactions > 0 and monthly_volume > 0:
                    # Determine transaction type
                    if employment == 'Employed' and current_date.day >= 25 and random.random() > 0.7:
                        trans_type = 'Deposit'  # Salary deposit
                    else:
                        trans_type = weighted_choice([
                            ('Payment', 40),
                            ('Transfer', 30),
                            ('Withdrawal', 30)
                        ])
                    
                    # Generate amount based on type
                    if trans_type == 'Deposit':
                        amount = 10000.0 + random.random() * 50000.0 if employment == 'Employed' else 500.0 + random.random() * 5000.0
                    elif trans_type == 'Withdrawal':
                        amount = 200.0 + random.random() * 3000.0 if is_urban else 100.0 + random.random() * 2000.0
                    else:  # Payment or transfer
                        amount = 50.0 + random.random() * 5000.0 if trans_type == 'Payment' else 100.0 + random.random() * 3000.0
                    
                    # Adjust amount to customer's typical volume
                    amount *= (0.8 + random.random() * 0.4)
                    
                    # Generate counterparty
                    if trans_type == 'Payment':
                        counterparty = weighted_choice([
                            ('Utility Company', 30),
                            ('School Fees', 25),
                            ('Online Shopping', 20),
                            ('Merchant Payment', 25)
                        ])
                    elif trans_type == 'Transfer':
                        counterparty = weighted_choice([
                            ('Family Member', 40),
                            ('Business Partner', 30),
                            ('Friend', 30)
                        ])
                    elif trans_type == 'Withdrawal':
                        counterparty = 'Agent'
                    else:  # Deposit
                        counterparty = 'Employer' if employment == 'Employed' else 'Bank Account'
                    
                    # Generate random hour (more during daytime)
                    hour = weighted_choice([
                        (7 + random.randint(0, 3), 30),  # Morning (7-10am)
                        (11 + random.randint(0, 3), 25),  # Midday (11am-2pm)
                        (15 + random.randint(0, 4), 25),  # Afternoon (3-7pm)
                        (20 + random.randint(0, 3), 20)   # Evening (8-11pm)
                    ])
                    
                    # Process transaction with overdraft
                    is_overdraft = False
                    overdraft_fee = 0.0
                    
                    if trans_type in ['Deposit', 'Transfer']:
                        balance += amount
                    else:  # Withdrawal or Payment
                        if (balance - amount) >= -overdraft_limit:
                            # Check for overdraft
                            if (balance - amount) < 0:
                                is_overdraft = True
                                overdraft_amount = abs(balance - amount)
                                overdraft_fee = overdraft_amount * 0.05  # 5% fee
                                daily_overdraft_fees += overdraft_fee
                                times_overdrafted += 1
                            
                            balance -= amount
                        else:
                            # Reduce amount to available balance + overdraft
                            amount = balance + overdraft_limit
                            
                            if amount > 0:
                                is_overdraft = True
                                overdraft_amount = abs(balance - amount)
                                overdraft_fee = overdraft_amount * 0.05
                                daily_overdraft_fees += overdraft_fee
                                times_overdrafted += 1
                                balance -= amount
                            else:
                                # Skip transaction if no available balance
                                daily_transactions -= 1
                                continue
                    
                    # Insert transaction
                    if amount > 0:
                        trans_date = current_date.replace(
                            hour=hour,
                            minute=random.randint(0, 59),
                            second=random.randint(0, 59)
                        )
                        
                        reference = f"{provider[:3]}_{trans_type[:2]}_{random.randint(100000000, 999999999)}"
                        
                        cursor.execute("""
                            INSERT INTO MobileMoneyTransactions (
                                CustomerID, TransactionDate, TransactionType, Amount,
                                Balance, Counterparty, Reference, IsOverdraft, OverdraftFee
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            customer_id, trans_date, trans_type, float(amount),
                            float(balance), counterparty, reference, is_overdraft, float(overdraft_fee)
                        ))
                    
                    daily_transactions -= 1
                
                total_overdraft_fees += daily_overdraft_fees
                current_date += datetime.timedelta(days=1)
            
            # Update overdraft info in credit record
            cursor.execute("""
                UPDATE CustomerCreditInfo
                SET TimesOverdrafted = TimesOverdrafted + ?,
                    TotalOverdraftFees = TotalOverdraftFees + ?,
                    LastUpdated = ?
                WHERE CustomerID = ?
            """, (times_overdrafted, total_overdraft_fees, datetime.datetime.now(), customer_id))
        
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise

def generate_loan_applications(start_date, end_date, apps_per_day):
    # Add this check to ensure reasonable volumes (optional, keep if you like)
    # if apps_per_day > 3000000: ...

    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            total_days = (end_date - start_date).days
            if total_days < 0: total_days = 0 # Handle edge case where end_date < start_date
            total_expected = total_days * apps_per_day
            print(f"Generating loan applications for {total_days} days (~{total_expected} applications)...")
            start_time = datetime.datetime.now()
            apps_generated = 0
            days_processed = 0

            # --- (Keep initial checks for customers/products) ---
            cursor.execute("SELECT COUNT(*) FROM Customers")
            if cursor.fetchone()[0] == 0: raise ValueError("No customers found")
            cursor.execute("SELECT COUNT(*) FROM LoanProducts")
            if cursor.fetchone()[0] == 0: raise ValueError("No loan products found")
            cursor.execute("SELECT c.CustomerID, DATEDIFF(YEAR, c.DateOfBirth, GETDATE()) AS age, c.County FROM Customers c")
            rows = cursor.fetchall(); customers = [(row[0], row[1], row[2]) for row in rows] if rows else []
            cursor.execute("SELECT ProductID FROM LoanProducts")
            product_ids = [row[0] for row in cursor.fetchall()]
            if not customers or not product_ids: raise GenerationError("No customers or products found")
            # ---

            current_date = start_date
            # <<<--- START Generation Loop ---<<<
            while current_date <= end_date:
                # Calculate daily apps volume
                daily_apps = apps_per_day
                if current_date.weekday() >= 5: daily_apps = max(1, int(apps_per_day * random.uniform(0.8, 0.95)))
                if current_date.day >= 25: daily_apps = int(daily_apps * 1.3)
                if current_date.month == 1: daily_apps = int(daily_apps * 1.4)
                elif current_date.month == 12: daily_apps = int(daily_apps * 1.2)

                apps_for_this_day = [] # Temporary list for daily batching (optional optimization)

                for _ in range(daily_apps):
                    try:
                        customer_id, age, county = random.choice(customers)

                        # Check if first time borrower
                        cursor.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM LoanApplications la JOIN Loans l ON la.ApplicationID = l.ApplicationID WHERE la.CustomerID = ? AND la.Status = 'Approved') THEN 0 ELSE 1 END", customer_id)
                        is_first_time = cursor.fetchone()[0]

                        # --- Fetch current loan eligibility and other credit info ---
                        cursor.execute("""
                            SELECT CreditScore, ActiveLoans, CurrentLoanTier, MaxEligibleLoanAmount, ConsecutiveOnTimeRepayments 
                            FROM CustomerCreditInfo WHERE CustomerID = ?
                        """, customer_id)
                        credit_info_row = cursor.fetchone()

                        if not credit_info_row: # Should not happen if customers are generated with credit info
                            print(f"Warning: No CustomerCreditInfo found for CustomerID {customer_id}. Skipping application.")
                            continue

                        credit_score, active_loans, current_loan_tier, max_eligible_loan_amount, consecutive_on_time_repayments = credit_info_row
                        credit_score = credit_score if credit_score else 400 # Default if NULL
                        active_loans = active_loans if active_loans else 0   # Default if NULL
                        current_loan_tier = current_loan_tier if current_loan_tier else 0
                        max_eligible_loan_amount = float(max_eligible_loan_amount if max_eligible_loan_amount else 1000.00)
                        consecutive_on_time_repayments = consecutive_on_time_repayments if consecutive_on_time_repayments else 0
                        
                        # --- Select a suitable loan product ---
                        # For tier 0, you might still want to restrict to the "First Time Loan" or similar low-value products
                        if current_loan_tier == 0:
                            cursor.execute("""
                                SELECT ProductID, MinAmount, MaxAmount, MinTermDays, MaxTermDays, ProcessingFee, InterestRate 
                                FROM LoanProducts 
                                WHERE ProductName = 'First Time Loan' AND MaxAmount <= ? 
                                ORDER BY NEWID()
                            """, (max_eligible_loan_amount,)) # Ensure even first time loan product is within their absolute max
                        else:
                            # For higher tiers, allow other products, ensuring the product's MinAmount is not above their MaxEligible
                            cursor.execute("""
                                SELECT ProductID, MinAmount, MaxAmount, MinTermDays, MaxTermDays, ProcessingFee, InterestRate 
                                FROM LoanProducts 
                                WHERE IsFirstTime = 0 AND MinAmount <= ? 
                                ORDER BY MaxAmount DESC, NEWID() -- Prioritize products they might qualify for
                            """, (max_eligible_loan_amount,))

                        product_row = cursor.fetchone()
                        if not product_row:
                            # Fallback: If no specific product matches, try a very basic one if they are tier 0, or skip
                            if current_loan_tier == 0:
                                cursor.execute("""
                                    SELECT ProductID, MinAmount, MaxAmount, MinTermDays, MaxTermDays, ProcessingFee, InterestRate 
                                    FROM LoanProducts 
                                    WHERE ProductName = 'First Time Loan'
                                    ORDER BY NEWID()
                                """)
                                product_row = cursor.fetchone()
                            if not product_row:
                                # print(f"Debug: No suitable product for Cust {customer_id}, Tier {current_loan_tier}, MaxElig {max_eligible_loan_amount}")
                                continue # Skip if no suitable product found

                        product_id, min_amount_prod, max_amount_prod, min_term, max_term, proc_fee_pct, int_rate = product_row
                        min_amount_prod = float(min_amount_prod)
                        max_amount_prod = float(max_amount_prod)

                        # --- (Get product details, calculate amount, term, purpose - Keep existing logic) ---
                        cursor.execute("SELECT MinAmount, MaxAmount, MinTermDays, MaxTermDays, ProcessingFee, InterestRate FROM LoanProducts WHERE ProductID = ?", product_id)
                        product_details = cursor.fetchone()
                        if not product_details: continue
                        min_amount, max_amount, min_term, max_term, proc_fee_pct, int_rate = product_details
                        min_amount=float(min_amount); max_amount=float(max_amount); proc_fee_pct=float(proc_fee_pct); int_rate=float(int_rate)
                       
                        # Effective maximum for this application is the lower of product's max and customer's eligible max
                        effective_application_max_amount = min(max_amount_prod, max_eligible_loan_amount)

                        if min_amount_prod > effective_application_max_amount:
                            # This means the customer's MaxEligibleLoanAmount is too low for even the MinAmount of the selected product.
                            # This logic might need refinement based on how products are selected above.
                            # print(f"Debug: Cust {customer_id} MaxEligible {max_eligible_loan_amount} too low for Product {product_id} MinAmount {min_amount_prod}")
                            continue 

                        # Generate amount, ensuring it's between product's min and the effective_application_max_amount
                        amount = (min_amount_prod + math.sqrt(random.random()) * (effective_application_max_amount - min_amount_prod))

                        # Loan amount/term adjustments by age/region/etc. (Keep existing logic but ensure 'amount' respects new cap)
                        amount_multiplier = 1.0 # From existing code
                        if age < 25: amount_multiplier = 0.7
                        elif age >= 35: amount_multiplier = 1.2
                        
                        if county in ['Nairobi', 'Mombasa']: amount *= 1.2
                        elif county in ['Garissa', 'Wajir', 'Mandera']: amount *= 0.7
                        
                        amount *= amount_multiplier # Apply existing multipliers

                        # Ensure amount is within the final calculated bounds and also product's original bounds (redundant if effective_application_max_amount used correctly)
                        amount = max(min_amount_prod, min(amount, effective_application_max_amount))
                        amount = math.floor(amount / 100) * 100 # Round down to nearest 100
                        
                        term = min_term if current_loan_tier == 0 else min_term + random.randint(0, max(0, max_term - min_term))
                        term_multiplier = 1.0 

                        # Purpose selection (Keep existing logic)
                        purpose = random.choice(loan_purposes) # Basic choice
                        if age < 25: purpose = weighted_choice([('School Fees', 50),('Business Capital', 20),('Holiday Spending', 15),('Other', 15)])
                        elif age < 35: purpose = weighted_choice([('Business Capital', 40),('Household Expenses', 25),('Rent', 20),('Other', 15)])
                        else: purpose = weighted_choice([('Medical Expenses', 35),('Family Emergency', 30),('Business Capital', 20),('Other', 15)])
                        if current_date.month in [1, 5, 9] and random.random() < 0.5: purpose = 'School Fees'
                        elif current_date.month in [4, 10] and random.random() < 0.3: purpose = 'Business Capital'
                        elif current_date.month == 12 and random.random() < 0.4: purpose = 'Holiday Spending'
                        
                        term = int(term * term_multiplier)
                        term = max(min_term, min(term, max_term))

                        # --- (Approval logic - Keep existing logic) ---
                        cursor.execute("SELECT CreditScore, ActiveLoans FROM CustomerCreditInfo WHERE CustomerID = ?", customer_id)
                        credit_info = cursor.fetchone(); credit_score=500; active_loans=0 # Defaults
                        if credit_info: credit_score, active_loans = (credit_info[0] or 500), (credit_info[1] or 0)
                        approval_prob = 0.95
                        if credit_score <= 400: approval_prob *= 0.7
                        elif credit_score <= 500: approval_prob *= 0.85
                        elif credit_score <= 600: approval_prob *= 0.95
                        if active_loans > 0: approval_prob *= max(0.5, 1 - (active_loans * 0.05))
                        approval_prob = max(0.1, min(0.95, approval_prob))
                        status = 'Approved' if random.random() <= approval_prob else 'Rejected'
                        # ---

                        # --- (Set dates, reason, device, IP - Keep existing logic) ---
                        app_time = current_date + datetime.timedelta(seconds=random.randint(0, 86399))
                        status_time = app_time + datetime.timedelta(minutes=random.randint(0, 120))
                        rejection_reason = None
                        if status == 'Rejected': rejection_reason = random.choice(['Insufficient Credit History', 'High Default Risk', 'Incomplete Information'])
                        device = random.choice(device_models); ip_address = f"197.156.{random.randint(0, 255)}.{random.randint(0, 255)}"
                        # ---

                        # Insert application and get ID
                        cursor.execute(""" INSERT INTO LoanApplications (CustomerID, ProductID, ApplicationDate, AmountRequested, TermDays, Purpose, Status, StatusDate, RejectionReason, DeviceUsed, IPAddress) OUTPUT INSERTED.ApplicationID VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """, (customer_id, product_id, app_time, float(amount), term, purpose, status, status_time, rejection_reason, device, ip_address))
                        app_id_row = cursor.fetchone()
                        if app_id_row is None: raise GenerationError(f"Failed to retrieve ApplicationID after insert for Customer {customer_id}")
                        app_id = app_id_row[0]

                        # If approved, create loan and update credit info
                        if status == 'Approved':
                            if is_first_time: proc_fee = float(amount) * 0.05
                            else: proc_fee = float(amount) * (proc_fee_pct / 100)
                            interest = float(amount) * (int_rate / 100) * (float(term) / 30.0)
                            total_repayable = float(amount) + float(interest) + float(proc_fee)

                            cursor.execute(""" INSERT INTO Loans (ApplicationID, DisbursementDate, PrincipalAmount, InterestAmount, ProcessingFee, TotalRepayable, DueDate, Status) VALUES (?, ?, ?, ?, ?, ?, ?, ?) """, (app_id, status_time, float(amount), float(interest), float(proc_fee), float(total_repayable), status_time + datetime.timedelta(days=term), 'Active'))

                            # Update credit info (using correct float conversions)
                            principal_float = float(amount) # Use consistent variable
                            cursor.execute("""
                                UPDATE CustomerCreditInfo
                                SET TotalLoansTaken = ISNULL(TotalLoansTaken, 0) + 1,
                                    TotalAmountBorrowed = ISNULL(TotalAmountBorrowed, 0) + ?,
                                    ActiveLoans = ISNULL(ActiveLoans, 0) + 1,
                                    ActiveLoanAmount = ISNULL(ActiveLoanAmount, 0) + ?,
                                    -- CreditUtilization might be better calculated separately or based on a limit
                                    RecentInquiries = ISNULL(RecentInquiries, 0) + 1,
                                    CreditHistoryLength = ISNULL(CreditHistoryLength, 0) + 1, -- Increment a simple counter for months/loans
                                    LastUpdated = ?
                                WHERE CustomerID = ?
                            """, (principal_float, principal_float, datetime.datetime.now(), customer_id))
                            # --- REMOVED 불필요한 커서 재할당, 커밋, 앱 취소 로직 ---

                        apps_generated += 1

                    except Exception as e:
                        # Log error but continue generating for the day
                        print(f"\nError processing one application on {current_date.date()}: {str(e)}. Skipping.")
                        # Maybe add more robust error tracking here if needed
                        conn.rollback() # Rollback the failed transaction for this app
                        continue # Continue to next application for the day

                # --- Progress Reporting (moved outside inner loop) ---
                days_processed += 1
                if days_processed % 5 == 0 or current_date == end_date or days_processed == 1:
                    elapsed = (datetime.datetime.now() - start_time).total_seconds()
                    apps_per_sec = apps_generated / elapsed if elapsed > 0 else 0
                    days_remaining = total_days - days_processed
                    if apps_per_sec > 0:
                         # Estimate remaining time based on apps per second and expected apps per day
                         remaining_apps = days_remaining * apps_per_day
                         eta_seconds = remaining_apps / apps_per_sec if apps_per_sec > 0 else 0
                         eta_str = f"ETA: {eta_seconds/60:.1f} min"
                    else:
                         eta_str = "ETA: Calculating..."

                    print(f"\rDays: {days_processed}/{total_days} | Apps Generated: {apps_generated} | Rate: {apps_per_sec:.1f}/sec | {eta_str}   ", end="")

                current_date += datetime.timedelta(days=1)
            # <<<--- END Generation Loop ---<<<

            print(f"\nFinished generating applications. Total generated: {apps_generated}")

            # --- Application Cancellation (Moved Here) ---
            print("Cleaning up old pending applications...")
            cursor.execute(""" UPDATE LoanApplications SET Status = 'Cancelled' WHERE Status = 'Pending' AND ApplicationDate < DATEADD(day, -7, GETDATE()) """)
            print(f"Cancelled {cursor.rowcount} old pending applications.")
            # ---

            # --- Final Commit ---
            print("Committing generated applications...")
            conn.commit()
            print(f"Successfully generated and committed {apps_generated} applications over {total_days} days.")

        except Exception as e:
            print(f"\nCritical error during application generation: {str(e)}")
            conn.rollback()
            import traceback
            traceback.print_exc()
            raise GenerationError(f"Loan application generation failed: {str(e)}")                 

def validate_decimal(value, max_digits=30, decimal_places=2):
    """Strict decimal validation with proper rounding and bounds checking"""
    try:
        if not isinstance(value, (Decimal, int, float)):
            raise ValueError("Value must be numeric")
            
        # Convert to Decimal with high precision
        decimal_value = Decimal(str(value)).quantize(
            Decimal('0.01'), 
            rounding=ROUND_HALF_UP
        )
        
        # Check against maximum allowed value
        max_value = Decimal('9' * (max_digits - decimal_places)) + (
            Decimal('0.9') * decimal_places
        )
        
        if decimal_value > max_value:
            raise ValueError(f"Value {decimal_value} exceeds maximum allowed {max_value}")
            
        return decimal_value
    except Exception as e:
        raise ValueError(f"Decimal validation failed: {str(e)}")
            
def generate_repayments():
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            config = load_config()
            # --- (Keep data integrity check and initial loan fetching) ---
            cursor.execute(""" SELECT COUNT(*) FROM Loans l LEFT JOIN LoanApplications la ON l.ApplicationID = la.ApplicationID WHERE la.ApplicationID IS NULL """)
            if cursor.fetchone()[0] > 0: raise GenerationError("Some loans are missing application data")
            cursor.execute(""" SELECT COUNT(*) FROM Loans l WHERE l.DueDate BETWEEN DATEADD(month, -12, GETDATE()) AND DATEADD(day, 30, GETDATE()) AND l.Status IN ('Active', 'Defaulted') """)
            total_loans_to_process = cursor.fetchone()[0]
            print(f"Generating repayment outcomes for {total_loans_to_process} due/past-due loans...")
            start_time = datetime.datetime.now()
            cursor.execute(""" SELECT l.LoanID, l.DueDate, l.TotalRepayable, l.PrincipalAmount, la.CustomerID, lp.Category, cci.CreditScore, cci.TimesDefaulted, DATEDIFF(YEAR, c.DateOfBirth, GETDATE()) as age, c.MonthlyIncome FROM Loans l JOIN LoanApplications la ON l.ApplicationID = la.ApplicationID JOIN LoanProducts lp ON la.ProductID = lp.ProductID JOIN CustomerCreditInfo cci ON la.CustomerID = cci.CustomerID JOIN Customers c ON la.CustomerID = c.CustomerID WHERE l.DueDate BETWEEN DATEADD(month, -12, GETDATE()) AND DATEADD(day, 30, GETDATE()) AND l.Status IN ('Active', 'Defaulted') """)
            loans = cursor.fetchall()

            repayments_to_insert = []
            loans_to_update = []
            credit_updates = []
            crb_updates = []
            payment_methods = ["M-Pesa", "Airtel Money", "T-Kash", "Bank Transfer", "Cash"]

            if not loans: print("No active or defaulted loans found due between 12 months ago and 30 days from now.")
            else:
                 if len(loans[0]) != 10: raise GenerationError(f"Expected 10 columns for main loan query, got {len(loans[0])}")

            # <<<--- START OF MAIN LOOP ---<<<
            for i, (loan_id, due_date, total_repayable, principal_amount_loan, customer_id, category,
                   credit_score, times_defaulted, age, monthly_income) in enumerate(loans, 1):

                # --- (Keep probability calculations) ---
                total_repayable_float = float(total_repayable) if total_repayable else 0.0
                monthly_income_float = float(monthly_income) if monthly_income else 0.0
                principal_loan_float = float(principal_amount_loan) if principal_amount_loan else 0.0

                repayment_prob = 0.85
                current_credit_score = credit_score if credit_score else 500
                current_times_defaulted = times_defaulted if times_defaulted else 0
                if age < 25: repayment_prob *= 0.85; late_prob = 0.7
                elif age < 35: repayment_prob *= 0.95; late_prob = 0.5
                else: repayment_prob *= 1.1; late_prob = 0.2
                if monthly_income_float > 50000: repayment_prob = min(0.95, repayment_prob * 1.2); late_prob *= 0.8
                cursor.execute("SELECT TimesOverdrafted FROM CustomerCreditInfo WHERE CustomerID = ?", customer_id)
                overdraft_res = cursor.fetchone(); overdraft_count = overdraft_res[0] if overdraft_res else 0
                if overdraft_count > 3: repayment_prob *= 0.8; late_prob *= 1.3
                if category == 'Business': repayment_prob *= 1.1
                elif category == 'Agricultural': repayment_prob *= 0.9
                if current_credit_score > 700: repayment_prob *= 1.1
                elif current_credit_score > 600: repayment_prob *= 1.0
                elif current_credit_score > 500: repayment_prob *= 0.9
                elif current_credit_score > 400: repayment_prob *= 0.8
                else: repayment_prob *= 0.6
                if current_times_defaulted > 0: repayment_prob *= (0.9 ** current_times_defaulted)
                repayment_prob = max(0.05, min(0.95, repayment_prob))
                late_prob = max(0.1, min(0.9, late_prob))

                # --- Repayment/Default Simulation ---
                if random.random() <= repayment_prob: # Repaid
                    days_early = 0; days_paid_early = 0
                    if random.random() <= late_prob: # Late
                        days_late = random.randint(1, 60); repayment_date = due_date + datetime.timedelta(days=days_late); late_fee = total_repayable_float * 0.015 * days_late
                    else: # On time/earlyprincipal_amount
                        days_early = random.randint(0, 7); days_late = 0; repayment_date = due_date - datetime.timedelta(days=days_early); late_fee = 0.0
                        days_paid_early = days_early
                    repayment_date = min(repayment_date, datetime.datetime.now())
                    repayment_date = repayment_date.replace(hour=random.randint(0, 23), minute=random.randint(0, 59))
                    if age < 30 or random.random() < 0.3: payment_type = weighted_choice([('full', 60), ('partial', 40)])
                    else: payment_type = 'full'
                    payment_method = random.choice(payment_methods)

                    if payment_type == 'partial': # Partial
                        amount_paid = total_repayable_float * (0.3 + random.random() * 0.6); status = 'Defaulted'
                        repayments_to_insert.append((loan_id, repayment_date, decimal.Decimal(str(amount_paid)), payment_method, f"MPESA{random.randint(100000000, 999999999)}", days_late > 0, decimal.Decimal(str(late_fee))))
                        loans_to_update.append((status, repayment_date, days_late, loan_id))
                        # --- 'partial' tuple creation (4 elements) ---
                        credit_updates.append(('partial', customer_id, amount_paid, datetime.datetime.now()))
                    else: # Full
                        amount_paid = total_repayable_float + late_fee; status = 'Paid'
                        repayments_to_insert.append((loan_id, repayment_date, decimal.Decimal(str(amount_paid)), payment_method, f"MPESA{random.randint(100000000, 999999999)}", days_late > 0, decimal.Decimal(str(late_fee))))
                        loans_to_update.append((status, repayment_date, days_late if days_late > 0 else -days_early, loan_id))
                        # --- 'success' tuple creation (6 elements) ---
                        # Uses total_repayable_float as the principal amount for reduction
                        credit_updates.append(('success', customer_id, days_early, amount_paid, principal_loan_float, datetime.datetime.now()))
                else: # Defaulted
                    days_delayed = (datetime.datetime.now() - due_date).days
                    if days_delayed > 0:
                        loans_to_update.append(('Defaulted', None, days_delayed, loan_id))
                        # --- 'default' tuple creation (4 elements - unchanged) ---
                        credit_updates.append(('default', customer_id, datetime.datetime.now(), datetime.datetime.now()))
                        if random.random() > 0.5:
                            listing_type = 'Major Default' if total_repayable_float > 1000 else 'Minor Default'
                            # --- 'crb' tuple creation (3 elements - unchanged) ---
                            crb_updates.append((datetime.datetime.now(), listing_type, customer_id))

                if i % 50 == 0 or i == len(loans):
                    show_progress(i, len(loans), start_time, "Due Repayments: ")
            # <<<--- END OF MAIN LOOP ---<<<


            # <<<--- START OF EARLY REPAYMENT BLOCK ---<<<
            print("\nGenerating early repayments...")
            cursor.execute(""" SELECT l.LoanID, l.DueDate, l.TotalRepayable, la.CustomerID FROM Loans l JOIN LoanApplications la ON l.ApplicationID = la.ApplicationID WHERE l.DueDate >= GETDATE() AND l.Status = 'Active' AND DATEDIFF(DAY, GETDATE(), l.DueDate) BETWEEN 1 AND 30 """)
            early_loans = cursor.fetchall()
            early_repayments_generated = 0
            for loan_data in early_loans:
                if random.random() < 0.1:
                    early_loan_id, early_due_date, early_total_repayable, early_customer_id = loan_data
                    if early_due_date is None or early_total_repayable is None: continue
                    early_total_repayable_float = float(early_total_repayable)
                    days_until_due = (early_due_date - datetime.datetime.now()).days
                    if days_until_due <= 0: continue
                    days_paid_early = random.randint(1, days_until_due)
                    repayment_date = early_due_date - datetime.timedelta(days=days_paid_early)
                    repayment_date = repayment_date.replace(hour=random.randint(0, 23), minute=random.randint(0, 59))
                    repayments_to_insert.append((early_loan_id, repayment_date, decimal.Decimal(str(early_total_repayable_float)), random.choice(payment_methods), f"MPESA{random.randint(100000000, 999999999)}", 0, 0.0))
                    loans_to_update.append(('Paid', repayment_date, -days_paid_early, early_loan_id))
                    # --- 'success' tuple creation (6 elements) ---
                    # Uses early_total_repayable_float as the principal amount for reduction
                    credit_updates.append(('success', early_customer_id, days_paid_early, early_total_repayable_float, early_total_repayable_float, datetime.datetime.now()))
                    early_repayments_generated += 1
            # <<<--- END OF EARLY REPAYMENT LOOP ---<<<

            print(f"\nFinished generating {len(loans)} due loan outcomes (plus {early_repayments_generated} potential early repayments).")


            # --- BATCH EXECUTION (using combined lists) ---
            if repayments_to_insert:
                 print(f"Inserting {len(repayments_to_insert)} repayments...")
                 cursor.executemany("""INSERT INTO Repayments (LoanID, RepaymentDate, Amount, PaymentMethod, TransactionReference, IsLate, LateFee) VALUES (?, ?, ?, ?, ?, ?, ?)""", repayments_to_insert)
            if loans_to_update:
                print(f"Updating {len(loans_to_update)} loans...")
                unique_loan_updates = {loan_id: (status, date, delay, loan_id) for status, date, delay, loan_id in loans_to_update}
                params_list = list(unique_loan_updates.values())
                if params_list:
                    cursor.executemany("""UPDATE Loans SET Status = ?, LastPaymentDate = ?, DaysDelayed = ? WHERE LoanID = ?""", params_list)

            # --- Process credit updates ---
            print(f"Processing {len(credit_updates)} credit updates...")
            for update in credit_updates:
                update_type = update[0]
                try:
                    if update_type == 'success':
                        # --- Check for length 6 ---
                        if len(update) != 6:
                            print(f"Skipping success update due to incorrect length: {len(update)}")
                            continue
                        # --- Unpack 5 values after type ---
                        _, cust_id, days_early_val, amt_paid, principal_amt, ts = update
                        
                        # (Keep the SQL logic, it uses the correct variables now)
                        cursor.execute("SELECT ActiveLoanAmount FROM CustomerCreditInfo WHERE CustomerID = ?", cust_id)
                        active_loan_amt_before_res = cursor.fetchone()
                        active_loan_amt_before = float(active_loan_amt_before_res[0]) if active_loan_amt_before_res and active_loan_amt_before_res[0] else 0.0
                        util_reduction = (float(principal_amt) / active_loan_amt_before * 100) if active_loan_amt_before > 0 else 0
                        cursor.execute("SELECT DATEDIFF(day, LastDefaultDate, GETDATE()) FROM CustomerCreditInfo WHERE CustomerID = ? AND LastDefaultDate IS NOT NULL", cust_id)
                        days_since_res = cursor.fetchone()
                        days_since_val = days_since_res[0] if days_since_res else None
                        payment_hist_increase = 10 if days_early_val > 0 else 5
                        cursor.execute("""UPDATE CustomerCreditInfo SET PaymentHistoryScore = LEAST(100, PaymentHistoryScore + ?), CreditUtilization = GREATEST(0, CreditUtilization - ?), DaysSinceLastDefault = ?, TotalAmountRepaid = TotalAmountRepaid + ?, ActiveLoans = GREATEST(0, ActiveLoans - 1), ActiveLoanAmount = GREATEST(0, ActiveLoanAmount - ?), LastUpdated = ? WHERE CustomerID = ? """, (payment_hist_increase, util_reduction, days_since_val, float(amt_paid), float(principal_amt), ts, cust_id))
                        
                        if days_early_val >= 0: # Paid on time or early (days_early_val is positive for early, 0 for on-time in your setup)
                            cursor.execute("""
                                SELECT CurrentLoanTier, MaxEligibleLoanAmount, ConsecutiveOnTimeRepayments
                                FROM CustomerCreditInfo WHERE CustomerID = ?
                            """, cust_id)
                            tier_info = cursor.fetchone()
                            if tier_info:
                                current_tier_db, current_max_eligible_db, consecutive_repayments_db = tier_info
                                current_tier_db = current_tier_db if current_tier_db is not None else 0
                                current_max_eligible_db = float(current_max_eligible_db if current_max_eligible_db is not None else 500.00)
                                consecutive_repayments_db = consecutive_repayments_db if consecutive_repayments_db is not None else 0

                                new_consecutive_repayments = consecutive_repayments_db + 1

                                new_tier = current_tier_db
                                new_max_eligible = current_max_eligible_db
                                
                                tier_upgrade_thresh = config['generation']['tier_upgrade_threshold']
                                max_possible_tier = config['generation']['max_tier']
                                increase_factor = config['generation']['tier_amount_multiplier']
                                increase_flat = config['generation']['tier_amount_increment']
                                abs_max_loan_amount = config['generation']['absolute_max_loan_amount']


                                if new_consecutive_repayments >= tier_upgrade_thresh and \
                                current_tier_db < max_possible_tier :
                                    new_tier += 1
                                    new_max_eligible = current_max_eligible_db * increase_factor + (increase_flat * new_tier)
                                    new_max_eligible = min(new_max_eligible, abs_max_loan_amount)
                                    new_consecutive_repayments = 0

                                # Update CustomerCreditInfo with new tier, max_eligible_amount, and consecutive_repayments
                                cursor.execute("""
                                    UPDATE CustomerCreditInfo 
                                    SET CurrentLoanTier = ?, MaxEligibleLoanAmount = ?, ConsecutiveOnTimeRepayments = ?,
                                        PaymentHistoryScore = LEAST(100, PaymentHistoryScore + ?), -- Existing update
                                        TotalAmountRepaid = ISNULL(TotalAmountRepaid,0) + ?,      -- Existing update
                                        ActiveLoans = GREATEST(0, ActiveLoans - 1),              -- Existing update
                                        ActiveLoanAmount = GREATEST(0, ActiveLoanAmount - ?),    -- Existing update
                                        LastUpdated = ?                                          -- Existing update
                                    WHERE CustomerID = ?
                                """, (
                                    new_tier, Decimal(str(round(new_max_eligible,2))), new_consecutive_repayments,
                                    (10 if days_early_val > 0 else 5), # payment_hist_increase
                                    Decimal(str(amt_paid)), 
                                    Decimal(str(principal_amt)), 
                                    ts, 
                                    cust_id
                                ))
                        else: # Paid late (days_early_val is negative, representing days_late)
                            # Reset consecutive on-time payments, potentially penalize tier/limit slightly
                            cursor.execute("""
                                UPDATE CustomerCreditInfo 
                                SET ConsecutiveOnTimeRepayments = 0,
                                    PaymentHistoryScore = GREATEST(0, PaymentHistoryScore - 5), -- Existing update for late
                                    TotalAmountRepaid = ISNULL(TotalAmountRepaid,0) + ?,      -- Existing update
                                    ActiveLoans = GREATEST(0, ActiveLoans - 1),              -- Existing update
                                    ActiveLoanAmount = GREATEST(0, ActiveLoanAmount - ?),    -- Existing update
                                    LastUpdated = ?                                          -- Existing update
                                WHERE CustomerID = ?
                            """, (
                                Decimal(str(amt_paid)), 
                                Decimal(str(principal_amt)), 
                                ts, 
                                cust_id
                            ))
                    elif update_type == 'default':
                         # Check for length 4 (unchanged)
                         if len(update) != 4: print(f"Skipping default update due to incorrect length: {len(update)}"); continue
                         _, cust_id, default_dt, update_dt = update
                         # Reset tier progression significantly on default
                         cursor.execute("""
                            UPDATE CustomerCreditInfo 
                            SET CurrentLoanTier = 0, MaxEligibleLoanAmount = ?, ConsecutiveOnTimeRepayments = 0,
                                TimesDefaulted = ISNULL(TimesDefaulted, 0) + 1, 
                                LastDefaultDate = ?, 
                                DaysSinceLastDefault = 0, 
                                PaymentHistoryScore = GREATEST(0, PaymentHistoryScore - 15), -- Existing penalty
                                LastUpdated = ?
                                -- If 'partial_hist', also update TotalAmountRepaid if applicable from your existing logic
                            WHERE CustomerID = ?
                         """, (
                            Decimal(str(config['generation']['initial_max_eligible_amount'])),
                            (update[2] if update_type == 'default_hist' else update[1]), # default_date or lastdefaultdate from tuple
                            (update[3] if update_type == 'default_hist' else update[2]), # last_updated_date or ts
                            cust_id
                         ))
                         
                    elif update_type == 'partial':
                         # --- Check for length 4 ---
                         if len(update) != 4:
                             print(f"Skipping partial update due to incorrect length: {len(update)}")
                             continue
                         # --- Unpack 3 values after type ---
                         _, cust_id, amt_paid, ts = update
                         # (Keep the SQL logic, it uses the correct variables now)
                         cursor.execute("""UPDATE CustomerCreditInfo SET PaymentHistoryScore = GREATEST(0, PaymentHistoryScore - 5), TotalAmountRepaid = TotalAmountRepaid + ?, TimesDefaulted = TimesDefaulted + 1, LastDefaultDate = ?, DaysSinceLastDefault = 0, LastUpdated = ? WHERE CustomerID = ?""", (float(amt_paid), ts, ts, cust_id))
                except Exception as credit_e:
                    print(f"\nError processing credit update for CustomerID {update[1]} ({update_type}): {credit_e}")


            # --- (Keep CRB update processing logic - unchanged) ---
            if crb_updates:
                 print(f"Processing {len(crb_updates)} CRB updates...")
                 unique_crb_updates = {(cust_id): (date, type, cust_id) for date, type, cust_id in crb_updates}
                 crb_params_list = list(unique_crb_updates.values())
                 if crb_params_list:
                     if crb_params_list and len(crb_params_list[0]) != 3: print(f"Skipping CRB update due to incorrect tuple length: {len(crb_params_list[0])}")
                     else: cursor.executemany("""UPDATE CustomerCreditInfo SET CRBListed = 1, CRBListingDate = ?, CRBListingType = ?, CreditScore = GREATEST(300, CreditScore - 100) WHERE CustomerID = ? AND CRBListed = 0 """, crb_params_list)

            conn.commit()
            print(f"\nFinished generating current repayments.")

        # --- (Keep except and finally blocks) ---
        except Exception as e:
            conn.rollback()
            import traceback
            print(f"\nError generating repayments: {str(e)}")
            traceback.print_exc()
            raise GenerationError(f"Error generating repayments: {str(e)}")
   
def generate_historical_repayments(months_back=12):
    """Generate repayments for loans that were due in the past"""
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            config_data = load_config()
            gen_config = config_data['generation']

            # Use a consistent "now" for date calculations to avoid shifts during long runs
            # This should ideally align with how 'now' is determined in main() for date setups
            simulation_current_time = datetime.datetime.now()
            overall_end_date_hist = simulation_current_time.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
            overall_start_date_hist = overall_end_date_hist - datetime.timedelta(days=30 * months_back)
            overall_start_date_hist = overall_start_date_hist.replace(day=1)

            print(f"\nGenerating historical repayments from {overall_start_date_hist.date()} to {overall_end_date_hist.date()}...")

            all_repayments_to_insert_hist = []
            all_loans_to_update_hist = []
            all_credit_events_hist = []
            # temp_crb_updates_from_hist_aggregation is defined later, so all_crb_updates_hist isn't strictly needed here

            current_processing_month_start = overall_start_date_hist
            while current_processing_month_start <= overall_end_date_hist:
                current_processing_month_end = (current_processing_month_start + datetime.timedelta(days=31)).replace(day=1) - datetime.timedelta(days=1)
                current_processing_month_end = min(current_processing_month_end, overall_end_date_hist)

                print(f"\nProcessing historical repayments for month: {current_processing_month_start.strftime('%Y-%m')}...")

                # Fetch active loans DUE in this specific historical month
                # Added ISNULL for TimesOverdrafted from CustomerCreditInfo
                cursor.execute("""
                    SELECT
                        l.LoanID, l.DueDate, l.PrincipalAmount, l.TotalRepayable, l.Status as LoanStatus,
                        la.CustomerID, lp.Category AS LoanCategory, lp.InterestRate, lp.ProcessingFee,
                        c.MonthlyIncome, DATEDIFF(YEAR, c.DateOfBirth, l.DueDate) as age_at_due_date,
                        cci.CreditScore AS credit_score_at_due_time,
                        ISNULL(cci.TimesDefaulted, 0) AS times_defaulted_at_due_time,
                        ISNULL(cci.TimesOverdrafted, 0) AS times_overdrafted_at_due_time
                    FROM Loans l
                    JOIN LoanApplications la ON l.ApplicationID = la.ApplicationID
                    JOIN LoanProducts lp ON la.ProductID = lp.ProductID
                    JOIN Customers c ON la.CustomerID = c.CustomerID
                    LEFT JOIN CustomerCreditInfo cci ON la.CustomerID = cci.CustomerID
                    WHERE l.DueDate BETWEEN ? AND ? AND l.Status = 'Active'
                    ORDER BY l.DueDate, la.CustomerID
                """, (current_processing_month_start, current_processing_month_end.replace(hour=23, minute=59, second=59)))

                loans_due_this_month = cursor.fetchall()
                if not loans_due_this_month:
                    print(f"No active loans found due in {current_processing_month_start.strftime('%Y-%m')}")
                    year = current_processing_month_start.year
                    month = current_processing_month_start.month
                    if month == 12: year += 1; month = 1
                    else: month += 1
                    current_processing_month_start = current_processing_month_start.replace(year=year, month=month, day=1)
                    continue

                for loan_data_tuple in loans_due_this_month:
                    (loan_id, due_date, principal_amount_loan, total_repayable_loan, _,
                     customer_id, category_loan, interest_rate_loan, processing_fee_loan,
                     monthly_income_cust, age_at_due_date_cust,
                     credit_score_at_due, times_defaulted_at_due,
                     times_overdrafted_at_due) = loan_data_tuple

                    simulated_credit_score = credit_score_at_due if credit_score_at_due is not None else 500
                    # times_defaulted_at_due and times_overdrafted_at_due are already handled with ISNULL in SQL

                    total_repayable_float = float(total_repayable_loan if total_repayable_loan is not None else 0.0)
                    monthly_income_float = float(monthly_income_cust if monthly_income_cust is not None else 0.0)

                    # --- REVISED Repayment Probability (to mirror generate_repayments) ---
                    base_repayment_prob = 0.85  # Base probability (as in generate_repayments)
                    base_late_prob = 0.3       # Base late probability (can be fine-tuned)
                    
                    if times_defaulted_at_due == 0 and simulated_credit_score < 500: # First loan for a modest-score customer
                            base_repayment_prob = 0.90 # Slightly higher chance for first success
                            base_late_prob = 0.25    # Slightly lower chance of being late

                    repayment_prob = base_repayment_prob
                    late_prob = base_late_prob
                    
                    # Age factor
                    if age_at_due_date_cust < 25:
                        repayment_prob *= 0.85; late_prob = 0.7
                    elif age_at_due_date_cust < 35:
                        repayment_prob *= 0.95; late_prob = 0.5
                    else:  # age >= 35
                        repayment_prob *= 1.1; late_prob = 0.2

                    # Monthly income factor
                    if monthly_income_float > 50000:
                        repayment_prob = min(0.95, repayment_prob * 1.2)
                        late_prob *= 0.8
                    
                    # Times Overdrafted factor
                    if times_overdrafted_at_due > 3: # Using the fetched historical overdraft count
                        repayment_prob *= 0.8
                        late_prob *= 1.3
                    
                    # Loan Category factor
                    if category_loan == 'Business':
                        repayment_prob *= 1.1
                    elif category_loan == 'Agricultural':
                        repayment_prob *= 0.9

                    # Credit Score factor
                    if simulated_credit_score > 700: repayment_prob *= 1.1
                    elif simulated_credit_score > 600: repayment_prob *= 1.0 # No change
                    elif simulated_credit_score > 500: repayment_prob *= 0.9
                    elif simulated_credit_score > 400: repayment_prob *= 0.8
                    else: repayment_prob *= 0.6 # For scores <= 400

                    # Clamp probabilities
                    repayment_prob = max(0.05, min(0.95, repayment_prob))
                    late_prob = max(0.1, min(0.9, late_prob))
                    # --- END REVISED Repayment Probability ---

                    days_early_or_late_val = 0
                    simulated_repayment_date = due_date

                    if random.random() <= repayment_prob: # Loan is Repaid
                        loan_status_final = 'Paid'
                        late_fee_hist = Decimal("0.0")

                        if random.random() <= late_prob: # Repaid LATE
                            days_late_hist = random.randint(1, 60)
                            simulated_repayment_date = due_date + datetime.timedelta(days=days_late_hist)
                            late_fee_hist = Decimal(str(round(total_repayable_float * 0.015 * days_late_hist, 2)))
                            days_early_or_late_val = -days_late_hist
                        else: # Repaid ON TIME or EARLY
                            days_early_hist = random.randint(0, int(gen_config.get('max_days_early_payment_hist', 7))) # Use gen_config
                            simulated_repayment_date = due_date - datetime.timedelta(days=days_early_hist)
                            days_early_or_late_val = days_early_hist
                        
                        simulated_repayment_date = min(simulated_repayment_date, overall_end_date_hist, simulation_current_time)
                        
                        amount_paid_hist = Decimal(str(round(total_repayable_float,2))) + late_fee_hist
                        payment_type_hist = 'full'
                        if age_at_due_date_cust < 30 and random.random() < float(gen_config.get('historical_partial_payment_prob', 0.2)): # Use gen_config
                            payment_type_hist = 'partial'
                            amount_paid_hist = Decimal(str(round(total_repayable_float * random.uniform(0.3, 0.7), 2)))
                            loan_status_final = 'Defaulted'

                        all_repayments_to_insert_hist.append((
                            loan_id, simulated_repayment_date, amount_paid_hist,
                            random.choice(payment_methods), f"MPESA_HIST_{random.randint(1000000,9999999)}",
                            (days_early_or_late_val < 0), late_fee_hist
                        ))
                        all_loans_to_update_hist.append((loan_status_final, simulated_repayment_date, days_early_or_late_val, loan_id))
                        
                        event_principal_repaid = float(principal_amount_loan if principal_amount_loan is not None else 0.0)
                        event_amount_repaid = float(amount_paid_hist - late_fee_hist)

                        if payment_type_hist == 'full':
                            all_credit_events_hist.append({
                                'type': 'success_hist', 'cust_id': customer_id, 
                                'event_date': simulated_repayment_date, 
                                'amount_repaid': event_amount_repaid,
                                'principal_repaid': event_principal_repaid,
                                'days_early_or_late': days_early_or_late_val
                            })
                        else: # Partial
                             all_credit_events_hist.append({
                                'type': 'partial_hist', 'cust_id': customer_id,
                                'event_date': simulated_repayment_date,
                                'amount_repaid': float(amount_paid_hist), # For partial, amount_repaid is just what was paid
                                'principal_repaid': event_principal_repaid, # Could be less than total principal for partial
                                'days_early_or_late': days_early_or_late_val
                            })
                    else: # Loan is Defaulted
                        loan_status_final = 'Defaulted'
                        simulated_default_processing_date = min(due_date + datetime.timedelta(days=random.randint(1, 30)), overall_end_date_hist)
                        days_delayed_for_loan_rec = (overall_end_date_hist - due_date).days
                        if days_delayed_for_loan_rec < 0: days_delayed_for_loan_rec = 0

                        all_loans_to_update_hist.append((loan_status_final, None, days_delayed_for_loan_rec, loan_id))
                        all_credit_events_hist.append({
                            'type': 'default_hist', 'cust_id': customer_id,
                            'event_date': simulated_default_processing_date,
                            'due_date': due_date,
                            'total_repayable': total_repayable_float,
                            'principal_repaid': float(principal_amount_loan if principal_amount_loan is not None else 0.0) # Add defaulted principal
                        })

                # Monthly commits for Repayments and Loans tables
                if all_repayments_to_insert_hist:
                    cursor.executemany("""INSERT INTO Repayments (LoanID, RepaymentDate, Amount, PaymentMethod, TransactionReference, IsLate, LateFee) VALUES (?, ?, ?, ?, ?, ?, ?)""", all_repayments_to_insert_hist)
                if all_loans_to_update_hist:
                    unique_loan_updates_month = {lid: (st, dt, dl, lid) for st, dt, dl, lid in all_loans_to_update_hist}
                    if unique_loan_updates_month:
                        cursor.executemany("""UPDATE Loans SET Status = ?, LastPaymentDate = ?, DaysDelayed = ? WHERE LoanID = ? AND Status = 'Active'""", list(unique_loan_updates_month.values()))
                conn.commit()
                all_repayments_to_insert_hist.clear()
                all_loans_to_update_hist.clear()

                year = current_processing_month_start.year
                month = current_processing_month_start.month
                if month == 12: year += 1; month = 1
                else: month += 1
                current_processing_month_start = current_processing_month_start.replace(year=year, month=month, day=1)

            # <<< --- FINAL AGGREGATED CREDITINFO UPDATES (after all historical months are processed) --- >>>
            print(f"\nProcessed {len(all_credit_events_hist)} historical credit events total.")
            print(f"Applying aggregated historical CustomerCreditInfo updates...")

            involved_customer_ids_final = list(set(event['cust_id'] for event in all_credit_events_hist))
            initial_cci_states = {}

            if involved_customer_ids_final:
                # Batching the query to avoid too many parameters
                # SQL Server's limit is 2100 parameters. Batch size is set lower for safety.
                batch_query_size = 1000
                for i in range(0, len(involved_customer_ids_final), batch_query_size):
                    batch_customer_ids = involved_customer_ids_final[i:i + batch_query_size]

                    # This check should ideally not be needed if involved_customer_ids_final is already checked
                    # but added for safety within the loop, though an empty batch_customer_ids
                    # given involved_customer_ids_final is not empty and batch_query_size > 0
                    # implies len(involved_customer_ids_final) was 0 initially.
                    if not batch_customer_ids:
                        continue

                    placeholders = ','.join(['?'] * len(batch_customer_ids))
                    query_string = f"""
                        SELECT CustomerID, CreditScore, PaymentHistoryScore, CreditUtilization, CreditHistoryLength,
                               TotalLoansTaken, TotalAmountBorrowed, TotalAmountRepaid, ActiveLoans, ActiveLoanAmount,
                               TimesDefaulted, LastDefaultDate, DaysSinceLastDefault, CRBListed, CRBListingDate, CRBListingType,
                               CurrentLoanTier, MaxEligibleLoanAmount, ConsecutiveOnTimeRepayments, OverdraftLimit, LastUpdated
                        FROM CustomerCreditInfo WHERE CustomerID IN ({placeholders})
                    """
                    try:
                        # The line number from the traceback (e.g., 1928) would correspond to this execute call
                        cursor.execute(query_string, tuple(batch_customer_ids))
                        for row_data in cursor.fetchall():
                            initial_cci_states[row_data[0]] = {
                                'CreditScore': row_data[1] if row_data[1] is not None else 500,
                                'PaymentHistoryScore': row_data[2] if row_data[2] is not None else 70,
                                'CreditUtilization': row_data[3],
                                'CreditHistoryLength': row_data[4] if row_data[4] is not None else 0,
                                'TotalLoansTaken': row_data[5] if row_data[5] is not None else 0,
                                'TotalAmountBorrowed': Decimal(row_data[6] if row_data[6] is not None else 0.0),
                                'TotalAmountRepaid': Decimal(row_data[7] if row_data[7] is not None else 0.0),
                                'ActiveLoans': row_data[8] if row_data[8] is not None else 0,
                                'ActiveLoanAmount': Decimal(row_data[9] if row_data[9] is not None else 0.0),
                                'TimesDefaulted': row_data[10] if row_data[10] is not None else 0,
                                'LastDefaultDate': row_data[11],
                                'DaysSinceLastDefault': row_data[12],
                                'CRBListed': row_data[13] if row_data[13] is not None else 0,
                                'CRBListingDate': row_data[14],
                                'CRBListingType': row_data[15],
                                'CurrentLoanTier': row_data[16] if row_data[16] is not None else 0,
                                'MaxEligibleLoanAmount': Decimal(row_data[17] if row_data[17] is not None else gen_config['initial_max_eligible_amount']),
                                'ConsecutiveOnTimeRepayments': row_data[18] if row_data[18] is not None else 0,
                                'OverdraftLimit': Decimal(row_data[19] if row_data[19] is not None else 5000.0),
                                'LastUpdated': row_data[20]
                            }
                    except pyodbc.Error as e_detail:
                        print(f"Error during batched CustomerCreditInfo fetch for batch starting at index {i}: {e_detail}")
                        print(f"Query that failed: {query_string}")
                        print(f"Number of params in this batch: {len(batch_customer_ids)}")
                        # It's often useful to see the first few IDs in the problematic batch
                        if batch_customer_ids:
                            print(f"First few IDs in batch: {batch_customer_ids[:5]}")
                        raise # Re-raise the error to halt execution and indicate a persistent issue.
            # Continue with customer_final_states logic...
            # Continue with customer_final_states logic, which will now correctly handle an empty initial_cci_states if no customers were involved.

            customer_final_states = {}
            temp_crb_updates_from_hist_aggregation = []

            for event in all_credit_events_hist:
                cust_id = event['cust_id']
                if cust_id not in customer_final_states:
                    if cust_id in initial_cci_states:
                        customer_final_states[cust_id] = initial_cci_states[cust_id].copy()
                    else:
                        customer_final_states[cust_id] = {
                            'CreditScore': 500, 'PaymentHistoryScore': 70, 'CreditUtilization': Decimal('0.0'), 'CreditHistoryLength': 0,
                            'TotalLoansTaken': 0, 'TotalAmountBorrowed': Decimal('0.0'), 'TotalAmountRepaid': Decimal('0.0'),
                            'ActiveLoans': 0, 'ActiveLoanAmount': Decimal('0.0'), 'TimesDefaulted': 0, 'LastDefaultDate': None,
                            'DaysSinceLastDefault': None, 'CRBListed': 0, 'CRBListingDate': None, 'CRBListingType': None,
                            'CurrentLoanTier': 0, 'MaxEligibleLoanAmount': Decimal(gen_config['initial_max_eligible_amount']),
                            'ConsecutiveOnTimeRepayments': 0, 'OverdraftLimit': Decimal('5000.0'), 
                            'LastUpdated': overall_start_date_hist 
                        }

                state = customer_final_states[cust_id]
                event_date = event['event_date']
                
                # Ensure state['LastUpdated'] is a datetime object for comparison
                # This logic should ideally ensure LastUpdated is always a datetime object when fetched or initialized
                current_last_updated = state['LastUpdated']
                if isinstance(current_last_updated, str):
                    try:
                        current_last_updated = datetime.datetime.strptime(current_last_updated, '%Y-%m-%d %H:%M:%S.%f') # Try with microseconds
                    except ValueError:
                        try:
                            current_last_updated = datetime.datetime.strptime(current_last_updated, '%Y-%m-%d %H:%M:%S') # Try without
                        except ValueError:
                             current_last_updated = None # Or some other default datetime if parsing fails
                
                state['LastUpdated'] = max(current_last_updated or event_date, event_date)


                tier_upgrade_thresh = gen_config['tier_upgrade_threshold']
                max_possible_tier = gen_config['max_tier']
                increase_factor = gen_config['tier_amount_multiplier']
                increase_flat = gen_config['tier_amount_increment']
                abs_max_loan_amount = gen_config['absolute_max_loan_amount']
                initial_max_eligible = Decimal(gen_config['initial_max_eligible_amount'])
                crb_listing_threshold_days_conf = int(gen_config.get('crb_listing_threshold_days', 90))
                crb_major_default_threshold_conf = float(gen_config.get('crb_major_default_threshold', 10000.0))

                if event['type'] == 'success_hist':
                    state['TotalAmountRepaid'] += Decimal(str(event['amount_repaid']))
                    state['ActiveLoans'] = max(0, state.get('ActiveLoans',0) - 1) # Use .get with default
                    state['ActiveLoanAmount'] = max(Decimal('0.0'), Decimal(str(state.get('ActiveLoanAmount', '0.0'))) - Decimal(str(event['principal_repaid'])))
                    
                    days_early_or_late = event['days_early_or_late']
                    if days_early_or_late >= 0: 
                        state['ConsecutiveOnTimeRepayments'] = state.get('ConsecutiveOnTimeRepayments',0) + 1
                        state['PaymentHistoryScore'] = min(100, state.get('PaymentHistoryScore',70) + (10 if days_early_or_late > 0 else 5))
                        if state['ConsecutiveOnTimeRepayments'] >= tier_upgrade_thresh and state.get('CurrentLoanTier',0) < max_possible_tier:
                            state['CurrentLoanTier'] = state.get('CurrentLoanTier',0) + 1
                            current_mel_decimal = Decimal(str(state.get('MaxEligibleLoanAmount', initial_max_eligible)))
                            new_max_el = current_mel_decimal * Decimal(str(increase_factor)) + (Decimal(str(increase_flat)) * Decimal(state['CurrentLoanTier']))
                            state['MaxEligibleLoanAmount'] = min(new_max_el, Decimal(str(abs_max_loan_amount)))
                            state['ConsecutiveOnTimeRepayments'] = 0
                    else: 
                        state['ConsecutiveOnTimeRepayments'] = 0
                        state['PaymentHistoryScore'] = max(0, state.get('PaymentHistoryScore',70) - 2)
                
                elif event['type'] == 'partial_hist':
                    state['TotalAmountRepaid'] += Decimal(str(event['amount_repaid']))
                    state['ActiveLoans'] = max(0, state.get('ActiveLoans',0) - 1) # Or handle active loan amount reduction differently
                    state['ActiveLoanAmount'] = max(Decimal('0.0'), Decimal(str(state.get('ActiveLoanAmount', '0.0'))) - Decimal(str(event['amount_repaid']))) # Reduce by amount paid

                    state['ConsecutiveOnTimeRepayments'] = 0
                    state['TimesDefaulted'] = state.get('TimesDefaulted',0) + 1
                    state['LastDefaultDate'] = max(state.get('LastDefaultDate', None) or event_date, event_date)
                    state['CurrentLoanTier'] = 0 
                    state['MaxEligibleLoanAmount'] = initial_max_eligible
                    state['PaymentHistoryScore'] = max(0, state.get('PaymentHistoryScore',70) - 5)

                elif event['type'] == 'default_hist':
                    state['TimesDefaulted'] = state.get('TimesDefaulted',0) + 1
                    state['LastDefaultDate'] = max(state.get('LastDefaultDate', None) or event_date, event_date)
                    state['ConsecutiveOnTimeRepayments'] = 0
                    state['CurrentLoanTier'] = 0
                    state['MaxEligibleLoanAmount'] = initial_max_eligible
                    state['PaymentHistoryScore'] = max(0, state.get('PaymentHistoryScore',70) - 15)
                    state['ActiveLoans'] = max(0, state.get('ActiveLoans',0) - 1)
                    # Assume the full principal of defaulted loan is no longer "active"
                    state['ActiveLoanAmount'] = max(Decimal('0.0'), Decimal(str(state.get('ActiveLoanAmount', '0.0'))) - Decimal(str(event['principal_repaid'])))


                    original_due_date = event['due_date']
                    total_repayable_at_default = event['total_repayable']
                    simulated_crb_listing_date = original_due_date + datetime.timedelta(days=crb_listing_threshold_days_conf + 1)
                    simulated_crb_listing_date = min(simulated_crb_listing_date, overall_end_date_hist, simulation_current_time)

                    if not state.get('CRBListed', False) or (state.get('CRBListingDate', None) and simulated_crb_listing_date > state['CRBListingDate']):
                        if (event['event_date'] - original_due_date).days >= crb_listing_threshold_days_conf:
                            listing_type = 'Major Default' if total_repayable_at_default > crb_major_default_threshold_conf else 'Minor Default'
                            temp_crb_updates_from_hist_aggregation.append((simulated_crb_listing_date, listing_type, cust_id))
                            state['CRBListed'] = 1
                            state['CRBListingDate'] = simulated_crb_listing_date
                            state['CRBListingType'] = listing_type
                            state['CreditScore'] = max(300, state.get('CreditScore',500) - 50)
            
            # Prepare batch update for CustomerCreditInfo
            final_cci_update_params = []
            for cust_id, state in customer_final_states.items():
                # ... (formatting for final_cci_update_params, ensure all keys exist in state using .get()) ...
                days_since_default_val = None
                last_default_dt_state = state.get('LastDefaultDate')
                last_updated_dt_state = state.get('LastUpdated')

                if last_default_dt_state and last_updated_dt_state:
                    # Ensure they are datetime objects
                    if isinstance(last_default_dt_state, str): last_default_dt_state = datetime.datetime.fromisoformat(last_default_dt_state)
                    if isinstance(last_updated_dt_state, str): last_updated_dt_state = datetime.datetime.fromisoformat(last_updated_dt_state)
                    if last_default_dt_state and last_updated_dt_state: # Check again after potential parse
                         days_since_default_val = (last_updated_dt_state - last_default_dt_state).days
                
                crb_listing_date_str = state.get('CRBListingDate').strftime('%Y-%m-%d %H:%M:%S') if state.get('CRBListingDate') else None
                last_default_date_str = state.get('LastDefaultDate').strftime('%Y-%m-%d %H:%M:%S') if state.get('LastDefaultDate') else None
                last_updated_str = state.get('LastUpdated').strftime('%Y-%m-%d %H:%M:%S') if state.get('LastUpdated') else simulation_current_time.strftime('%Y-%m-%d %H:%M:%S')


                final_cci_update_params.append((
                    state.get('CreditScore', 500), state.get('PaymentHistoryScore', 70), 
                    state.get('CreditUtilization', Decimal('0.0')), state.get('CreditHistoryLength', 0),
                    state.get('TotalLoansTaken', 0), state.get('TotalAmountBorrowed', Decimal('0.0')), 
                    state.get('TotalAmountRepaid', Decimal('0.0')),
                    state.get('ActiveLoans', 0), state.get('ActiveLoanAmount', Decimal('0.0')), 
                    state.get('TimesDefaulted', 0), last_default_date_str,
                    days_since_default_val, 
                    state.get('CRBListed', 0), crb_listing_date_str, state.get('CRBListingType'),
                    state.get('CurrentLoanTier', 0), state.get('MaxEligibleLoanAmount', initial_max_eligible), 
                    state.get('ConsecutiveOnTimeRepayments', 0),
                    state.get('OverdraftLimit',  Decimal('5000.0')), 
                    last_updated_str, 
                    cust_id
                ))


            if final_cci_update_params:
                print(f"Applying {len(final_cci_update_params)} final aggregated CustomerCreditInfo updates...")
                cursor.executemany("""
                    UPDATE CustomerCreditInfo SET
                        CreditScore = ?, PaymentHistoryScore = ?, CreditUtilization = ?, CreditHistoryLength = ?,
                        TotalLoansTaken = ?, TotalAmountBorrowed = ?, TotalAmountRepaid = ?, ActiveLoans = ?, ActiveLoanAmount = ?,
                        TimesDefaulted = ?, LastDefaultDate = ?, DaysSinceLastDefault = ?, CRBListed = ?, CRBListingDate = ?, CRBListingType = ?,
                        CurrentLoanTier = ?, MaxEligibleLoanAmount = ?, ConsecutiveOnTimeRepayments = ?,
                        OverdraftLimit = ?, LastUpdated = ?
                    WHERE CustomerID = ?
                """, final_cci_update_params)

            if temp_crb_updates_from_hist_aggregation:
                unique_crb_for_final_update = {(c_id): (dt, typ, c_id) for dt, typ, c_id in temp_crb_updates_from_hist_aggregation}
                final_crb_params_list = list(unique_crb_for_final_update.values())
                if final_crb_params_list:
                     cursor.executemany("""
                        UPDATE CustomerCreditInfo
                        SET CRBListed = 1, CRBListingDate = ?, CRBListingType = ?,
                            CreditScore = GREATEST(300, ISNULL(CreditScore,500) - 75)
                        WHERE CustomerID = ? AND (CRBListed = 0 OR CRBListingDate < ?)
                        """, [(p[0].strftime('%Y-%m-%d %H:%M:%S') if p[0] else None, p[1], p[2], 
                              p[0].strftime('%Y-%m-%d %H:%M:%S') if p[0] else None) for p in final_crb_params_list])

            conn.commit()
            print("\nFinished generating historical repayments and applied final CustomerCreditInfo updates.")

        except Exception as e:
            if conn: conn.rollback()
            import traceback
            print(f"\nError in generate_historical_repayments: {str(e)}")
            traceback.print_exc()
            pass

def generate_credit_inquiries(months_back=12):
    conn = None
    with db_connection() as conn:
        cursor = conn.cursor()
        try:
            start_date = datetime.datetime.now() - datetime.timedelta(days=30*months_back)
            end_date = datetime.datetime.now()
            
            cursor.execute("SELECT COUNT(CustomerID) FROM Customers WHERE RegistrationDate <= ?", end_date)
            total_customers = cursor.fetchone()[0]
            
            print(f"Generating credit inquiries for {total_customers} customers...")
            start_time = datetime.datetime.now()
            
            cursor.execute("SELECT CustomerID FROM Customers WHERE RegistrationDate <= ?", end_date)
            customers = [row[0] for row in cursor.fetchall()]
            
            inquiries_generated = 0
            for i, customer_id in enumerate(customers, 1):
                # Generate 0-5 inquiries per customer
                inquiry_count = random.randint(0, 5)
                
                for _ in range(inquiry_count):
                    inquiry_date = random_date(start_date, end_date)
                    
                    lender = weighted_choice([
                        ('Commercial Bank', 30),
                        ('Sacco', 25),
                        ('Microfinance', 20),
                        ('Mobile Lender', 25)
                    ])
                    
                    purpose = weighted_choice([
                        ('Loan Application', 40),
                        ('Credit Card', 30),
                        ('Overdraft', 30)
                    ])
                    
                    # Generate amount based on lender type
                    if lender == 'Commercial Bank':
                        amount = 50000 + random.random() * 500000
                    elif lender == 'Sacco':
                        amount = 20000 + random.random() * 300000
                    elif lender == 'Microfinance':
                        amount = 5000 + random.random() * 100000
                    else:  # Mobile Lender
                        amount = 1000 + random.random() * 50000
                    
                    status = weighted_choice([
                        ('Approved', 40),
                        ('Pending', 30),
                        ('Rejected', 30)
                    ])
                    
                    cursor.execute("""
                        INSERT INTO CreditInquiries (
                            CustomerID, InquiryDate, LenderName, Purpose,
                            AmountRequested, Status
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (customer_id, inquiry_date, lender, purpose, amount, status))
                    
                    # Update recent inquiries count
                    cursor.execute("""
                        UPDATE CustomerCreditInfo
                        SET RecentInquiries = RecentInquiries + 1,
                            LastUpdated = ?
                        WHERE CustomerID = ?
                    """, (datetime.datetime.now(), customer_id))
            
                    inquiries_generated += 1
                
                    # Show progress every 100 customers
                    if i % 100 == 0 or i == total_customers:
                        show_progress(i, total_customers, start_time, f"Inquiries (total: {inquiries_generated}): ")
            
            conn.commit()
            print(f"\nGenerated {inquiries_generated} credit inquiries for {total_customers} customers")
        except Exception as e:
            conn.rollback()
            raise GenerationError(f"Error generating credit inquiries: {str(e)}")

def validate_age_distribution():
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN age < 26 THEN '18-25'
                    WHEN age < 36 THEN '26-35'
                    WHEN age < 46 THEN '36-45'
                    ELSE '46+'
                END AS age_group,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Customers) AS percentage
            FROM (
                SELECT DATEDIFF(YEAR, DateOfBirth, GETDATE()) AS age
                FROM Customers
            ) AS ages
            GROUP BY CASE 
                WHEN age < 26 THEN '18-25'
                WHEN age < 36 THEN '26-35'
                WHEN age < 46 THEN '36-45'
                ELSE '46+'
            END
        """)
        print("\nAge Distribution Validation:")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]:.1f}%")

def main():
    """Main execution function with proper error handling"""
    try:
        config = load_config()

        print("Initializing database...")
        initialize_database()

        print("Generating customers...")
        generate_customers(
            count=config['generation']['customer_count'],
            batch_size=config['generation']['batch_size']
        )

        print("Generating device info...")
        generate_device_info()

        print("Generating mobile money transactions...")
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT CustomerID FROM Customers WHERE IsActive = 1")
            active_customers = [row[0] for row in cursor.fetchall()]

        # Process transactions with progress reporting
        start_time_mm = datetime.datetime.now() # Use different start_time variable
        total_mm = len(active_customers)
        for i, customer_id in enumerate(active_customers, 1):
            try:
                generate_mobile_money_transactions(
                    customer_id,
                    months_back=config['generation']['transaction_months']
                )
                show_progress(i, total_mm, start_time_mm, "Mobile Money: ")
            except Exception as e:
                print(f"\nError processing mobile money for customer {customer_id}: {str(e)}")

        print("\nGenerating credit inquiries...")
        generate_credit_inquiries(months_back=config['generation']['transaction_months'])
        
        print("Generating SEED historical loan applications...")
        seed_app_duration_months = 4 # Generate seed loans over a 4-month application period

        seed_app_end_date = datetime.datetime.now() - datetime.timedelta(days=30 * (config['generation']['transaction_months'] -1)) # e.g., End applications 11 months ago
        seed_app_start_date = seed_app_end_date - datetime.timedelta(days=30 * seed_app_duration_months) # Start applications 2 months before that

        print(f"Generating SEED applications from {seed_app_start_date.date()} to {seed_app_end_date.date()}")
        generate_loan_applications(
            seed_app_start_date,
            seed_app_end_date,
            # Use a fraction of normal daily apps for the seed period to keep it manageable
            apps_per_day=config['generation']['loan_apps_per_day']  
        )
        
        print("Generating historical repayments (processing SEED loans and updating tiers)...")
        generate_historical_repayments(months_back=config['generation']['transaction_months'])

        
        print("Generating MAIN loan applications (for analysis)...")
        start_date = datetime.datetime.now() - datetime.timedelta(days=365)
        end_date = datetime.datetime.now() - datetime.timedelta(days=60)
        
        print(f"Generating MAIN applications from {start_date.date()} to {end_date.date()}")
        generate_loan_applications(
            start_date, end_date,
            apps_per_day=config['generation']['loan_apps_per_day']
        )

        print("Generating current repayments...")
         # This will also use the modified logic
        generate_repayments()  

        with db_connection() as conn:
            cursor = conn.cursor()
        # Check approval rates
            cursor.execute("""
                SELECT
                    COUNT(*) as total_apps,
                    SUM(CASE WHEN Status = 'Approved' THEN 1 ELSE 0 END) as approved,
                    SUM(CASE WHEN Status = 'Rejected' THEN 1 ELSE 0 END) as rejected
                FROM LoanApplications
            """)
            print("Approval Rate:", cursor.fetchone())

            # Check loan statuses
            cursor.execute("""
                SELECT Status, COUNT(*) as count
                FROM Loans
                GROUP BY Status
            """)
            print("\nLoan Status Distribution:")
            total_loans = 0
            status_counts = {}
            results = cursor.fetchall()
            for row in results:
                print(f"- {row[0]}: {row[1]}")
                status_counts[row[0]] = row[1]
                total_loans += row[1]

            active_count = status_counts.get('Active', 0)
            paid_count = status_counts.get('Paid', 0)
            defaulted_count = status_counts.get('Defaulted', 0)
            crb_count = status_counts.get('CRB', 0) # Assuming CRB is also non-active

            non_active_count = paid_count + defaulted_count + crb_count

            print(f"\nTotal Loans: {total_loans}")
            print(f"Active Loans: {active_count}")
            print(f"Non-Active (Paid + Defaulted + CRB): {non_active_count}")

            if non_active_count > active_count:
                print("Success: Non-Active loans exceed Active loans.")
            else:
                 print("Note: Non-Active loans DO NOT exceed Active loans with current settings.")


            cursor.execute("""
                SELECT
                    FLOOR(CreditScore/100)*100 as score_range,
                    COUNT(*) as customers
                FROM CustomerCreditInfo
                GROUP BY FLOOR(CreditScore/100)*100
                ORDER BY score_range
            """)
            print("\nCredit Score Distribution:", cursor.fetchall()) # Use fetchall

            cursor.execute("""
                SELECT ActiveLoans, COUNT(*) as customers
                FROM CustomerCreditInfo
                GROUP BY ActiveLoans
                ORDER BY ActiveLoans
            """)
            print("Active Loans per Customer:", cursor.fetchall()) # Use fetchall

        # Validate
        validate_age_distribution()
        # validate_decimal() # Commented out as it's not a validation function in the provided snippet

    except DataGenerationError as e:
        print(f"\nGeneration failed: {str(e)}")
        return 1
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        return 1
    finally:
        print("\nData generation complete!")

if __name__ == "__main__":
    sys.exit(main())
    
    
    