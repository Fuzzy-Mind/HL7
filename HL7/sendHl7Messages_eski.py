#!/usr/bin/python

import psycopg2
import psycopg2.extras
import socket
import time
import logging
from datetime import datetime
import subprocess

start_time = time.time()
# Log ayarları
logging.basicConfig(filename='/var/www/kuvoz/storage/logs/custom.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Veritabanı bağlantı bilgileri
config = {
    'user': 'hl7',
    'password': 'hl7',
    'host': '127.0.0.1',
    'dbname': 'hl7',
    'port': 5432
}

# Cihaz Seri Numarası
serial = "720"

# Hastane ismi
hastane = "Eskişehir Devlet Hastanesi"

# Küvözden gelen verilerin diğer özellikleri
props = {
    "skin1temp": ["ST1", "NM", "1001"],
    "skin2temp": ["ST2", "NM", "1002"],
    "spo2": ["SPO2", "NM", "1003"],
    "pulserate": ["PR", "NM", "1004"],
    "perfusionindex": ["PI", "NM", "1005"],
    "systolic": ["SYS", "NM", "1006"],
    "diastolic": ["DIA", "NM", "1007"],
    "pulseratenibp": ["PRN", "NM", "1008"],
    "map": ["MAP", "NM", "1009"],
    "co": ["CO", "NM", "1010"],
    "met": ["MET", "NM", "1011"],
    "hb": ["HB", "NM", "1012"],
    "runmode": ["RM", "CE", "2001"],
    "powerdata": ["PD", "CE", "2002"],
    "nibperror": ["NIBP-E", "CE", "2003"],
    "doorstatus": ["DS", "CE", "2004"],
    "thermomain": ["TM", "NM", "2005"],
    "thermohumidity": ["TH", "NM", "2006"],
    "floatdata": ["FD", "CE", "2007"],
    "batdata": ["BD", "NM", "2008"],
    "heaterpwm": ["PWM", "NM", "2009"],
    "o2": ["O2", "NM", "3001"],
    "humidity": ["HUM", "NM", "3002"],
    "tempair": ["TEMP-AIR", "NM", "3003"],
    "tempaux": ["TEMP-AUX", "NM", "3004"],
    "seto2": ["SET-O2", "NM", "4001"],
    "sethumidity": ["SET-HUM", "NM", "4002"],
    "settemperature": ["SET-TEMP", "NM", "4003"],
    "alarmled": ["ALARM_STATUS", "CE", "01"],
}

# Veritabanına bağlan
conn1 = psycopg2.connect(**config)
cursor1 = conn1.cursor()

# HL7 Server baglanti bilgileri cekiliyor
query = "SELECT * FROM hlyedis LIMIT 1"
cursor1.execute(query)
record = cursor1.fetchone()

cursor1.close()
conn1.close()
# print(record)

# Gönderilecek IP ve port bilgisi
HL7_SERVER_IP = record[2]
HL7_SERVER_PORT = int(record[3])
# print(HL7_SERVER_IP)
# print(HL7_SERVER_PORT)

# Raspberry uzerindeki servislerin aalisip calismadigini kontrol eden modul
def check_service_status(service_name):
    try:
        result = subprocess.run(['systemctl', 'is-active', service_name],
                                stdout=subprocess.PIPE, text=True, check=True)
        if result.stdout.strip() == 'active':
            return True
        else:
            return False
    except subprocess.CalledProcessError:
        # Servis aktif değil veya başka bir hata oluştu
        return False

while True:
    try:
        # Veritabanina tekrar baglaniliyor
        conn = psycopg2.connect(**config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Yatan hasta bilgisi aliniyor
        query2 = "SELECT * FROM incubators WHERE status = 1"
        cursor.execute(query2)
        pid = cursor.fetchone()

        # Tarih aliniyor
        now = datetime.now()
        fnow = now.strftime("%Y%m%d%H%M%S")

        # HL7 Mesajinin MSH alani olusturuluyor
        hl7_message = [
            "MSH|^~\&|EONIS|Magic-Loggia-Ultimate|Hastane_HBYS|" + hastane + "|" +
            fnow + "||ORU^R01|" + fnow + "-" + serial + "|P|2.3.1||||||UTF8|"
        ]

        # Eger yatan hasta bilgisi yoksa gerekli error alani olusturuluyor
        if pid == None:
            hl7_message.append(
                f"ERR|||0|W|103^Hasta Bilgileri Bulunamadı|MLU-CM11-{serial}|")
        else:
            # Hasta bilgisi varsa PID alani olusturuluyor
            hl7_message.append(
                "PID||" +
                (pid[1] or '') + "|" +  # Hasta Kimliği
                (pid[4] or '') + "|" +  # Hasta Kimlik Listesi
                (pid[5] or '') + "|" +  # Alternatif Hasta Kimliği
                (pid[2] or '') + '^' +  #
                (pid[3].replace(' ', '^') if pid[3] else '') + "||" +
                (pid[6] or '') + "|" +
                (pid[7] or '') + "|||" +
                (pid[8].replace(' ', '^') if pid[8] else '') + "||" +
                (pid[9] or '') + '^^^' + (pid[10] or '') + "||||||||" +
                (pid[11] or '')
            )

            # Datas tablosundaki kayit sayisi cekiliyor
            query3 = "SELECT COUNT(*) FROM datas"
            cursor.execute(query3)
            data_count = cursor.fetchone()[0]

            # Eger yatan hastanin datalari yoksa gerekli error alani olusturuluyor
            if data_count == 0:
                hl7_message.append(
                    f"ERR|||0|W|100^Veritabanında Veri Yok|MLU-CM11-{serial}|")
            else:
                # Kullanici tarafından gonderilmesi istenilen alanlar seciliyor
                query1 = "SELECT slug FROM telemetries WHERE status = 1 ORDER BY sira ASC"
                cursor.execute(query1)
                data_types_to_send = [row[0] for row in cursor.fetchall()]

                # Kullanici tarafindan zin verilen verilere gore datas tablosundaki ilk kayittan bu bilgileri aliniyor
                query3 = f"SELECT {' ,'.join(data_types_to_send)} FROM datas ORDER BY id LIMIT 1"
                cursor.execute(query3)
                data = cursor.fetchone()

                # Yukaridaki alinan kayitlara gore OBX kayitlari olusturuluyor
                for data_type in data_types_to_send:
                    hl7_message.append(
                        f"OBX||{props[data_type][1]}|{props[data_type][2]}^{props[data_type][0]}||{str(data[data_type])}||||||F|")

                # hostReader servisi durmussa gerekli error alani olustutluyor.
                if not check_service_status('hostReader.service'):
                    hl7_message.append(
                        f"ERR|||0|W|101^Host Okuma Servisi Durdu|MLU-CM11-{serial}|")

                # slaveReader servisi durmussa gerekli error alani olustutluyor.
                if not check_service_status('slaveReader.service'):
                    hl7_message.append(
                        f"ERR|||0|W|102^Slave Okuma Servisi Durdu|MLU-CM11-{serial}|")

                # Herhangibir hata yoksa mesaj basarili sekilde gonderildi seklinde bir ERR alani olusturuluyor.
                if check_service_status('hostReader.service') and check_service_status('slaveReader.service'):
                    hl7_message.append(
                        f"ERR|||0|W|0^Mesaj Kabul Edildi|MLU-CM11-{serial}|")


        # Bir websocket acilip olusturulan hl7 mesaji buradan daha once bilgilerini aldiğimiz HL7 Servera gonderiliyor
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HL7_SERVER_IP, HL7_SERVER_PORT))
            s.sendall("\r".join(hl7_message).encode('utf-8'))

        # print('gonderme6')
        print(hl7_message)
        #delete_query = "DELETE FROM datas WHERE ctid IN (SELECT ctid FROM datas ORDER BY id LIMIT 1)"
        #cursor.execute(delete_query)
        #conn.commit()

        # DB sorgusu cekmek icin actigimiz corsur kapatiliyor
        cursor.close()
        conn.close()

        logger.info("HL7 mesaji gonderildi: " + str(hl7_message))

    # Bir hata olmasi durumunda gerekli exceptionslar firlatiliyor
    except psycopg2.Error as err:
        logger.error(f"Error with PostgreSQL: {err}")
    except socket.error as err:
        logger.error(f"Error with socket connection: {err}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    # 5 saniye gecikme koyularak scriptin tekrar calismasi saglaniyor
    time.sleep(5)
