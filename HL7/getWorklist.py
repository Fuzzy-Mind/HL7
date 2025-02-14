#!/usr/bin/python

import psycopg2
import socket
import time
import logging
from pydicom.dataset import Dataset
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import ModalityWorklistInformationFind, SecondaryCaptureImageStorage, EncapsulatedPDFStorage
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

# Log ayarları
logging.basicConfig(filename='/var/www/kuvoz2/storage/logs/custom.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
error = ''

# Veritabanı bağlantı bilgileri
config = {
    'user': 'hl7',
    'password': 'hl7',
    'host': '127.0.0.1',
    'dbname': 'hl7',
    'port': 5432
}

# Veritabanına bağlan
conn1 = psycopg2.connect(**config)
cursor1 = conn1.cursor()

query = "SELECT * FROM dicoms LIMIT 1"
cursor1.execute(query)
record = cursor1.fetchone()

cursor1.close()
conn1.close()


try:
    pass
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    sql = "DELETE FROM worklists"
    cursor.execute(sql)
    conn.commit()
    logging.info('Hasta Listesi Silindi')
except Exception as e:
    error = error + "Hasta Listesi silinmesi sırasında bir hata oluştu"
    logging.error(f"Hasta Listesi silinmesi sırasında bir hata oluştu: {e}")

if error:
    pass
else:
    query = """INSERT INTO worklists (accesionnumber, patientid, surname, forename, sex, birthdate, referringphysician, modality, examdate, examtime, examdescription, studyuid, procedureid, procedurestepid, hospitalname, otherpatientid)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    # Initialise the Application Entity
    ae = AE()

    # Add a requested presentation context
    ae.add_requested_context(ModalityWorklistInformationFind)
    ae.ae_title = record[5]

    # Timeout mutlaka ayarlanmalı
    ae.dimse_timeout = 60
    ae.network_timeout = 60

    # Create our Identifier (query) dataset
    ds = Dataset()

    # ScheduledProcedureStepSequence alt sıralaması için alt sıralama nesnesi oluşturun
    ds.ScheduledProcedureStepSequence = [Dataset()]
    item = ds.ScheduledProcedureStepSequence[0]

    item.Modality = record[5]

    # Associate with peer AE at IP 127.0.0.1 and port 11112
    # assoc = ae.associate(wl_serverip, wl_serverport)
    assoc = ae.associate(record[2], int(record[3]), ae_title=record[4])

    if assoc.is_established:
        responses = assoc.send_c_find(ds, ModalityWorklistInformationFind)
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        for (status, identifier) in responses:
            if status:
                access_id = identifier.AccessionNumber
                # if not is_record_exists(access_id):  # Eğer AccessionNumber zaten yoksa
                access_id = identifier.AccessionNumber
                patient_id = identifier.PatientID
                b = identifier.PatientName.encode('raw_unicode_escape').decode('ISO-8859-9', errors='ignore')
                name_parts = b.split('^')
                patient_surname = name_parts[0] if len(name_parts) > 0 else ""
                patient_name = name_parts[1] if len(name_parts) > 1 else ""
                middle_name = name_parts[2] if len(name_parts) > 2 else ""
                sex = identifier.PatientSex
                birthdate = identifier.PatientBirthDate
                referring_name = identifier.ReferringPhysicianName.encode('raw_unicode_escape').decode(
                    'ISO-8859-9',
                    errors='ignore')
                modality = identifier.ScheduledProcedureStepSequence[0].Modality
                exam_date = identifier.ScheduledProcedureStepSequence[0].ScheduledProcedureStepStartDate
                exam_time = \
                    identifier.ScheduledProcedureStepSequence[0].ScheduledProcedureStepStartTime.split('.')[0]
                exam_description = identifier.ScheduledProcedureStepSequence[
                    0].ScheduledProcedureStepDescription
                study_uid = generate_uid()
                procedure_id = identifier.RequestedProcedureID
                procedure_step_id = identifier.ScheduledProcedureStepSequence[0].ScheduledProcedureStepID
                # hospital_name = identifier.InstitutionName
                hospital_name = "ERTUNC_OZCAN"
                # other_patient_id = identifier.OtherPatientIDs
                other_patient_id = identifier.PatientID
                values = (
                    access_id, patient_id, patient_surname, patient_name, sex, birthdate, referring_name, modality,
                    exam_date, exam_time, exam_description, study_uid, procedure_id, procedure_step_id,
                    hospital_name, other_patient_id)
                print(values)
                cursor.execute(query, values)
                conn.commit()

            else:
                error = "Uygun Hasta Bulunamadı"
                logging.error('Bağlantı zaman aşımına uğradı, iptal edildi veya geçersiz yanıt alındı')

        # Release the association
        assoc.release()
        cursor.close()
        conn.close()
    else:
        error = "DICOM bağlantısı oluşturulamadı"
        logging.error('Bağlantı reddedildi, iptal edildi veya hiç bağlanmadı')

if error:
    print(error)
else:
    print('success')
#get_worklist(record[2], int(record[3]), record[4], record[5], record[6])
