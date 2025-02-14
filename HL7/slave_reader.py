import sys, os, time
import math
import serial
import redis
import json
import RPi.GPIO as GPIO
import psycopg2
from paho.mqtt import client as mqtt_client
from datetime import datetime as dt

wifiLed = 4



try:
	postgreDb = psycopg2.connect(host='localhost', database = "hl7", user = "hl7", password = "hl7")
	print("Connected to database")
except:
	print("Database connection failed")

dbRedis = redis.Redis(host = '127.0.0.1', port = 6379, db=0)
dbTare = redis.Redis(host = '127.0.0.1', port = 6379, db=1)
dbWeight = redis.Redis(host = '127.0.0.1', port = 6379, db=2)
dbSlave = redis.Redis(host = '127.0.0.1', port = 6379, db=3)
dbTime = redis.Redis(host = '127.0.0.1', port = 6379, db=4)
dbMatch = redis.Redis(host = '127.0.0.1', port = 6379, db=5)


jsonValues = {
	"device": {   
	"serial": 0,
	"heaterPwm": 0,
	"runMode": 0,
	"alarmLed": 0,
	"alarmInfrared": 0,
	"nibpStatus": 0,
	"thermoMain": 0,
	"thermoHumidity": 0,
	"powerData": 0,
	"batData": 0,
	"nibpError": 0,
	"floatData": 0,
	"doorStatus": 0
	},
	"patient": { 
	"skin1Temp": 0,
	"skin2Temp": 0,
	"spo2": 0,
	"pulseRate": 0,
	"perfusionIndex": 0,
	"systolic": 0,
	"diastolic": 0,
	"map": 0,
	"pulseRateNibp": 0,
	"cuffPressure":0,
	"weight": 0,
	"co": 0,
	"met": 0,
	"hb": 0
	}, 
	"set": {
	"temperature": 0,
	"humidity": 0,
	"o2": 0
	},  
	"environment": {
	"tempAir": 0,
	"tempAux": 0,
	"humidity": 0,
	"o2": 0,
	},
	"exceptions":{
	"spo2Exp":0,
	"pulseRateExp":0,
	"piExp":0,
	"coExp":0,
	"metExp":0,
	"hbExp":0,
	"pulseSystemExp":0,
	"pulseBoardExp":0,
	"pulseDiagExp":0,
	"pulseProgExp":0
	}
}


currentTime = {"time": 0}
currentTime["time"] = time.time()
dbTime.mset(currentTime)

matchStatus = {"match": 0}
matchStatus["match"] = 0
dbMatch.mset(matchStatus)

tareValue = {"tare": 0}
dbTare.mset(tareValue)

weightRawValue = {"weight1Raw": 0, "weight2Raw": 0, "weightRaw":0}
dbWeight.mset(weightRawValue)

f = open("/boot/serialNo.txt", 'r')
serialNoBase = int(f.read())
f.close()

broker = '209.38.224.156'
port = 1883
topic = str(serialNoBase)
jsonValues["device"]["serial"] = topic
client_id = ""
username = 'hl7'
password = 'hl7.0423'

GPIO.setmode(GPIO.BCM)
GPIO.setup(wifiLed, GPIO.OUT)
ledStatus = False
GPIO.output(wifiLed, ledStatus)

try:
	ser = serial.Serial('/dev/ttyS0', 115200)
	print("Slave Port Successful")
except:
	print("Slave Port Failed")
	sys.exit()
	
def connect_mqtt():
	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			print("Connected to MQTT Broker!")
		else:
			print("Failed to connect, return code %d\n", rc)

	client = mqtt_client.Client(client_id)
	client.username_pw_set(username, password)
	client.on_connect = on_connect
	client.connect(broker, port)
	return client

def readPacketSlave():
	while(1):
		packet = []
		csum = 0
		incomingByte = ser.read(1)
		incomingHex = incomingByte[0]
		if(incomingHex == 0xa1):
			packet.append(incomingHex)
			incomingByte = ser.read(1)
			incomingHex = incomingByte[0]
			if(incomingHex == 0x52):
				packet.append(incomingHex)
				for i in range(88):
					incomingByte = ser.read(1)
					incomingHex = incomingByte[0]
					packet.append(incomingHex)
				if(packet[-1] == 0xaf):
					csum = sum(packet) - packet[-2]
					if((csum & 0xff) == packet[-2]):
						return packet
					else:
						return 0
				else:
					return 0
			if(incomingHex == 0x50):
				packet.append(incomingHex)
				for i in range(21):
					incomingByte = ser.read(1)
					incomingHex = incomingByte[0]
					packet.append(incomingHex)
				if(packet[-1] == 0xaf):
					csum = sum(packet) - packet[-2]
					if((csum & 0xff) == packet[-2]):
						return packet
					else:
						return 0
				else:
					return 0

def checkConnection():
	ipAddress = os.popen('hostname -I')
	ipAddress = ipAddress.read()
	if(len(ipAddress)>2):
		print("Connected")
		GPIO.output(wifiLed, True)
	else:
		print("Disconnected")
		GPIO.output(wifiLed, False)
		
'''while 1:
	incomingPacket = readPacketSlave()
	if(incomingPacket != 0):
		if(incomingPacket[1] == 0x50):
			serialNo = (incomingPacket[7] << 8) + incomingPacket[6]
			if(serialNoBase == int(serialNo)):
				print("Matched")
				jsonValues["device"]["serial"] = serialNoBase
				topic = str(serialNoBase)
				matchStatus["match"] = 1
				dbMatch.mset(matchStatus)
				break
			else:
				print("Match Failed")
				matchStatus["match"] = 2
				dbMatch.mset(matchStatus)
				sys.exit()'''	
			
while 1:					
	ipAddress = os.popen('hostname -I')
	ipAddress = ipAddress.read()
	ledStatus = not ledStatus
	GPIO.output(wifiLed, ledStatus)
	if(len(ipAddress)>2):
		print("Connected")
		GPIO.output(wifiLed, True)
		break
	else:
		time.sleep(0.2)
		
while 1:
	try:					
		client = connect_mqtt()
		client.loop_start()
		break
	except:
		time.sleep(1)
			

while(1):
	checkConnection()
	ser.flushInput()
	incomingPacket = readPacketSlave()
	outcomingPacket = ""
	print("---------------------")
	if(incomingPacket != 0):
		'''if(incomingPacket[1] == 0x50):
			serialNo = (incomingPacket[7] << 8) + incomingPacket[6]
			fileSerial = open('/boot/serialNo.txt', 'w')
			fileSerial.write(str(serialNo))
			fileSerial.close()
			jsonValues["device"]["serial"] = serialNo
			
			currentTime["time"] = time.time()
			dbTime.mset(currentTime)'''
				
		if(incomingPacket[1] == 0x52):
			airA = 1.406316360e-03
			airB = 2.366845211e-04
			airC = 1.027170342e-07
			try:
				airLn = math.log((incomingPacket[12] << 8) + incomingPacket[11])
				airTemp = 1/(airA + (airB * airLn) + (airC * pow(airLn,3))) - 273.15
				jsonValues["environment"]["tempAir"] = round(airTemp, 2)
				
				airAux = ((incomingPacket[14] << 8) + incomingPacket[13])/100
				jsonValues["environment"]["tempAux"] = round(airAux, 2)
			except:
				jsonValues["environment"]["tempAir"] = 0
				jsonValues["environment"]["tempAux"] = 0
			
			skinA = 1.213e-03
			skinB = 2.353e-04
			skinC = 0.9e-07
			try:
				skin1Ln = math.log((incomingPacket[16] << 8) + incomingPacket[15])
			except:
				skin1Ln = 0
				
			skin1Temp = 1/(skinA + (skinB * skin1Ln) + (skinC * pow(skin1Ln,3))) - 273.15
			if(skin1Temp<0):
				skin1Temp = 0
			jsonValues["patient"]["skin1Temp"] = round(skin1Temp, 2)
			
			try:
				skin2Ln = math.log((incomingPacket[18] << 8) + incomingPacket[17])
			except:
				skin2Ln = 0
			skin2Temp = 1/(skinA + (skinB * skin2Ln) + (skinC * pow(skin2Ln,3))) - 273.15
			if(skin2Temp<0):
				skin2Temp=0
			jsonValues["patient"]["skin2Temp"] = round(skin2Temp, 2)
			
			humidity = ((incomingPacket[20] << 8) + incomingPacket[19])/100
			jsonValues["environment"]["humidity"] = round(humidity, 2)
					
			oxygenPercent_1 = float(dbRedis.get("calibOxygen1"))
			oxygenPercent_2 = float(dbRedis.get("calibOxygen2"))
			
			o2raw_1 = (incomingPacket[22] << 8) + incomingPacket[21]
			o2raw_2 = (incomingPacket[24] << 8) + incomingPacket[23]
			o2value_1 = o2raw_1/oxygenPercent_1
			o2value_2 = o2raw_2/oxygenPercent_2
			o2averageValue=((o2value_1+o2value_2)/2)+0.5
			if(o2averageValue>100):
				o2averageValue = 0
			jsonValues["environment"]["o2"] = round(o2averageValue,2)
			'''
			weightCalibFile = open("/home/hl7/scripts/hl7_2/calib0.txt", 'r')
			calib0list = weightCalibFile.read().split("; ")
			weightCalibFile.close()

			weightCalibFile = open("/home/hl7/scripts/hl7_2/calib5.txt", 'r')
			calib5list = weightCalibFile.read().split("; ")
			weightCalibFile.close()

			weight1calib0 = int(calib0list[0])
			weight2calib0 = int(calib0list[1])
			weight1calib5 = int(calib5list[0])
			weight2calib5 = int(calib5list[1])
		
			
			weight1Raw = (incomingPacket[26] << 8) + incomingPacket[25]
			weight2Raw = (incomingPacket[28] << 8) + incomingPacket[27]
			
			dbWeight["weight1Raw"] = weight1Raw
			dbWeight["weight2Raw"] = weight2Raw
			
			tare = int(dbTare["tare"])
		
			percent1raw = (weight1calib5 - weight1calib0)/5000
			percent2raw = (weight2calib5 - weight2calib0)/5000
			try:
				weight1 = (weight1Raw - weight1calib0)/percent1raw
			except:
				weight1 = 32747
			try:
				weight2 = (weight2Raw - weight2calib0)/percent2raw
			except:
				weight2 = 32747
			weight = round((weight1 + weight2)/2)
			dbWeight["weightRaw"] = weight
			weight = weight - tare
			if(weight<=0):
				weight = 0	
			jsonValues["patient"]["weight"] = weight'''
			
			spo2 = (incomingPacket[30] << 8) + incomingPacket[29]
			jsonValues["patient"]["spo2"] = spo2
			
			pr = (incomingPacket[32] << 8) + incomingPacket[31]
			jsonValues["patient"]["pulseRate"] = pr
			
			pi = (incomingPacket[34] << 8) + incomingPacket[33]
			jsonValues["patient"]["perfusionIndex"] = pi
			
			co = (incomingPacket[36] << 8) + incomingPacket[35]
			jsonValues["patient"]["co"] = co
			
			met = (incomingPacket[38] << 8) + incomingPacket[37]
			jsonValues["patient"]["met"] = met
			
			hb = (incomingPacket[40] << 8) + incomingPacket[39]
			jsonValues["patient"]["hb"] = hb
			
			spo2Exp = (incomingPacket[42] << 8) + incomingPacket[41]
			jsonValues["exceptions"]["spo2Exp"] = spo2Exp
			
			prExp = (incomingPacket[44] << 8) + incomingPacket[43]
			jsonValues["exceptions"]["pulseRateExp"] = prExp
			
			piExp = (incomingPacket[46] << 8) + incomingPacket[45]
			jsonValues["exceptions"]["piExp"] = piExp
			
			coExp = (incomingPacket[48] << 8) + incomingPacket[47]
			jsonValues["exceptions"]["coExp"] = coExp
			
			metExp = (incomingPacket[50] << 8) + incomingPacket[49]
			jsonValues["exceptions"]["metExp"] = metExp
			
			hbExp = (incomingPacket[52] << 8) + incomingPacket[51]
			jsonValues["exceptions"]["hbExp"] = hbExp
			
			pulseSystemExp = (incomingPacket[56] << 24) + (incomingPacket[55] << 16) + (incomingPacket[54] << 8) + incomingPacket[53]
			jsonValues["exceptions"]["pulseSystemExp"] = pulseSystemExp
			
			pulseBoardExp = (incomingPacket[58] << 8) + incomingPacket[57]
			jsonValues["exceptions"]["pulseBoardExp"] = pulseBoardExp
			
			pulseDiagExp = (incomingPacket[60] << 8) + incomingPacket[59]
			jsonValues["exceptions"]["pulseDiagExp"] = pulseDiagExp
			
			pulseProgExp = (incomingPacket[62] << 8) + incomingPacket[61]
			jsonValues["exceptions"]["pulseProgExp"] = pulseProgExp
			
			thermoMain = (incomingPacket[64] << 8) + incomingPacket[63]
			jsonValues["device"]["thermoMain"] = thermoMain
			
			thermoHumidity = (incomingPacket[66] << 8) + incomingPacket[65]
			jsonValues["device"]["thermoHumidity"] = thermoHumidity
			
			floatData = (incomingPacket[68] << 8) + incomingPacket[67]
			jsonValues["device"]["floatData"] = floatData
			
			powerData = (incomingPacket[70] << 8) + incomingPacket[69]
			jsonValues["device"]["powerData"] = powerData
			
			batData = ((incomingPacket[72] << 8) + incomingPacket[71])/10
			jsonValues["device"]["batData"] = batData
			
			nibpStatus = incomingPacket[73] 
			jsonValues["device"]["nibpStatus"] = nibpStatus
			
			nibpSystolic = (incomingPacket[75] << 8) + incomingPacket[74]
			jsonValues["patient"]["systolic"] = nibpSystolic
			
			nibpDiastolic = (incomingPacket[77] << 8) + incomingPacket[76]
			jsonValues["patient"]["diastolic"] = nibpDiastolic
			
			nibpMap = (incomingPacket[79] << 8) + incomingPacket[78]
			jsonValues["patient"]["map"] = nibpMap
			
			nibpPr = (incomingPacket[81] << 8) + incomingPacket[80]
			jsonValues["patient"]["pulseRateNibp"] = nibpPr
			
			nibpCuff = (incomingPacket[83] << 8) + incomingPacket[82]
			jsonValues["patient"]["cuffPressure"] = nibpCuff
			
			nibpError = incomingPacket[84] 
			jsonValues["device"]["nibpError"] = nibpError
			
			alarmInfrared = (incomingPacket[86] << 8) + incomingPacket[85]
			jsonValues["device"]["alarmInfrared"] = alarmInfrared
			
			doorStatus = incomingPacket[87] 
			jsonValues["device"]["doorStatus"] = doorStatus
			
			jsonValues["device"]["heaterPwm"] = int(dbRedis["heaterPwm"])
			jsonValues["device"]["runMode"] = int(dbRedis["runMode"])
			jsonValues["device"]["alarmLed"] = int(dbRedis["alarmLed"])
			jsonValues["set"]["temperature"] = float(dbRedis["setTemperature"])
			jsonValues["set"]["humidity"] = int(dbRedis["setHumidity"])
			jsonValues["set"]["o2"] = int(dbRedis["setO2"])
			
			currentTime["time"] = time.time()
			dbTime.mset(currentTime)
		
		print(jsonValues["device"])
		print(jsonValues["patient"])
		print(jsonValues["set"])
		print(jsonValues["environment"])
		print(jsonValues["exceptions"])
		
		try:
			cursorDb = postgreDb.cursor()
			command = ('INSERT INTO datas(serial, heaterPwm, runMode, alarmLed, '
					   'alarmInfrared, nibpStatus, thermoMain, thermoHumidity, powerData, '
					   'batData, nibpError, floatData, doorStatus, skin1Temp, skin2Temp, '
					   'spo2, pulseRate, perfusionIndex, systolic, diastolic, map, pulseRateNibp, '
					   'cuffPressure, weight, co, met, hb, setTemperature, setHumidity, setO2, '
					   'tempAir, tempAux, humidity, o2, spo2Exp, pulseRateExp, piExp, coExp, metExp, '
					   'hbExp, pulseSystemExp, pulseBoardExp, pulseDiagExp, pulseProgExp, timestamp) VALUES (')
					   
			command = command + jsonValues["device"]["serial"] + ", "
			command = command + str(jsonValues["device"]["heaterPwm"]) + ", "
			command = command + str(jsonValues["device"]["runMode"]) + ", "
			command = command + str(jsonValues["device"]["alarmLed"]) + ", "
			command = command + str(jsonValues["device"]["alarmInfrared"]) + ", "
			command = command + str(jsonValues["device"]["nibpStatus"]) + ", "
			command = command + str(jsonValues["device"]["thermoMain"]) + ", "
			command = command + str(jsonValues["device"]["thermoHumidity"]) + ", "
			command = command + str(jsonValues["device"]["powerData"]) + ", "
			command = command + str(jsonValues["device"]["batData"]) + ", "
			command = command + str(jsonValues["device"]["nibpError"]) + ", "
			command = command + str(jsonValues["device"]["floatData"]) + ", "
			command = command + str(jsonValues["device"]["doorStatus"]) + ", "
			command = command + str(jsonValues["patient"]["skin1Temp"]) + ", "
			command = command + str(jsonValues["patient"]["skin2Temp"]) + ", "
			command = command + str(jsonValues["patient"]["spo2"]) + ", "
			command = command + str(jsonValues["patient"]["pulseRate"]) + ", "
			command = command + str(jsonValues["patient"]["perfusionIndex"]) + ", "
			command = command + str(jsonValues["patient"]["systolic"]) + ", "
			command = command + str(jsonValues["patient"]["diastolic"]) + ", "
			command = command + str(jsonValues["patient"]["map"]) + ", "
			command = command + str(jsonValues["patient"]["pulseRateNibp"]) + ", "
			command = command + str(jsonValues["patient"]["cuffPressure"]) + ", "
			command = command + str(jsonValues["patient"]["weight"]) + ", "
			command = command + str(jsonValues["patient"]["co"]) + ", "
			command = command + str(jsonValues["patient"]["met"]) + ", "
			command = command + str(jsonValues["patient"]["hb"]) + ", "
			command = command + str(jsonValues["set"]["temperature"]) + ", "
			command = command + str(jsonValues["set"]["humidity"]) + ", "
			command = command + str(jsonValues["set"]["o2"]) + ", "
			command = command + str(jsonValues["environment"]["tempAir"]) + ", "
			command = command + str(jsonValues["environment"]["tempAux"]) + ", "
			command = command + str(jsonValues["environment"]["humidity"]) + ", "
			command = command + str(jsonValues["environment"]["o2"]) + ", "
			command = command + str(jsonValues["exceptions"]["spo2Exp"]) + ", "
			command = command + str(jsonValues["exceptions"]["pulseRateExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["piExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["coExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["metExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["hbExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["pulseSystemExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["pulseBoardExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["pulseDiagExp"]) + ", "
			command = command + str(jsonValues["exceptions"]["pulseProgExp"]) + ", "
			command = command + "'" + str(dt.now())[:19] + "'" + ");"
			
			cursorDb.execute(command)
			postgreDb.commit()
			cursorDb.close()
		except:
			print("Inserting to database failed")

		jsonStr = json.dumps(jsonValues)
		try:
			result = client.publish(topic, jsonStr)
			print(result[0])
		except:
			print("Couldnt Send Data to Server")
		jsonBytes = bytes(jsonStr, 'utf-8')
		dbSlave.set('datas', jsonBytes)
	else:
		print("No Incubator")
		
