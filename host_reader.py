import sys
import serial
import time
import math
import redis

dbValues = {
	"setTemperature": 0,
	"setHumidity": 0,
	"setO2": 0,
	"calibOxygen1": 0,
	"calibOxygen2": 0,
	"alarmLed": 0,
	"heaterPwm": 0
	}


dbRedis = redis.Redis(host = '127.0.0.1', port = 6379, db=0)

try:
	ser = serial.Serial('/dev/ttyUSB0', 115200)
	print("Host Port Successful")
except:
	print("Host Port Failed")
	sys.exit()
	
def readPacketHost(packetLen):	#46 Byte Magic Loggia Ultimate
	while(1):
		packet = []
		csum = 0
		counter = 0
		incomingByte = ser.read(1)
		incomingHex = incomingByte[0]
		if(incomingHex == 0xa1):
			packet.append(incomingHex)
			while(1):
				incomingByte = ser.read(1)
				incomingHex = incomingByte[0]
				counter += 1
				if(counter < packetLen):
					packet.append(incomingHex)
				else:
					packet.append(incomingHex)
					if(incomingHex == 0xaf):
						csum = sum(packet) - packet[-2]
						if((csum & 0xff) == packet[-2]):
							return packet
					else:
						return 0

while(1):
	incomingPacket = readPacketHost(46)
	if(incomingPacket != 0):
		heaterPwm = ((incomingPacket[9] << 8) + incomingPacket[8])
		dbValues["heaterPwm"] = round(heaterPwm*10/3)
		
		setTemperatureData = ((incomingPacket[11] << 8) + incomingPacket[10]) / 100
		dbValues["setTemperature"] = setTemperatureData
		
		setHumidityData = (incomingPacket[14] << 8) + incomingPacket[13]
		dbValues["setHumidity"] = setHumidityData
		
		setOxygenData = (incomingPacket[16] << 8) + incomingPacket[15]
		dbValues["setO2"] = setOxygenData
		
		calibOxygen1 = ((incomingPacket[18] << 8) + incomingPacket[17]) / 21
		dbValues["calibOxygen1"] = calibOxygen1
		
		calibOxygen2 = ((incomingPacket[20] << 8) + incomingPacket[19]) / 21
		dbValues["calibOxygen2"] = calibOxygen2
		
		alarmLed = incomingPacket[35]
		dbValues["alarmLed"] = alarmLed
		
		dbRedis.mset(dbValues)
		print(dbValues)
	
