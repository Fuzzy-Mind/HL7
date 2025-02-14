import time
import os
import RPi.GPIO as GPIO
from bluepy import btle

bleLed = 24
GPIO.setmode(GPIO.BCM)
GPIO.setup(bleLed, GPIO.OUT)
GPIO.output(bleLed, GPIO.LOW)

deviceMAC  = "00:00:00:00:00:00"
try:
	serialFile = open('/boot/tool.txt', 'r')
	deviceMAC = serialFile.read(17)
	if(len(deviceMAC) != 17):
		print("Wrong MAC Address")
	else:
		print(deviceMAC)
except:
	print("Wrong MAC Address")
	

toolSrvc = "4fafc201-1fb5-459e-8fcc-c5c9c331914b"
#ssidChar = "beb5483e-36e1-4688-b7f5-ea07361b26a8"
#passChar = "beb5483e-36e1-4688-b7f5-ea07361b1b23"


def connect():
	print("Connecting ...")
	GPIO.output(bleLed, GPIO.LOW)
	try:
		dev = btle.Peripheral(deviceMAC)
		print("Connected")
		GPIO.output(bleLed, GPIO.HIGH)
		time.sleep(1)
		for svc in dev.services:
			print(str(svc))
		return dev
	except:
		print("No Device")
		
		
def readCharacteristic(device):
	try:
		tool = btle.UUID(toolSrvc)
		toolService = device.getServiceByUUID(tool)
		wifiSsid = toolService.getCharacteristics()[0].read()
		wifiPass = toolService.getCharacteristics()[1].read()
		if(sum(wifiSsid) == 0 or sum(wifiPass) == 0):
			return 0
		else:
			ssidStr = ""
			passStr = ""
			for i in range(20):
				if(wifiSsid[i] != 0):
					ssidStr = ssidStr + chr(wifiSsid[i])
			for i in range(20):
				if(wifiPass[i] != 0):
					passStr = passStr + chr(wifiPass[i])
			return [ssidStr, passStr]
	except:
		return None

device = connect()

while 1:
	data = readCharacteristic(device)
	if(data == None):
		device = connect()
	else:
		if(data != 0):
			print(data)
			try:
				wpaFile = open('/etc/wpa_supplicant/wpa_supplicant.conf', 'w')
				wpaStr = "ctrl_interface=DIR=/var/run/wpa_supplicant GROUP=netdev\nupdate_config=1\ncountry=GB\n\nnetwork={\n\tssid=" + "\"" + data[0] + "\"\n\tpsk="  + "\"" + data[1] + "\"\n}"
				wpaFile.write(wpaStr)
				wpaFile.close()
				for i in range(10):
					print("*\r")
					time.sleep(0.2)
				print("\nSSID and PASSWORD succesfull\n")
				print("Rebooting ...")
				time.sleep(2)
				os.system('sudo shutdown -r now')
			except:
				print("wpa_supplicant.conf file couldn't open !")
		else:
			print("No valid SSID or PASSWORD")
		
	time.sleep(1)
