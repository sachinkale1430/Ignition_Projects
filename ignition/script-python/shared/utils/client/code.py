#======================================
#======================================
# shared.utils.client
# Rok
# 2018-09-14
#======================================
#======================================

#======================================
# check station ip in main menu parameter config
#======================================
def checkStation(mainMenuPath="[default]factory/TW/param_main_menu"):
	clientIP = system.tag.read("[System]Client/Network/IPAddress").value	
	data = system.tag.read(mainMenuPath).value
	dataPy = system.dataset.toPyDataSet(data)

	result = None
	
	for row in dataPy:
		if row["stationIP"] == clientIP:
			result = {
				"clientIP" : clientIP,
				"ignitionTagPath" : row["ignitionTagPath"],
				"windowsToOpen": row["windowsToOpen"]
			}
			
			break

	return result

	
#======================================
# detect missing operator state
# NOT USED CURRENTLY
#======================================
def changeStateOfMissingOperator(requiredRole):
	if requiredRole in system.security.getRoles():
		if system.security.isScreenLocked():
			station = checkStation()
			
			if station:
				system.tag.write(station["ignitionTagPath"] + "/mes/oee_state", 730)
				
				
#======================================
# open dedicated windows
# client startup script to detect correct ip address and roles.
# based on that it will open dedicated windows defined in tag factory/param_main_menu
#======================================
def clientStartup(requiredOperatorRole, mainMenuPath = "factory/param_main_menu"):
	if requiredOperatorRole in system.security.getRoles():
		station = checkStation(mainMenuPath)
		if station:
			windowsToOpen = station["windowsToOpen"].replace(" ", "")
			windowsToOpen = windowsToOpen.split(",")
			for window in windowsToOpen:
				if len(window) > 0:
					system.nav.openWindow(window, {"ignitionTagPath": station["ignitionTagPath"]})
		else:
			clientIP = system.tag.read("[System]Client/Network/IPAddress").value
			system.gui.messageBox("<html>Cannot find correct station configuration. Please contact Plantformance AG<br><br>Required role: <b>" + requiredOperatorRole + "</b><br>Client IP: <b>" + clientIP + "</b></html>")		
	elif "windows" not in system.util.getGlobals():		
		system.nav.openWindow("_common/mainMenu")


#####################################################
#check who loged into the system and write to prod_operator tag
#####################################################
def checkUserAndStoreToAdditionalFactor(mainMenuTagPath):
	username = system.security.getUsername()
	clientIP = system.tag.read("[System]Client/Network/IPAddress").value
	mainMenu = system.tag.read(mainMenuTagPath).value
	
	mainMenuPy = system.dataset.toPyDataSet(mainMenu)
	try:
		for row in mainMenuPy:
			if clientIP == row["stationIP"]:
				ignitionTagPath = row["ignitionTagPath"]
				if system.tag.exists(ignitionTagPath + "/MESProcessTags/prod_operator"):
					system.tag.write(ignitionTagPath + "/MESProcessTags/prod_operator", username)
	except:
		pass



def decryptCode():
	"""
	Function for decrypting Plantformance owned python code
	"""
	import project
	from jarray import array
	from org.python.core import imp
	from java.io import ByteArrayInputStream
	# Maybe make it more dynamic? to be discussed
	modules = ["nfc", "barcode", "sylvacCaliper"]
	
	for module in modules:	
			
		# Read dataset where java bytecode is stored
		code = system.util.sendRequest("pfce_sga_interfaces", "getEncryptedCode", payload={"module": module})
		
		# Transform bytes into input stream
		bytes =  ByteArrayInputStream(code["bytes"])
		
		# Get compiled module from java bytecode
		script = imp.loadFromCompiled(code["scriptName"],bytes,"bytes",code["scriptName"]+".py")
		
		# Add to the project scripts. 
		project.__dict__[code["scriptName"]] = script
		
		
