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