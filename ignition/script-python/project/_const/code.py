# ==============================================
# project._const
# Last update: 2021-07-20
# Modified by: Rok Zupan
# ==============================================

#===========================================================
#
#   
#
#===========================================================

#===========================================================
# CONSTANTS
#===========================================================

# user inactivity time in seconds
userInactivity = 1200

# application version
applicationVersion = "1.0.0"

# Various data on gateway
hostname = system.tag.read("[System]Client/Network/GatewayAddress").value
company = system.tag.read("Factory/param_company").value
country = system.tag.read("Factory/param_country").value
name = system.tag.read("Factory/param_name").value
process = system.tag.read("Factory/param_process").value
code = system.tag.read("Factory/param_code").value

# Standard application title
appTitle = "%s - %s - %s (%s)  - SGA - TW Ovens - Operator UI - %s (Plantformance AG)"% (company, name, process, country, applicationVersion)

# project title - shown in the header
headerTitleDisplay = "<html><body>" + ""+ " &nbsp&nbsp&nbsp<font face='Open Sans Light'>" + applicationVersion + "</font></body></html>"

#======================================
# main menu tag path
#======================================
mainMenuTagPath = "factory/TW/param_main_menu"

#======================================
# main roles used in application
#======================================
operatorRole = "  _operator"
superuserRole = "  _superuser"


#=======================================
# PO docking window properties
#=======================================
defaultPODockingWindowPath = "_common/poDetailsThin"
poDockingWindowPath = "_common/poDetails"
poDockingWindowThinPath = "_common/poDetailsThin"
poDockingWindowEnableToggle = 1


# Standard logger for this application
def appLogger():
	return system.util.getLogger("  ")


#=======================================
# header icons definitions
#=======================================
#def icons(ignitionTagPath = ""):
#	if len(ignitionTagPath) > 0:
#		icons = {
#			"barcodeScanner": {
#				"position": 1,
#				"type": "hardware",
#				"templatePath": "[shared]gui/header/icons/barcodeScanner"
#			}
#
#		}
#	
#		return icons
#	return None	

def icons(ignitionTagPath = ""):
	if len(ignitionTagPath) > 0:
		icons = {
			"communicator": {
				"position": 1,
				"type": "software", 
				"templatePath": "[shared]gui/header/icons/commIcon",
#				"templatePath": "[SharedTemplates]/gui/header/icons/commIcon",
				"parameters": {
					"ignitionTagPath": (ignitionTagPath)
				}
			},	
			"trigger": {
				"position": 1,
				"type": "software", 
				"templatePath": "[shared]gui/header/icons/triggersIcon",
#				"templatePath": "[SharedTemplates]/gui/header/icons/triggersIcon",
				"parameters": {
					"ignitionTagPath": str(ignitionTagPath)
				}
			},
#			"barcodeScanner": {
#				"position": 1,
#				"type": "hardware",
#				"templatePath": "[shared]gui/header/icons/barcodeScanner"
##				"templatePath": "[SharedTemplates]/gui/header/icons/barcodeScanner"
#			},
			"fiTools": {
				"position": 1,
				"type": "software",
				"templatePath": "[shared]gui/header/icons/fiToolsIcon",
#				"templatePath": "[SharedTemplates]/gui/header/icons/fiToolsIcon",
				"parameters": {
					"ignitionTagPath": str(ignitionTagPath)
				}
			}

		}
	
		return icons
	return None	
#=======================================
# MES Colors
#=======================================
def getMESColors():
	MESColorHeaders = [
		"stateType",
		"modeType",
		"color",
		"mappedValue"
	]
	


	MESColorData = []
	MESColorData.append(["Unplanned Downtime","","ff582b","0"])
	MESColorData.append(["Planned Downtime","Other","7f8c8d","1"])
	MESColorData.append(["Blocked","","ff582b","2"])
	MESColorData.append(["Starved","","140,120,120","3"])
	MESColorData.append(["Running","Production","38cdbc","4"])
	MESColorData.append(["Idle","Idle","4d545b","5"])
	MESColorData.append(["Disabled","Disabled","ff582b","6"])
	MESColorData.append(["Changeover","Changeover","F39C12","0"])
	MESColorData.append(["Maintenance","Maintenance","ff582b","0"])
	MESColorData.append(["Unknown","","0,0,0","1000"])

	MESColorDataDS = system.dataset.toDataSet(MESColorHeaders, MESColorData)

	return MESColorDataDS

#=======================================
# Version
#=======================================

# Application version
def version():
	return applicationVersion
