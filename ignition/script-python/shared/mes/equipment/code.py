#===============================================
#===============================================
#
# mes.equipment.*
#
# Functions in relation with equipment, modes, states
#
#===============================================
#===============================================



#===============================================
# Update equipment STATE tags
# 
#===============================================
def updateEquipmentStateTagsOld( Ignition_Path ):

	MES_Path = system.tag.read( Ignition_Path+"/mes/param_mesObject").value

	headers = ['TypeName', 'Code', 'Name', 'Type', 'OverrideScope', 'ShortStopThreshold', 'OverrideSetting', 'UUID']
	data = []

	# Equipment states need recursivity
	def getChildren( parentUUID ):
		list = {}
		for child in system.mes.getEquipmentStateOptions(MES_Path, parentUUID, ""):
			newObj = {}
			name = str(child.getName())
			newObj["UUID"]= str(child.getUUID())
			newObj["Name"]= name
			newObj["Code"]= child.getStateCode()
			newObj["OverrideScope"]= str(child.getStateOverrideScope())
			newObj["OverrideSetting"]= str(child.getStateOverrideSetting())
			newObj["ShortStopThreshold"]= child.getStateShortStopThreshold()
			newObj["Type"]= str(child.getStateType())
			newObj["TypeName"]= str(child.getStateTypeName())
			data.append(newObj.values())
			children = getChildren(child.getUUID())
			if len(children) > 0:
				newObj["children"] = children
			list[name] = newObj
		return list
	
	objLink = system.mes.getMESObjectLinkByEquipmentPath(MES_Path)
	lineObj = objLink.getMESObject()
	eqStateClassUUID = lineObj.getEquipmentStateClassUUID()

	# Write list of states as structured json tree	
	json = { "children" : getChildren( eqStateClassUUID ) }
	json_path = Ignition_Path + "\mes\param_statesJson"
	system.tag.write( json_path, system.util.jsonEncode(json))

	# Write list of states as flat dataset
	dataset_path = Ignition_Path + "\mes\param_statesDataset"
	ds = system.dataset.toDataSet( headers, data)
	system.tag.write( dataset_path, ds)

def updateEquipmentStateTags( Ignition_Path ):

	MES_Path = system.tag.read( Ignition_Path+"/mes/param_mesObject").value

	headers = ['TypeName', 'Code', 'Name', 'Type', 'OverrideScope', 'ShortStopThreshold', 'OverrideSetting', 'UUID', 'fullPath']
	data = []

	# Equipment states need recursivity
	def getChildren(parentUUID, path=""):
		list = {}
		for child in system.mes.getEquipmentStateOptions(MES_Path, parentUUID, ""):
			newObj = {}
			name = str(child.getName())
			newObj["UUID"]= str(child.getUUID())
			newObj["Name"]= name
			newObj["Code"]= child.getStateCode()
			newObj["OverrideScope"]= str(child.getStateOverrideScope())
			newObj["OverrideSetting"]= str(child.getStateOverrideSetting())
			newObj["ShortStopThreshold"]= child.getStateShortStopThreshold()
			newObj["Type"]= str(child.getStateType())
			newObj["TypeName"]= str(child.getStateTypeName())
			data.append(newObj.values() + [path+"/"+name])
			children = getChildren(child.getUUID(), path+"/"+name)
			if len(children) > 0:
				newObj["children"] = children
			list[name] = newObj
			
		return list
	
	objLink = system.mes.getMESObjectLinkByEquipmentPath(MES_Path)
	lineObj = objLink.getMESObject()
	#print lineObj
	eqStateClassUUID = lineObj.getEquipmentStateClassUUID()
	
	rootEquipmentState = system.mes.getMESObjectLink(eqStateClassUUID)
	# Write list of states as structured json tree	
	json = { "children" : getChildren(eqStateClassUUID, rootEquipmentState.getName()) }
	json_path = Ignition_Path + "\mes\param_statesJson"
	system.tag.write( json_path, system.util.jsonEncode(json))

	# Write list of states as flat dataset
	dataset_path = Ignition_Path + "\mes\param_statesDataset"
	ds = system.dataset.toDataSet( headers, data)
	
	system.tag.write( dataset_path, ds)
#===============================================
# Update equipment MODE tags
# 
#===============================================
def updateEquipmentModeTags( Ignition_Path ):

	MES_Path = system.tag.read( Ignition_Path+"/mes/param_mesObject").value

	headers = ['TypeName', 'MESObjectType', 'Code', 'Name', 'Type', 'IncludeProductionCounts', 'UUID', 'IncludeInOEE']
	data = []
	
	json ={}
	for child in system.mes.getEquipmentModeOptions(MES_Path, ""):
		newObj = {}
		name = str(child.getName())
		newObj["UUID"]= str(child.getUUID())
		newObj["Name"]= name
		newObj["IncludeInOEE"]= child.getIncludeInOEE()   # boolean
		newObj["IncludeProductionCounts"]= child.getIncludeProductionCounts() # boolean
		newObj["MESObjectType"]= str(child.getMESObjectType())
		newObj["Code"]= child.getModeCode()
		newObj["Type"]= str(child.getModeType())
		newObj["TypeName"]= str(child.getModeTypeName())
		json[name] = newObj
		data.append(newObj.values() )

	# Write list of modes as structured json tree	
	json = { "children" : json }
	json_path = Ignition_Path + "\mes\param_modesJson"
	system.tag.write( json_path, system.util.jsonEncode(json))

	# Write list of modes as flat dataset
	dataset_path = Ignition_Path + "\mes\param_modesDataset"
	ds = system.dataset.toDataSet( headers, data)
	system.tag.write( dataset_path, ds)
	
#===============================================
# getEquipmentStateOptions
# 
# Returns all the standard data corresponding to an equipment, and a code
#===============================================	
def getEquipmentStateOptions(ignitionTagPath, stateCode):
	statesDatasetPath = ignitionTagPath + "\mes\param_statesDataset"

	statesDS = system.tag.read(statesDatasetPath).value

	for row in system.dataset.toPyDataSet(statesDS):
		if row["Code"] == stateCode:
			return row

	return False


#===============================================
# Function that will based on uuid and generate ignitionTagPath (pointer to root folder of equipment in tags)
#===============================================
def findMesLinkUDTFromUUID(uuid):
	try:
		MESObject = system.mes.loadMESObject(uuid)
	except:
		return None
		
	equipmentPath = MESObject.getEquipmentPath()

	mesUDTs = system.tag.browse("[default]", {"tagType":"UdtInstance", "name":"mes", "recursive":True}).results
	tagsToRead = []
	

	tags = []
	tagPaths = []
	
	# build the list of tags to read to find mapping
	for mesUDT in mesUDTs:
		
		param_mesObjectPath = str(mesUDT["fullPath"]) + "/param_mesObject"
		
		tags.append(param_mesObjectPath)
		tagPaths.append((str(mesUDT["fullPath"])).replace("/mes", ""))
		
	tagValues = system.tag.readBlocking(tags)
	
	# find if there is a match between ignitionTagPath and MESObject
	for x in range(0, len(tagValues)):
		if equipmentPath in tagValues[x].value:
			return str(tagPaths[x])
	return None
	
def getCompatibleWorkcenterList(eqPath):
	"""
	Last modified / date: I.Felicijan / 2020-11-04
	
	Function that checks if there are any machine with equal machineGroup custom property value
	
	Parameters
	----------
	eqPath : string
		MES Equipment path
	
	Returns
	-------
	workcenterList
		List with compatible Workcenters. Return empty if none
	""" 
	#Load MES Object and get machineGroup value
	obj = system.mes.loadMESObjectByEquipmentPath(eqPath)
	machineGroup = obj.getPropertyValue('machineGroup')
	currentWorkcenter = obj.getPropertyValue('sap_work_center')
	workcenterList = []
	# If there is a value in machineGroup custom property and sap_work_center
	# create custom property filter to check if there are any machineGroup properties with same value
	
	if machineGroup and currentWorkcenter:
		filter = system.mes.object.filter.createFilter()
		filter.setEnableStateName('ENABLED')
		filter.setMESObjectTypeName('Line')
		list = system.mes.object.filter.parseCustomPropertyValueFilter('machineGroup = '+machineGroup)
		filter.setCustomPropertyValueFilter(list)
		compatibleMachines = system.mes.loadMESObjects(filter)
		if len(compatibleMachines) > 1:		
			for machine in compatibleMachines:
				workcenter = machine.getPropertyValue('sap_work_center')
				if workcenter != currentWorkcenter and  workcenter != None:
					workcenterList.append(workcenter)
	
	return workcenterList

def canRunThisPo(productionOrder, currentWorkCenter, eqPath):
	"""
	Last modified by / date: Izidor Felicijan / 2020-11-06
	
	Function returns True or False if workcenter can run provided PO.
	It will check if currentWorkCenter either belongs to the PO or if it belongs to the same machineGroup as machine on PO
	
	Parameters
	----------
	productionOrder : string
		PO number
	
	currentWorkCenter : string
		Workcenter of current machine
	
	eqPath : string
		MES Eqipment path of current machine
	 
	Returns
	-------
	True or False
		Can run or cannot run
	""" 
	fields = ["workOrderName", "operationNumber", "workCenter"]
	productionOrderData = shared.mes.workorder.getSAPProductionStepDetails(workOrderName = productionOrder, fields = fields)
	compatibleWorkcenterList = shared.mes.equipment.getCompatibleWorkcenterList(eqPath)
	if len(productionOrderData) > 0:
		for row in productionOrderData:
			if row['workCenter'] == currentWorkCenter or (row['workCenter'] in compatibleWorkcenterList):
				return True
	return False
		
def syncWorkcenterFromUdtToMesObject():
	"""
	Last modified / date: I.Felicijan / 2021-09-07 (adapted for 8.1)
	
	Function that synchronizes SAP Workcenter from MES UDT to MES Equipment object custom property 'sap_work_center'
	
	Parameters
	----------
	None
	
	Returns
	-------
	None
		Adds or sets MES Equipment object custom property sap_work_center
	""" 
	#Browse all MES UDTs
	#system.tag.browse
	mesTagPaths = system.tag.browse("[default]", {"tagType":"Udtinstance", "name":"mes", "recursive":True}).results
	tagsToRead = []
	#Loop and append param_mesObject and param_sapWorkCenter tags to get equipment path and workcenter
	for mesTagPath in mesTagPaths:
		workcenter = str(mesTagPath["fullPath"]) + "/param_sapWorkCenter"
		equipmentPath = str(mesTagPath["fullPath"]) + "/param_mesObject"
		tagsToRead.append(equipmentPath)
		tagsToRead.append(workcenter)
	
	# Read list of tags
	tagValues = system.tag.readBlocking(tagsToRead)
	obj = None
	#Because the order is Equipment path - Workcenter we need to load object by eqPath first item and perform action on second item
	for tagValue in tagValues:
		var = str(tagValue.value)
		if var[:3] == 'SGA':	
			obj = system.mes.loadMESObjectByEquipmentPath(var)
		else:
			if obj and len(var) > 0:
				if 'sap_work_center' in obj.getCustomProperties():
					obj.setPropertyValue('sap_work_center', var)
					try:
						system.mes.saveMESObject(obj)
						print 'WILL SET ' +var+ ' TO ' + obj.getEquipmentPath()
					except:
						print 'FAILED SETTING ' +var+ ' TO ' + obj.getEquipmentPath()
				else:
					try:
						obj.addCustomProperty('sap_work_center', 'String', 'SAP Workcenter', '', True, False)
						system.mes.saveMESObject(obj)
						print 'WILL ADD ' +var+ ' TO ' + obj.getEquipmentPath()
					except:
						print 'FAILED ADDING ' +var+ ' TO ' + obj.getEquipmentPath()
			obj = None