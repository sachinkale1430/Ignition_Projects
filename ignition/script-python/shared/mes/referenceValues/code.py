def getAllMesEquipment():
	#Build filter for Line objects
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName('Line')
	#Create list of Line objects
	lines = list(system.mes.searchMESObjects(filter))
	headers = ["value","name"]
	data = []
	#Loop through list and append UUIDs and Names
	for line in lines:
		data.append([line.getMESObjectUUID(), line.getName()])
		
	ds = system.dataset.toDataSet(headers, data)
	return ds
	
	
def getStatesFromEquipment(uuid):
	#Fucnction to list and append child states and format it in tree structrure fashion 
	def recursiveBuild(states, level = 1):
		data = []
		#Loop through state list, list and append child states for each state
		for state in states:
			children = state.getChildCollection().getList()
			props = state.getCustomProperties()
			val = ""
			#Search for value in downtRef custom property.
			for prop in props:	
				if "downtLossRef" in prop:
					try:
						val = round(float(state.getPropertyValue(prop)),2)
					except TypeError:
						val = ""
			if len(children) > 0:
				#Append formated data
				child = system.mes.loadMESObjects(children)
				data.append([state.getUUID(), '    '*level+state.getName(), str(val)])
				data += recursiveBuild(child, level + 1)
			else:
				data.append([state.getUUID(), '    '*level+state.getName(), str(val)])
		return data
		
	#Load MES object from Line uuid	
	machine = system.mes.loadMESObject(uuid)
	#List MES object custom properties and prepare machine name (for first row) 
	cpList = machine.getCustomProperties()
	machineName = machine.getName()
	#Find downtLossRefMax property value on line object and prepare an array
	if "downtLossRefMax" in cpList:
		val = round(float(machine.getPropertyValue("downtLossRefMax")),2)
		downtRefMax = [uuid,machineName,str(val)]
	else:
		downtRefMax = [uuid,machineName, ""]
	
	#Prepare state list for selected Line object
	statesUUID = machine.getEquipmentStateClassUUID()
	stateClass = system.mes.loadMESObject(statesUUID)
	states = stateClass.getChildCollection().getList()
	states = system.mes.loadMESObjects(states)
	#Send state list to recursiceBuild function and get formated dataset
	data = recursiveBuild(states)
	#Instert the first row for root level (machine and downtRefMax)
	data.insert(0,downtRefMax)
	
	header= ["uuid", "State", "Reference (%)"]
	#print data
	ds = system.dataset.toDataSet(header, data)
	return ds


def setCustomPropertyOnObject(uuid,name, customPropertyType, description, value, parent = None):
	# Compensating for sepasoft versioning issue, UPDATE VALUE ONLY
	sql = """
		UPDATE MESProperty 
		SET Value = ? 
		WHERE MESPropertyUUID = ?
		  """
	# Load object
	obj = system.mes.loadMESObject(uuid)
	props = obj.getCustomProperties()
	if parent:
		# The folder property is not existing add them
		if parent not in props:
			obj.addCustomProperty(parent, "String", '','', True, False, 'Reference values')
			obj.addCustomProperty(parent, name, "String", description,'', True, False, value)
			system.mes.saveMESObject(obj)
		# Folder is existing get parent
		else:	
			# get parent property
			prop = props[parent]
			# get sub properties
			child = prop.getChildProperties()
			if name in child:
				# 
				childUUID = child[name].getPropertyUUID()
				system.db.runPrepUpdate(sql, args=[value, childUUID], database="mes_analysis")
				# To refresh the values in the cache
				system.mes.invalidateCache()
			else:
				obj.addCustomProperty(parent, name, "String", description,'', True, False, value)
				system.mes.saveMESObject(obj)
				
	else:	
		# Check if property exists	
		if name not in props:
			obj.addCustomProperty(name, customPropertyType, description,'', True, False, value)
			system.mes.saveMESObject(obj)
		else:
			propertyUUID = props[name].getPropertyUUID()
			system.db.runPrepUpdate(sql, args=[value, propertyUUID], database="mes_analysis")
			# To refresh the values in the cache
			system.mes.invalidateCache()

def setCustomPropertyOnStateObject(uuid,name, customPropertyType, description, value):
	# Compensating for sepasoft versioning issue, UPDATE VALUE ONLY
	sql = """
		UPDATE MESProperty 
		SET Value = ? 
		WHERE MESPropertyUUID = ?
		  """
	# Load object
	obj = system.mes.loadMESObject(uuid)
	props = obj.getCustomProperties()
	
	# Check if property exists	
	add = None
	for prop in props:
		if "downtLossRef" in prop:
			add = prop
			break
			
	if not add:
		obj.addCustomProperty(name, customPropertyType, description,'', True, False, value)
		system.mes.saveMESObject(obj)

	else:
		propertyUUID = props[prop].getPropertyUUID()
		system.db.runPrepUpdate(sql, args=[value, propertyUUID], database="mes_analysis")
		# To refresh the values in the cache
		system.mes.invalidateCache()

				
def updateCustomPropertyValue(uuid, parent, name, value):
	#Prepare UPDATE sql clause
	sql = """
		UPDATE MESProperty 
		SET Value = ? 
		WHERE MESPropertyUUID = ?
	"""
	#Load MES Object
	obj = system.mes.loadMESObject(uuid)
	props = obj.getCustomProperties()
	if parent:
		# The folder property is not existing add them
		if parent in props:
			prop = props[parent]
			# get sub properties
			child = prop.getChildProperties()
			if name in child:
				# 
				childUUID = child[name].getPropertyUUID()
				system.db.runPrepUpdate(sql, args=[value, childUUID], database="mes_analysis")
				# To refresh the values in the cache
				system.mes.invalidateCache()
			else:
				obj.addCustomProperty(parent, name, "String", description,'', True, False, value)
				system.mes.saveMESObject(obj)

def updateCustomPropertyName(uuid, propNameOld, propNameNew, lossType):
	#Load MES Object
	mesObject = system.mes.loadMESObject(uuid)
	#Run rename function
	mesObject.renameCustomProperty(lossType+"."+propNameOld, propNameNew)
	#Save MES Object
	system.mes.saveMESObject(mesObject)

def deleteCustomProperty(uuid, propName, lossType):
	#Load MES Object
	obj = system.mes.loadMESObject(uuid)
	#Run remove function
	obj.removeCustomProperty(lossType+"."+propName)
	#Save MES object
	system.mes.saveMESObject(obj)
	
def deleteCustomPropertyOnObject(stateUUID, lossType):
	#Load state object
	stateObj = system.mes.loadMESObject(stateUUID)
	#Loop through custom property list, find lossType, delete and save
	cpList = stateObj.getCustomPropertiesFull()
	for cp in cpList:
		if lossType in cp:
			stateObj.removeCustomProperty(cp)
			system.mes.saveMESObject(stateObj)



def getReferenceLossCustomProperties(uuid, propName):
	#Prepare DS headers based on input parameter
	if propName == "speedLossRef":
		header = ["Machine sub-component", "Reference(%)", "propName"]
	elif propName == "qualLossRef":
		header = ["Reject category", "Reference(%)", "propName"]
	data = []
	
	#Load MES object
	machine = system.mes.loadMESObject(uuid)
	#Prepare machine name and "Max" ref value for first row
	machineName = machine.getName()
	cpList = machine.getCustomPropertiesFull()
	try:
		propMax = round(float(machine.getPropertyValue(propName+"Max")),2)
	except TypeError:
		propMax = ""
	#Prepare array for first row
	if propMax:
		data.append([machineName, str(propMax),propName+"Max"])
	else:
		data.append([machineName, "", propName+"Max"])
	#Loop through custom property list and find values for loss references. List and append		
	for key in cpList:
		splitKey = key.split(".")
		if len(splitKey) > 1:
			if splitKey[0] == propName:
				val = round(float(machine.getPropertyValue(key)),2)
				data.append(["   > "+splitKey[1], str(val),splitKey[1]])
	#Build and return dataset	
	lossDS = system.dataset.toDataSet(header, data)
	
	return lossDS


#===============================================
# Function that will collect all DOWNTIME reference values based on ignitionTagPath (pointer to MES_UDT instance)
#===============================================
def getDowntimeCustomProperties(ignitionTagPath):
	# build path to states dataset
	param_statesDatasetPath = ignitionTagPath + "/mes/param_statesDataset"
	
	if system.tag.exists(param_statesDatasetPath):
		# get all states from MES_UDT instance as dataset
		dataset = system.tag.read(param_statesDatasetPath).value
		
		if dataset.getRowCount() > 0:
			# load all states that have downtRef custom property > 0
			filter = system.mes.object.filter.createFilter()
			childFilter = system.mes.object.filter.createFilter()
			filter.setMESObjectTypeName("EquipmentState")
			filter.setCustomPropertyNamePattern("downtLossRef*")

			# get all results as objects
			results = system.mes.loadMESObjects(filter)
			system.mes.getMESObjectChildLinks(filter)
			# build list of UUID values based on filter
			statesWithDowntRef = []

			for link in results:
				cpList = link.getCustomPropertiesFull()
				stateCode = link.getStateCode()
				
				if len(cpList.keys()) > 0:

					customPropertyValues = link.getPropertyValue(cpList.keys()[0])
					if customPropertyValues != None:
						statesWithDowntRef.append([str(link.getUUID()), stateCode, customPropertyValues])
				
			# build 2nd list of all state UUID's from dataset
			machineStates = []
			for state in system.dataset.toPyDataSet(dataset):
				machineStates.append(str(state["UUID"]))
					
			# build interpolation of two lists
			results = [value for value in statesWithDowntRef if value[0] in machineStates]
			
			# based on results, we build dict as result
			json = {}	
			for result in results:
				json[result[1]] = round(float(result[2]), 2)
			
			return json

	return None


#===============================================
# Function that will collect all SPEED LOSS reference values based on equipment uuid
#===============================================
def getSpeedLossCustomProperties(equipmentUUID):
	try:
		objLink = system.mes.loadMESObject(equipmentUUID)
	except:
		return None
		
	# get all custom properties recursive for equipmentUUID
	customProperties = objLink.getCustomPropertiesFull()
	
	json = {}
	for customProperty in customProperties:
		# separate string in array
		# ex.: speedLossRef.A1
		key = customProperty.split(".")
		
		# if it is real key and not the folder (parent) itself
		if key[0] == "speedLossRef" and len(key) > 1:
			value = objLink.getPropertyValue(customProperty)
			json[str(key[1])] = round(float(value), 2)
		
	return json
	
	
#===============================================
# Function that will collect all QUALITY reference values based on equipment UUID
#===============================================	
def getQualLossCustomProperties(equipmentUUID):
	try:
		objLink = system.mes.loadMESObject(equipmentUUID)
	except:
		return None
		
	# get all custom properties recursive for equipmentUUID
	customProperties = objLink.getCustomPropertiesFull()
	
	json = {}
	for customProperty in customProperties:
		# separate string in array
		# ex.: qualLossRef.A1
		key = customProperty.split(".")
		
		# if it is real key and not the folder (parent) itself
		if key[0] == "qualLossRef" and len(key) > 1:
			value = objLink.getPropertyValue(customProperty)
			json[str(key[1])] = round(float(value), 2)
		
	return json
	

#===============================================
# Function that will collect all reference values and update MES_UDT instance
#===============================================	
def updateUdtFromCustomProperties(equipmentUUID):
	def valueCheck(value):
		if value is not None:
			return float(value)
		else:
			return None
			
			
	ignitionTagPath = shared.mes.equipment.findMesLinkUDTFromUUID(equipmentUUID)
	
	if ignitionTagPath:
		downtLoss = getDowntimeCustomProperties(ignitionTagPath)
		speedLoss = getSpeedLossCustomProperties(equipmentUUID)
		qualLoss = getQualLossCustomProperties(equipmentUUID)
	

		objLink = system.mes.loadMESObject(equipmentUUID)
		
		# get all custom properties recursive for equipmentUUID
		customProperties = objLink.getCustomProperties()
		
		downtLossRefMax = valueCheck(objLink.getPropertyValue("downtLossRefMax"))
		speedLossRefMax = valueCheck(objLink.getPropertyValue("speedLossRefMax"))
		qualLossRefMax = valueCheck(objLink.getPropertyValue("qualLossRefMax"))

	
		tagsToWrite = [
			ignitionTagPath + "/mes/references/downtLoss",
			ignitionTagPath + "/mes/references/qualLoss",
			ignitionTagPath + "/mes/references/speedLoss",
			ignitionTagPath + "/mes/references/machineDowntLoss",
			ignitionTagPath + "/mes/references/machineSpeedLoss",
			ignitionTagPath + "/mes/references/machineQualLoss"
		]
		
		tagValues = [
			system.util.jsonEncode(downtLoss),
			system.util.jsonEncode(qualLoss),
			system.util.jsonEncode(speedLoss),
			downtLossRefMax,
			speedLossRefMax,
			qualLossRefMax
		]
		
		system.tag.writeAll(tagsToWrite, tagValues)

#===============================================
# Function that will collect all material custom properties
#===============================================	

def getDataFromOperationDefinition(equipmentPath, materialName=None, fieldName=None):
    def getCustomProperties(obj, folder):
        json = {}
        cpList = obj.getCustomPropertiesFull()
        #Loop through custom property list and find values for folder 
        for key in cpList:
            splitKey = key.split(".")
            if len(splitKey) > 1:
                if splitKey[0] == folder:               
                    json[splitKey[1]] = cpList[key][1]
        return json
    equipmentPath = str(equipmentPath)
    # checks if equipment exists
    try:
        equipmentObject = system.mes.loadMESObjectByEquipmentPath(equipmentPath)
    except:
        return "No such equipment: "+equipmentPath
    customPropJson = getCustomProperties(equipmentObject,"propCustom")
    
    if materialName:
        materialName = str(materialName)
        # This checks if materials exists
        try:
            materialObject = system.mes.getMESObjectLinkByName("MaterialDef",materialName)
            matName = materialObject.getName()
            eqPath = equipmentObject.getEquipmentPath()
            eqPath =eqPath.replace("\\", ":")
            opDefName = matName+"-"+eqPath
            opDef = system.mes.loadMESObject(opDefName,"OperationsDefinition")
            customPropJson.update(getCustomProperties(opDef,"propCustom"))
        except:
            # Return default values configured on machine
            pass
    
    # Check if field needs to be extracted.
    if fieldName:
		tmp = {}
		for field in fieldName:
			if field in customPropJson:
				tmp[field] = customPropJson[field]
        #print tmp
		return tmp
    # Else return customPropJson
    return customPropJson