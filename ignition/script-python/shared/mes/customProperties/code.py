def getCustomProperties(mesObject, customPropertyNames=None):
	"""
	Function  to get multiple custom properties from a single MES object.
	
	Parameters
	----------
	mesObject : AbstractMESObject
		MES object from which to get custom properties
	customPropertyNames : list default None
		List of property names to get. If none get all custom properties
	
	Returns
	-------
	dict
		Dict of properties that were requested, if none get all properties
	"""
	# helper function to get customProperties out of folder
	def getCustomProperty(obj, cpFilter):
		json = {}
		cpList = obj.getCustomPropertiesFull()
	    #Loop through custom property list and find values for folder 
		
		for cp in cpList:
			splited = cp.split(".")
			#print splited
			cpName = splited[-1]
			if cpFilter:
				if cpName in cpFilter:
					propValues = cpList[cp]
					json[cpName] = {"type":propValues[0], 
									"value":propValues[1],
									"description":propValues[2],
									"units":propValues[3],
									"productionVisible":propValues[4],
									"required":propValues[5],
									"fullPath":cp} 
			else:
				propValues = cpList[cp]
				json[cpName] = {"type":propValues[0], 
								"value":propValues[1],
								"description":propValues[2],
								"units":propValues[3],
								"productionVisible":propValues[4],
								"required":propValues[5],
								"fullPath":cp} 
		return json	
		
	customPropJson = {}
	# TODO check if its abstractMESObject
	if mesObject:
		customPropJson = getCustomProperty(mesObject, customPropertyNames)
		if customPropertyNames:
			for cp in customPropertyNames:
				if cp not in customPropJson:
					customPropJson[cp] = {"type":None, 
									"value":None,
									"description":None,
									"units":None,
									"productionVisible":None,
									"required":None,
									"fullPath":None} 
	return customPropJson
	
def setCustomProperties(mesObject, customPropertyNames):
	"""
	Function to set multiple custom properties to a single MES object.
	
	Parameters
	----------
	mesObject : AbstractMESObject
		MES object to set custom properties on
	customPropertyNames : list default None
		List of properties to save. Uses same format as getCustomProperties

	
	Returns
	-------
	dict
		Dict of properties with status of each if it was saved.
	"""
	
	
	def recursiveCheck(obj,props,path,finalPropAttributes,fullPath="", level=0):
		
		splitted = path.split(".")
		parent = None
		
		# Get name and parent out of path
		if level == 0:
			name = splitted[level]
		else:
			name = splitted[level]
			parent = splitted[level-1] 
			
		
		# Compensating for sepasoft versioning issue, UPDATE VALUE ONLY
		sql = """
			UPDATE MESProperty 
			SET Value = ? 
			WHERE MESPropertyUUID = ?
		  """
		
		if fullPath == "":
			fullPath = name
		else:
			fullPath =fullPath + "." + name
		
		# Check if on the last level of recursion
		if len(splitted)>level+1:
		
			if fullPath not in props:
				if parent == None:
					obj.addCustomProperty(name, "String", '','', True, False, "Folder")
				else:
					obj.addCustomProperty(fullPath,name, "String", '','', True, False, "Folder")
					
			obj = recursiveCheck(obj,props,path,finalPropAttributes,fullPath=fullPath, level=level+1)
			
		else:
			# Get inner json properties
			customPropertyType = finalPropAttributes["type"] if "type" in finalPropAttributes else "String"
			description = finalPropAttributes["description"] if "description" in finalPropAttributes else ""
			value = finalPropAttributes["value"] if "value" in finalPropAttributes else "Value missing"
			units = finalPropAttributes["unit"] if "unit" in finalPropAttributes else "Value missing"
			# Add the edge custom property		
			#print fullPath		
			if fullPath not in props:
				
				if parent:
					obj.addCustomProperty(fullPath, name, customPropertyType, description,units, True, False, value)
				else:
					obj.addCustomProperty(name, customPropertyType, description,units, True, False, value)
			# Update the value of custom property
			else:
				prop = props[fullPath]
				propUUID = prop.getPropertyUUID()
				system.db.runPrepUpdate(sql, args=[value, propUUID], database="mes_analysis")
				# To refresh the values in the cache
				system.mes.invalidateCache()
		return obj
	
	# Load object
	# TODO make checks if abstractMESObject (check sourcecode for location)
	if mesObject:
	
		obj = mesObject
		props = obj.getCustomProperties()
		# Go over the json of custom properties and add them to MES object
		for customProperty in customPropertyNames:

			obj = recursiveCheck(obj, props,customProperty, customPropertyNames[customProperty])
		
		mesObject = obj	
		#system.mes.saveMESObject(obj)
	return mesObject	

def getResponseSegment(startDate, endDate, equipmentPath, material=None):
	"""
	Temporary solution to get MESresponseSegment between two date for specific equipment. PCS requirement
	
	TODO: Fix checking on inputs, error handling.
	
	Parameters
	----------
	startDate : java.util.Date
		Start date of production runs
	endDate : java.util.Date
		End date of production runs 
	equipmentPath: str
		equipment path where to search 
	material: str default None
		Optional argument to search for specific material

	Returns
	-------
	List
		list of dict of custom properties save on responseSegment plus beginDate and endDate
	"""
	sql="""
	SELECT * FROM MESResponseSegment
	WHERE (BeginDateTime >= ? and EndDateTime <= ?  or EndDateTime is NULL)
	"""
	eqPath = equipmentPath.replace("\\", ":")
	if material:
		sql+= "and Name = '"+material+"-"+eqPath+"'"
	else:
		sql+= "and Name like '%-"+eqPath+"'"
	sql+=" ORDER BY BeginDateTIme ASC"
	ds = system.db.runPrepQuery(sql, args=[startDate,endDate], database="mes_analysis")
	
	responseSegments = []
	
	for x in ds:
		tmpJson = {"BeginDateTime":x["BeginDateTime"], "EndDateTime":x["EndDateTime"]}
		try:
			mesObject = system.mes.loadMESObject(x["MESResponseSegmentUUID"]) 
			tmpJson.update(getCustomProperties(mesObject))
			  #shared.mes.customProperties.getCustomProperties(mesObject, customPropertyNames=None)
		except:
			pass
		responseSegments.append(tmpJson)
	
	return responseSegments