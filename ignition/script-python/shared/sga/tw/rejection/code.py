def buildRejectStructureTW(ignitionTagPath, clear = False):	
	"""
	Function that builds reject codes structure based on configuration made through factory configurator
	
	Parameters
	----------
	machineName: str
		name of the machine. coming from mes_udt param_stationName
	ignitionTagPath: str
		tag path to machine
		
	Returns
	-------
	dataset
		dataset that fits template repeater
	"""
	
	headers = [
		"code",
		"ignitionTagPath",
		"image",
		"wasteDescription",
		"wasteName",
		"familyId",
		"familyName"
	]
	
	headersProdRunRejects = [
		"rowId",
		"ignitionTagPath",
		"allocationFamilyArray",
		"allocationCodeArray",
		"code",
		"selectedFamily",
		"lastValue",
		"action"
	]
	
	headersFamilies = [
		"Selected Label",
		"Selected String Value"
	]
	
	dataProdRunRejects = 	[]
	family = 				[]
	dataFamilies = 			[]
	rejectionFamilies = 	[]
	data = 					[]
	
	prodRunRejectDict = 	{}
	prodDefaultRejectCode = {}
	

	
	
	if clear:
		dataDS = system.dataset.toDataSet(headers, data)
		#familiesDS = system.dataset.toDataSet(headersFamilies, dataFamilies)
		
		dataProdRunRejects.append([0, ignitionTagPath, system.util.jsonEncode(dataFamilies), system.util.jsonEncode([]), "", "", 0.0, "add"])
		
		prodRunRejectsDS = system.dataset.toDataSet(headersProdRunRejects, dataProdRunRejects)	
	
	
	else:
		tagsToRead = [
			ignitionTagPath + "/press/rejectionDeclaration/param_defaultRejectCode",
			ignitionTagPath + "/mes/param_stationName",
			ignitionTagPath + ""
		]
		
		tagValues = system.tag.readBlocking(tagsToRead)
		defaultRejectCode = tagValues[0].value
		machineName = tagValues[1].value
	
		try:
			ignitionTagPathQuery = ignitionTagPath.split("[default]")[1]
		except:
			pass
		sqlQuery = """
			SELECT 
				factory_reject_codes.id as 'id',
				factory_reject_codes.timestamp as 'Last Update',
				factory_reject_codes.rejectCode as 'Reject Code',	
				factory_reject_codes.rejectDescription as 'Description',
				factory_reject_codes.rejectDescriptionLocal as 'Local Description',
				CAST(factory_reject_codes.factoryErpCode as CHAR) as 'Factory Code',
				factory_reject_codes.enabled as 'enabled',
				factory_reject_codes.id_reject_family as 'Family Id',
				factory_reject_families.category as 'Family Name',
				factory_reject_codes.id_reject_image as 'Image Id'
			FROM factory_reject_codes 
			LEFT JOIN factory_reject_families ON factory_reject_families.id = factory_reject_codes.id_reject_family  
			WHERE 
				JSON_EXTRACT(factory_reject_codes.mesMachines, '$.\"""" + machineName + """\"') = '[default]""" + ignitionTagPathQuery + """'
		"""
	
		sqlResult = system.db.runQuery(sqlQuery, "factory_parameters")
		
		defaultCodeDescription = "Default rejection code"
		defaultCodeFamily = "Default" 
		
		for row in system.dataset.toPyDataSet(sqlResult):
			if row['enabled']:	
				
				familyIdValue = 0
				imageId = None
				appendData = 0
				
				familyName = row["Family Name"] if row["Family Name"] is not None else "Other"
				familyIdValue = row["Family Id"]
				appendData = 1	
				if row["Image Id"] > 0:
					imageId = str(row["Image Id"])
				else:
					imageId = None
						
				wasteName = row["Description"]
					
				if row["Local Description"] is not None:
					if len(row["Local Description"]) > 0: 
						wasteName = row["Local Description"]
				
				displayText = wasteName
				appendData = 1
				
				if row["Reject Code"] == defaultRejectCode:
					defaultCodeDescription = displayText
					if row["Family Name"] is not None:
						defaultCodeFamily = row["Family Name"]
				
				if familyName not in rejectionFamilies:
					rejectionFamilies.append(familyName)
					dataFamilies.append([familyName, familyName])
				
				
				if appendData == 1:
					data.append([
						row["Reject Code"],
						ignitionTagPath,
						imageId,
						row["Local Description"],
						displayText,
						familyIdValue,
						familyName
					])	
		
		
		dataDS = system.dataset.toDataSet(headers, data)
		#familiesDS = system.dataset.toDataSet(headersFamilies, dataFamilies)
		
		#dataProdRunRejects.append([0, ignitionTagPath, familiesDS, system.dataset.toDataSet(["Selected String Value", "Selected Label"],[]), "", "", 0.0, "add"])
		
		dataProdRunRejects.append([0, ignitionTagPath, system.util.jsonEncode(dataFamilies), system.util.jsonEncode([]), "", "", 0.0, "add"])

		 
		
		
		prodRunRejectsDS = system.dataset.toDataSet(headersProdRunRejects, dataProdRunRejects)
		
		prodRunRejectDict["delta"] = 			0.0
		prodRunRejectDict[defaultRejectCode] = 	0.0
		
		prodDefaultRejectCode["name"] = 		defaultRejectCode
		prodDefaultRejectCode["description"] = 	defaultCodeDescription 
		prodDefaultRejectCode["family"] = 		defaultCodeFamily 
		
	tagsToWrite = [
		ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejects",
		ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejectDict",
		ignitionTagPath + "/press/rejectionDeclaration/prod_defaultRejectCode",
		ignitionTagPath + "/press/rejectionDeclaration/prod_rejectCodes"
	]
	
	#print ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejects"
	valuesToWrite = [
		prodRunRejectsDS,
		system.util.jsonEncode(prodRunRejectDict),
		system.util.jsonEncode(prodDefaultRejectCode),
		dataDS
	]
	

	print system.tag.writeBlocking(tagsToWrite, valuesToWrite)	

def buildRejectStructureTW_old(ignitionTagPath, clear = False):	
	"""
	Function that builds reject codes structure based on configuration made through factory configurator
	
	Parameters
	----------
	machineName: str
		name of the machine. coming from mes_udt param_stationName
	ignitionTagPath: str
		tag path to machine
		
	Returns
	-------
	dataset
		dataset that fits template repeater
	"""
	
	headers = [
		"code",
		"ignitionTagPath",
		"image",
		"wasteDescription",
		"wasteName",
		"familyId",
		"familyName"
	]
	
	headersProdRunRejects = [
		"rowId",
		"ignitionTagPath",
		"allocationFamilyDs",
		"allocationCodeDs",
		"code",
		"selectedFamily",
		"lastValue",
		"action"
	]
	
	headersFamilies = [
		"Selected Label",
		"Selected String Value"
	]
	
	dataProdRunRejects = 	[]
	family = 				[]
	dataFamilies = 			[]
	rejectionFamilies = 	[]
	data = 					[]
	
	prodRunRejectDict = 	{}
	prodDefaultRejectCode = {}
	

	
	
	if clear:
		dataDS = system.dataset.toDataSet(headers, data)
		familiesDS = system.dataset.toDataSet(headersFamilies, dataFamilies)
		
		dataProdRunRejects.append([0, ignitionTagPath, familiesDS, system.dataset.toDataSet(["Selected String Value", "Selected Label"],[]), "", "", 0.0, "add"])
		
		prodRunRejectsDS = system.dataset.toDataSet(headersProdRunRejects, dataProdRunRejects)	
	
	
	else:
		tagsToRead = [
			ignitionTagPath + "/press/rejectionDeclaration/param_defaultRejectCode",
			ignitionTagPath + "/mes/param_stationName",
			ignitionTagPath + ""
		]
		
		tagValues = system.tag.readBlocking(tagsToRead)
		defaultRejectCode = tagValues[0].value
		machineName = tagValues[1].value
	
		try:
			ignitionTagPathQuery = ignitionTagPath.split("[default]")[1]
		except:
			pass
		sqlQuery = """
			SELECT 
				factory_reject_codes.id as 'id',
				factory_reject_codes.timestamp as 'Last Update',
				factory_reject_codes.rejectCode as 'Reject Code',	
				factory_reject_codes.rejectDescription as 'Description',
				factory_reject_codes.rejectDescriptionLocal as 'Local Description',
				CAST(factory_reject_codes.factoryErpCode as CHAR) as 'Factory Code',
				factory_reject_codes.enabled as 'enabled',
				factory_reject_codes.id_reject_family as 'Family Id',
				factory_reject_families.category as 'Family Name',
				factory_reject_codes.id_reject_image as 'Image Id'
			FROM factory_reject_codes 
			LEFT JOIN factory_reject_families ON factory_reject_families.id = factory_reject_codes.id_reject_family  
			WHERE 
				JSON_EXTRACT(factory_reject_codes.mesMachines, '$.\"""" + machineName + """\"') = '[default]""" + ignitionTagPathQuery + """'
		"""
	
		sqlResult = system.db.runQuery(sqlQuery, "factory_parameters")
		
		defaultCodeDescription = "Default rejection code"
		defaultCodeFamily = "Default" 
		
		for row in system.dataset.toPyDataSet(sqlResult):
			if row['enabled']:	
				
				familyIdValue = 0
				imageId = None
				appendData = 0
				
				familyName = row["Family Name"] if row["Family Name"] is not None else "Other"
				familyIdValue = row["Family Id"]
				appendData = 1	
				if row["Image Id"] > 0:
					imageId = str(row["Image Id"])
				else:
					imageId = None
						
				wasteName = row["Description"]
					
				if row["Local Description"] is not None:
					if len(row["Local Description"]) > 0: 
						wasteName = row["Local Description"]
				
				displayText = wasteName
				appendData = 1
				
				if row["Reject Code"] == defaultRejectCode:
					defaultCodeDescription = displayText
					if row["Family Name"] is not None:
						defaultCodeFamily = row["Family Name"]
				
				if familyName not in rejectionFamilies:
					rejectionFamilies.append(familyName)
					dataFamilies.append([familyName, familyName])
				
				
				if appendData == 1:
					data.append([
						row["Reject Code"],
						ignitionTagPath,
						imageId,
						row["Local Description"],
						displayText,
						familyIdValue,
						familyName
					])	
		
		
		dataDS = system.dataset.toDataSet(headers, data)
		familiesDS = system.dataset.toDataSet(headersFamilies, dataFamilies)
		
		dataProdRunRejects.append([0, ignitionTagPath, familiesDS, system.dataset.toDataSet(["Selected String Value", "Selected Label"],[]), "", "", 0.0, "add"])
		
		prodRunRejectsDS = system.dataset.toDataSet(headersProdRunRejects, dataProdRunRejects)
		
		prodRunRejectDict["delta"] = 			0.0
		prodRunRejectDict[defaultRejectCode] = 	0.0
		
		prodDefaultRejectCode["name"] = 		defaultRejectCode
		prodDefaultRejectCode["description"] = 	defaultCodeDescription 
		prodDefaultRejectCode["family"] = 		defaultCodeFamily 
		
	tagsToWrite = [
		ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejects",
		ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejectDict",
		ignitionTagPath + "/press/rejectionDeclaration/prod_defaultRejectCode",
		ignitionTagPath + "/press/rejectionDeclaration/prod_rejectCodes"
	]
	
	valuesToWrite = [
		prodRunRejectsDS,
		system.util.jsonEncode(prodRunRejectDict),
		system.util.jsonEncode(prodDefaultRejectCode),
		dataDS
	]
	
	system.tag.writeBlocking(tagsToWrite, valuesToWrite)	


def formatTWRejectJson(ignitionTagPath):
		
	def convertVariableToFloat(variable, targetType = "float"):
		if variable:
			if isinstance(variable, basestring):
				return float(variable) if len(variable) else 0.0
			elif isinstance(variable, int):
				return float(variable)
			else:
				try:
					return variable.floatValue()
				except:
					#return float(variable)
					return (variable)
		else:
			return 0.0
		
	tagsToRead = [
		ignitionTagPath + "/press/rejectionDeclaration/prod_prodRunRejectDict",
		ignitionTagPath + "/press/rejectionDeclaration/prod_defaultRejectCode",
		ignitionTagPath + "/press/analysis/scopeProductionRun/prod_rejectedWheels"
	]
	
	tagValues = system.tag.readBlocking(tagsToRead)

	prod_prodRunRejectDict 	= tagValues[0].value.toDict() 
	prod_defaultRejectCode 	= tagValues[1].value.toDict()
	prod_rejectedWheels		= tagValues[2].value

	rejectedWheels = convertVariableToFloat(prod_prodRunRejectDict['delta'], targetType = "float") + prod_rejectedWheels
	
	del prod_prodRunRejectDict['delta']
	
	for key, value in prod_prodRunRejectDict.iteritems():
		if value < 1:
			del prod_prodRunRejectDict[key] 
	
	prod_prodRunRejectDict[prod_defaultRejectCode['name']] = rejectedWheels
	
	return prod_prodRunRejectDict
