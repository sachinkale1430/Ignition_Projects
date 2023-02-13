def setMaterialResourceProperty(mesObject, properties):
	"""
	Function to set internal MES complex properties on material resource complex property
	
	Parameters
	----------
	mesObject : MESAbstractObject
		Mes object, response or operation segment
	properties : dict
		Dict of all properties to write

	Returns
	-------
	MESObject
		Object with changes done
	"""	
	
	ratePeriod = "Min"
	scheduledRate = properties["calcScheduleRate"]["value"] if properties["calcScheduleRate"]["value"] else 10.0
	units = properties["operationUnits"]["value"] if properties["operationUnits"]["value"] else "PC"
	
	
	if mesObject.getMESObjectTypeName() == 'OperationsSegment' or mesObject.getMESObjectTypeName() == 'ResponseSegment':
		
		materialProp = mesObject.getComplexProperty('Material', 'Material Out')
		if materialProp:
			materialProp.setRatePeriod(ratePeriod)
			materialProp.setRate(float(scheduledRate))
			
			materialProp.setUnits(units)
	
	return mesObject

def setProductionSettingsProperty(mesObject, properties):
	"""
	Function to set intenal MES complex properties on production setting complex proeprty
	
	Parameters
	----------
	mesObject : MESAbstractObject
		Mes object, response or operation segment
	properties : dict
		Dict of all properties to write

	Returns
	-------
	dict,str
		Object with changes done 
	"""	
	oeeRate = properties["calcStandardRate"]["value"] if properties["calcStandardRate"]["value"] else 10.0
	units  = properties["operationUnits"]["value"] if properties["operationUnits"]["value"] else "PC"
	
	if mesObject.getMESObjectTypeName() == 'OperationsSegment' or mesObject.getMESObjectTypeName() == 'ResponseSegment':
		count = mesObject.getComplexPropertyCount('ProductionSettings')
		for complexPropNum in range(count):
			productionSettings = mesObject.getComplexProperty('ProductionSettings', complexPropNum)
			productionSettings.setOEERate(float(oeeRate))
			productionSettings.setOutfeedUnits(units)

		mesObject.setPropertyValue('ProductionSettings', productionSettings)

	
	return mesObject
	

def setEndTriggerProperty(mesObject, properties,changeOver = True):
	"""
	Function to set intenal MES complex properties on end trigger complex proeprty
	
	Parameters
	----------
	mesObject : MESAbstractObject
		Mes object, response or operation segment
	properties : dict
		Dict of all properties to write
	changeOver: bool
		if response / operation segment is changeover, set to true

	Returns
	-------
	dict,str
		Object with changes done 
	"""	
	duration = int(properties["calcSetupTime"]) if properties["calcSetupTime"] else 0
	auto = properties["autoChange"] if properties["autoChange"] else False

	try:
		if mesObject.getMESObjectTypeName() == 'OperationsSegment' or mesObject.getMESObjectTypeName() == 'ResponseSegment':
			endTrigger = mesObject.getPrimaryEndTrigger()
			endTrigger.setAuto(auto)
			endTrigger.setFixedDuration(duration)
	except:
		#Error getting material complex prop
		pass
	return mesObject