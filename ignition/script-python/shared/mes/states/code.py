def getStateCode(ignitionTagPath, stateName):
	"""
	Function that will return state code based on name
	
	Parameters
	----------
	ignitionTagPath: str
		path to machine tag pointer (MES_UDT)
	stateName: str
		name of the state to search for
		
	Returns
	-------
	stateCode: str
		state code
	"""	
	def searchForStateCode(json, key):
		array = []
	
		def extract(json, array, key):
			result =  None
			
			if isinstance(json, dict):
				for k, v in json.items():
					if isinstance(v, dict):
						extract(v, array, key)
					if k == key:
						result = v["Code"]
						array.append(result)
	
			return array
			
		results = extract(json, array, key)
	
		return results


	statesDefinitionValues = system.tag.read(ignitionTagPath + "/mes/param_statesJson").value
	statesDefinitionValues = system.util.jsonDecode(statesDefinitionValues)
	stateCode = searchForStateCode(statesDefinitionValues, stateName)
	
	if len(stateCode)  > 0:
		return stateCode[0]
	else:
		return None


def setStateOnEquipment(ignitionTagPath, stateName):
	"""
	Function that will set state code based on state name
	
	Parameters
	----------
	ignitionTagPath: str
		path to machine tag pointer (MES_UDT)
	stateCode: str
		state code to be set
		
	Returns
	-------
	none
	"""	
	
	statePath = ignitionTagPath + "/mes/oee_state"
	
	stateCode = getStateCode(ignitionTagPath, stateName)
	
	if stateCode is not None:
		system.tag.write(statePath, stateCode)
	else:
		logger = system.util.getLogger("STE-GW-Tags")
		content = "Cannot set state code for state name " + str(stateName) + " (" + ignitionTagPath + ")"
		logger.info(content)
	