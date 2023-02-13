def recordToLog(content):
	logger = system.util.getLogger("PFCE-TW-Press")
	logger.info(content)
	

def diagnosticsChanged(tagPath, value):
	return
	def MSB(n):
		ndx = 0
		while ( 1 < n ):
			n = ( n >> 1 )
			ndx += 1
		return ndx
			
	tagPathParts = tagPath.split("/")
	
	# If value has changed to zero, basically nothing to do
	# (state is change automatically to run when a cycle is finished)
	if value == 0:
		return
	
	# First we want to make sure that machine is running
	# If not, then we will not do anything
	ignitionTagPath =  tagPathParts[0] + "/" + tagPathParts[1] + "/" + tagPathParts[2] + "/" + tagPathParts[3]
	currentState = system.tag.read(ignitionTagPath + "/mes/oee_state").value
    
	tagsToRead = [
		ignitionTagPath + "/signals/cycles/prod_cycleDynamicT2",
		"SGA/Kolo/TW/P55/press/param_cycleTimeTarget"
	]
    
	tagValues = system.tag.readAll(tagsToRead)

	dynamicCycleTime = tagValues[0].value
	cycleTimeTarget = tagValues[1].value
    
    # if current state is not in production, skip this action
	#if currentState <> 1 or (dynamicCycleTime < cycleTimeTarget):
	if currentState <> 1:
		return
		
	# Otherwise we will calculate the index of state in the excel file
	tagName  = tagPathParts[-1]
	table = tagName[5] + tagName[6]

	# Table = 1000 for T1, 2000 for T2
	if table == "t1":
		state = 10000
	elif table == "t2":
		state = 20000
	else:
		return

	# Get station : multiply by 100
	station = tagName[3]
		
	#if (tagName[3] in "0123456789") and (tagName[7] in '01'):
	if (tagName[3] in "0123456789") and (tagName[8] in '12'):
		state = (state + int(station) * 100)
	else:
		recordToLog("Station not found!")
		return
	
	# Get high or low dword ? returns 1 or 2 (first or second)
	dword = tagName[-1:]
	
	if dword == "1":
		state = state + 1   # index 0 is for the station
	elif dword=="2":
		state = state + 33  # index 0 for station, +32 for next dword
	else:
		recordToLog("Status word not correct!")
		return

	# Add the MSB to the state	
	ror = lambda val, r_bits, max_bits: \
		    ((val & (2**max_bits-1)) >> r_bits%max_bits) | \
		    (val << (max_bits-(r_bits%max_bits)) & (2**max_bits-1))
	
	rorBit = ror(value, 16, 32)
	state = state + int(MSB(rorBit))

	# If we are sure it is a stoppage (there is a match in list)
	stateDetails = shared.mes.equipment.getEquipmentStateOptions(ignitionTagPath, state)
	
	if stateDetails:
		if stateDetails["Name"] == "Unknown State":
			system.tag.write(ignitionTagPath + "/mes/oee_state", 1)
		else:
			system.tag.write(ignitionTagPath + "/mes/oee_state", state)


# NEW MATERNINI DIAGNOSTICS SCRIPT
def diagnosticsChangedNew(tagPath, value):
	def MSB(n):
		ndx = 0
		while ( 1 < n ):
			n = ( n >> 1 )
			ndx += 1
		return ndx
			
	tagPathParts = tagPath.split("/")
	
	# If value has changed to zero, basically nothing to do
	# (state is change automatically to run when a cycle is finished)
	if value == 0:
		return
	
	# First we want to make sure that machine is running
	# If not, then we will not do anything
	ignitionTagPath =  tagPathParts[0] + "/" + tagPathParts[1] + "/" + tagPathParts[2] + "/" + tagPathParts[3]
	
	prod_tempStateCode = system.tag.read(ignitionTagPath + "/press/cycles/current/prod_tempStateCode").value
	
	# exit if it is not the 1st alarm
	if prod_tempStateCode is not None:
		return
		
	# otherwise we will calculate the index of state in the excel file
	tagName  = tagPathParts[-1]
	table = tagName[5] + tagName[6]

	# Table = 1000 for T1, 2000 for T2
	if table == "t1":
		state = 10000
	elif table == "t2":
		state = 20000
	else:
		return

	# Get station : multiply by 100
	station = tagName[3]
	
	#if (tagName[3] in "0123456789") and (tagName[7] in '01'):
	if (tagName[3] in "0123456789") and (tagName[8] in '12'):
		state = (state + int(station) * 100)
	else:
		recordToLog("Station not found!")
		return
	
	# Get high or low dword ? returns 1 or 2 (first or second)
	dword = tagName[-1:]
	
	if dword == "1":
		state = state + 1   # index 0 is for the station
	elif dword=="2":
		state = state + 33  # index 0 for station, +32 for next dword
	else:
		recordToLog("Status word not correct!")
		return

	# Add the MSB to the state	
	ror = lambda val, r_bits, max_bits: \
			((val & (2**max_bits-1)) >> r_bits%max_bits) | \
			(val << (max_bits-(r_bits%max_bits)) & (2**max_bits-1))
	
	rorBit = ror(value, 16, 32)
	state = state + int(MSB(rorBit))

	system.tag.write(ignitionTagPath + "/press/cycles/current/prod_tempStateCode", state)




def getTagValues(parentPath = "", tagPath = "*"):
	"""
	Function that scans tag folder path defined and provides array of values
	
	Parameters
	----------
	factory : tagPath
		path to folder where tag values will be captured
	
	Returns
	-------
	list
		list of tag values ordered, if no values found, return None
	"""	

	tags = system.tag.browseTags(parentPath = parentPath, tagPath = tagPath, sort = "ASC")
			
	tagsToRead = []
	tagValuesData = []
	
	tagsData = {}
	
	# loop through tags	
	for tag in tags:
		tagsToRead.append(tag.fullPath)
		
	tagValues = system.tag.readAll(tagsToRead)

	# get values and build list
	for tagValue in tagValues:
		tagValuesData.append(tagValue.value)	
		
	# if values are available, return content, otherwise None
	if len(tagValuesData) > 0:
		data = {
			'tagValues': tagValuesData
		}
		
		if any(x in parentPath for x in ["thickness", "weight"]):
			data['meanValue'] = system.math.mean(tagValuesData)
		
	else:
		return None
	
	return data
