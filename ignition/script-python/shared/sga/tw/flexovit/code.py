def recordToLog(content):
	logger = system.util.getLogger("PFCE-TW-Press")
	logger.info(content)
	

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

def diagnosticsChanged(tagPath, value):
	"""
	Function that will:
	From diagnostic tag (Flexovit Press) take the intiger value and tagPath
	then will calculate state code based on tagPath and set it to correct location
	
	Parameters
	----------
	tagPath : str
		path targeting diagnostic tag
		example: P51/signals/diagnostics/press/table2/st05_d02
	value: int
		value that tagPath has
	
	Returns
	------
	None
	
	"""
	
	#Most significant bit calculation
	def MSB(n):
		ndx = 0
		while ( 1 < n ):
			n = ( n >> 1 )
			ndx += 1
		return ndx

	#Exit immediatly if value is 0
	if value == 0:
		return False

	#Define all needed variables
	tableOffset, categoryOffset, addressOffset, stationOffset = 0, 0, 0, 0
	#Split tag path to get parts
	tagPathParts = tagPath.split("/")
	#Prepare igntionTagPath to target MES tag later
	ignitionTagPath = tagPathParts[0] + "/" + tagPathParts[1] + "/" + tagPathParts[2] + "/" + tagPathParts[3]
	#Define comparison values  from most significant tag path parts
	lastPart = tagPathParts[-1]
	tablePart = tagPathParts[-2]
	categoryPart = tagPathParts[-3]
	
	#If third character from last is d or w or b than tagName is correct and we can take last two characters and turn them in INT and define as addressOffset value
	if lastPart[-3] == 'd' or lastPart[-3] == 'w' or lastPart[-3] == 'b':
		addressOffset = int(lastPart[-2:])
	#If we detect a station identifier in lastPart we define stationOffset value
		if len(lastPart) == 8 and lastPart[-4] == '_' and lastPart[:2] == 'st':
			stationId = lastPart[2:4]
			stationOffset =  int(stationId)*1000
	#Exit if tagName is not correct
	else:
		return False
	
	#If tablePart is table1 check what category is in categoryPart.
	#If it is not 'press' in can only be 'general'. Define categoryOffset and tableOffset accordingly
	if tablePart == 'table1':
		if categoryPart == 'press':
			categoryOffset = 200000
			tableOffset = 10000
		else:
			categoryOffset = 100000
			tableOffset = 500
	
	#Same check fot table2
	elif tablePart == 'table2':
		if categoryPart == 'press':
			categoryOffset = 200000
			tableOffset = 50000
		else:
			categoryOffset = 100000
			tableOffset = 750
	
	#If table part is 'general' then table is not specified. Only define categoryOffset
	elif tablePart == 'general':
		categoryOffset = 100000
	
	#If table part is 'stacking' then there can be no table only categoryOffset
	elif tablePart == 'stacking':
		categoryOffset = 300000
	
	else:
		return False
	
	#Sum up all offsets to get state starting bit
	state = categoryOffset + tableOffset + stationOffset + (addressOffset*8)
	
	#Add a bit value to state root 
	state = state + int(MSB(value))
	
	# if state is found in definitions
	if shared.mes.equipment.getEquipmentStateOptions(ignitionTagPath, state):
		#Write state to /press/cycles/current/prod_tempStateCode press UDT
		system.tag.write(ignitionTagPath + "/press/cycles/current/prod_tempStateCode", state)


def updateWeightByMouldId(pressName, table1MoldId, weights, weightAverage, thickness, thicknessAverage, updateNbRecordBack = 2):
	"""
	Initial version (prototype) - Has lines for testing 
	Function used for update of weight parameter in cycles database entry based on table and mould id.
	Intended exclusevly for Flexovit. Since this press only has one weight station we dont need to calculate mean.
	
	Parameters
	----------
	pressId: string
		Example: 'P51'
	tableId: int
		Can be only 1 or 2 (may not be even necessary once it is clear)
	mouldId: int
		This is the id of mould. Will come from PLC !!
	weight: float
		This will be currentValue.value from weighting station OPC tag
	weightDict=None
		This is here just in case it should be scalable for any future presses.
		Primarly unused
		
	Returns
	-------
		None
			Updates cycles table in sga_twpress db
	"""	
	# Queries for select (testing)
	# Queries for update - Where clause targets latest record with specified table and mould id
	
	# DW:Replaced old one sqlQuery = """SELECT id FROM cycles WHERE press = ? and table1Mold = ? ORDER by id DESC LIMIT """ + str(updateNbRecordBack)
	# From 5000ms in Eibergen to 20ms
	
	#IF: REplaced again with old one due to errors
	#sqlQuery = """SELECT id FROM cycles WHERE cycleEndDate>=(NOW()-INTERVAL 5 MINUTE) and press = ? and table1Mold = ? ORDER by id DESC LIMIT """ + str(updateNbRecordBack)
	sqlQuery = """SELECT id FROM cycles WHERE cycleEndDate >= (NOW()-INTERVAL 5 MINUTE) and press = ? and table1Mold = ? ORDER by id DESC LIMIT """ + str(updateNbRecordBack)
	sqlResult = system.db.runPrepQuery(sqlQuery, [pressName, table1MoldId], "sga_twpress")
	
	rowCount = sqlResult.getRowCount()
	
	if rowCount > 0:
		if rowCount >= updateNbRecordBack - 1:
			idToUpdate = sqlResult.getValueAt(updateNbRecordBack - 1, 0)
		else:
			idToUpdate = sqlResult.getValueAt(0, 0)
	
		sqlQuery= """
			UPDATE 
				cycles 
			SET 
				weights = ?, 
				weightAverage = ?,
				thickness = ?,
				thicknessAverage = ?
			WHERE 
				id = ?
		"""
	
		args = [weights, weightAverage, thickness, thicknessAverage, idToUpdate]
		sqlResult = system.db.runPrepUpdate(sqlQuery, args, database='sga_twpress')
		
