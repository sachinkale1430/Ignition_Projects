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
	


def storeCycleData(data):
	"""
	Function that stores cycle data to database
	
	Parameters
	----------
	data: dict
		dict structure of data needed to be stored in cycles table
	
	Returns
	-------
	None
	"""	

	args = [
		data["press"],
		data["cycleEndDate"],
		data["table1Mold"],
		data["table2Mold"],
		data["cycleDuration"],
		data["slowestStationName"],
		data["thicknessAverage"],
		data["thickness"],
		data["weightAverage"],
		data["weight"],
		data["rejects"],
		data["outfeed"]
	]
	
	sqlQuery = """
		INSERT INTO cycles
			(
				press, 
				cycleEndDate, 
				table1Mold, 
				table2Mold, 
				cycleDuration, 
				slowestStationName, 
				thicknessAverage, 
				thickness, 
				weightAverage, 
				weights,
				rejects,
				outfeed
			)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?) 
	"""
	
	sqlResult = system.db.runPrepUpdate(sqlQuery, args = args, database = "sga_twpress")
	

def manageCycle(ignitionTagPath, lastCycleDuration, cycleEndDate, wheelsRejected = False):
	"""
	function that prepares all the data needed to be stored in database
	
	Parameters
	----------
	ignitionTagPath: str
		tag path pointer to machine T_MES_Link
	lastCycleDuration: float
		last cycle duration (coming from tag)
	cycleEndDate: date
		date presenting last cycle update
	
	Returns
	-------
	None
	"""	
	
	tagsToRead = [
		ignitionTagPath + "/mes/param_stationName",
		ignitionTagPath + "/press/prod_nbCavities"
	]
	
	tagValues = system.tag.readAll(tagsToRead)
	
	press = tagValues[0].value
	prodNbCavities = tagValues[1].value
	
	# moulds identification
	parentPath = ignitionTagPath + "/signals/molds/memorized"
	mouldsData = getTagValues(parentPath = parentPath)
	table1Mould, table2Mould = mouldsData['tagValues'][0], mouldsData['tagValues'][1]

	# thickness
	parentPath = ignitionTagPath + "/signals/thickness/"
	thicknessData = getTagValues(parentPath = parentPath)

	thicknessMean = thicknessData["meanValue"]
	thicknessList = {}
	thicknessList["t"] = thicknessData["tagValues"]

	# weight
	parentPath = ignitionTagPath + "/signals/weights/"
	weightData = getTagValues(parentPath = parentPath)

	weightMean = weightData["meanValue"]
	weightList = {}
	weightList["w"] = weightData["tagValues"]

	# slowest station
	slowestStationName, slowestStationCycleTime = getSlowestStationName(ignitionTagPath)
	
	# maange outfeed and rejects values
	rejects = outfeed = 0
	
	if wheelsRejected:
		rejects = prodNbCavities
	else:
		outfeed = prodNbCavities		
		
	
	# build final data package
	data = {}
	data["press"] = 				press
	data["cycleEndDate"] = 			cycleEndDate
	data["table1Mold"] = 			table1Mould
	data["table2Mold"] = 			table2Mould
	data["cycleDuration"] = 		lastCycleDuration
	data["slowestStationName"] = 	slowestStationName
	data["thicknessAverage"] = 		thicknessMean
	data["thickness"] = 			system.util.jsonEncode(thicknessList)
	data["weightAverage"] = 		weightMean
	data["weight"] = 				system.util.jsonEncode(weightList)
	data["rejects"] = 				rejects
	data["outfeed"] = 				outfeed
	
	# store to database
	storeCycleData(data)
	


def reset(ignitionTagPath, targetCycle = "current"):
	"""
	function that clears T_TW_Press_Cycle UDT instance
	
	Parameters
	----------
	ignitionTagPath: str
		tag path pointer to machine T_MES_Link
	targetCycle : str
		pointer to "current" or "previous" cycle UDT instance
	
	Returns
	-------
	None
	"""	
	
	# build full path to current T_TW_Press_Cycles udt instance
	prefix = ignitionTagPath + "/press/cycles/" + targetCycle + "/"
	# get press name from ignition tag path
	press = ignitionTagPath.rsplit("/")[-1]

	# building tag paths list to initialize
	def build(path):
		return prefix + path
	
	tags = []
	tags.append(build("prod_tempStateCode"))
	tags.append(build("prod_tempStateCodeSource"))
	tags.append(build("prod_startTS"))
	tags.append(build("prod_maxDuration"))
	tags.append(build("prod_targetDuration"))
	tags.append(build("prod_maxTS"))
	tags.append(build("prod_targetTS"))
	tags.append(build("prod_type"))
	tags.append(build("db/speedLossStateCode"))
	tags.append(build("db/cycleEndDate"))
	tags.append(build("db/outfeed"))
	tags.append(build("db/slowestStationName"))
	tags.append(build("db/thickness"))
	tags.append(build("db/speedLossDuration"))
	tags.append(build("db/table2Mold"))
	tags.append(build("db/cycleDuration"))
	tags.append(build("db/weights"))
	tags.append(build("db/weightAverage"))
	tags.append(build("db/rejects"))
	tags.append(build("db/table1Mold"))
	tags.append(build("db/thicknessAverage"))
	tags.append(build("db/id"))
	tags.append(build("db/press"))

	
	# build list of values, same size as tags and set them to None
	values = [None] * len(tags)
	
	# overwrite last value with press name
	values[-1] = press
	#values[-1] = "P99"
	
	# added to force production
	#values[0] = 1
	
	system.tag.writeAllSynchronous(tags, values)



def copyUDTContent(ignitionTagPath):
	"""
	Copy full structure of current T_TW_Press_Cycle UDT instance to previous
	
	Parameters
	----------
	ignitionTagPath: str
		path to folder of the press
	
	Returns
	-------
	None
	"""	

	# browse tags in currect T_TW_Press_Cylces UDT instance
	udtTagsList = system.tag.browseTags(parentPath = ignitionTagPath, recursive = True)
	
	tagsToRead = []
	
	# loop and collect all paths
	for tag in udtTagsList:
		if not tag.isFolder():
			tagsToRead.append(tag.path)
		
	# read all values in 1 shot
	tagValues = system.tag.readAll(tagsToRead)
	
	# prepare tag destination to write to
	tagsToWrite = []
	
	for tag in tagsToRead:
		destinationPath = tag.replace("current", "previous")
		tagsToWrite.append(destinationPath)
	
	# write all values in 1 shot from current to previous
	system.tag.writeAllSynchronous(tagsToWrite, tagValues)
	
	
	
def saveCycle(ignitionTagPath):
	"""
	Closing current cycle in T_TW_Press_cycles udt instance
	Collecting all data in current T_TW_Press_cycle udt instance
	Storing to database
	
	Parameters
	----------
	ignitionTagPath: str
		path to folder of the press
	
	Returns
	-------
	recordId: int
		Returned database id after insertation
	"""
	
	# build full path to current T_TW_Press_Cycles udt instance
	prefix = ignitionTagPath + "/press/cycles/current/"

	# building tag paths list to initialize
	def build(path):
		return prefix + path

	# generate list of tag paths	
	tags = []
	tags.append(build("prod_tempStateCode"))
	tags.append(build("prod_startTS"))
	tags.append(build("prod_maxDuration"))
	tags.append(build("prod_targetDuration"))
	tags.append(build("prod_maxTS"))
	tags.append(build("prod_targetTS"))	
	tags.append(build("db/speedLossStateCode"))
	tags.append(build("db/cycleEndDate"))
	tags.append(build("db/outfeed"))
	tags.append(build("db/slowestStationName"))
	tags.append(build("db/thickness"))
	tags.append(build("db/speedLossDuration"))
	tags.append(build("db/table2Mold"))
	tags.append(build("db/cycleDuration"))
	tags.append(build("db/weights"))
	tags.append(build("db/weightAverage"))
	tags.append(build("db/rejects"))
	tags.append(build("db/table1Mold"))
	tags.append(build("db/thicknessAverage"))
	tags.append(build("db/id"))
	tags.append(build("db/press"))	
	
	values = system.tag.readAll(tags)
	
	# store all results into json
	cycle = {}
	for i in range(0, len(tags)):
		cycle[tags[i].rsplit("/")[-1]] = values[i].value

	# ***************** STORE TO DB ***************************
	# get data from cycle json
	args = [
		cycle["press"],
		cycle["cycleEndDate"],
		cycle["table1Mold"],
		cycle["table2Mold"],
		cycle["cycleDuration"],
		cycle["speedLossDuration"],
		cycle["speedLossStateCode"],
		cycle["slowestStationName"],
		system.util.jsonEncode(cycle["thickness"]),
		cycle["thicknessAverage"],
		system.util.jsonEncode(cycle["weights"]),
		cycle["weightAverage"],
		cycle["rejects"],
		cycle["outfeed"]
	]
	
	sqlQuery = """
		INSERT INTO cycles
			(
				press, 
				cycleEndDate, 
				table1Mold, 
				table2Mold, 
				cycleDuration, 
				speedLossDuration,
				speedLossStateCode,
				slowestStationName, 
				thickness, 
				thicknessAverage, 
				weights,
				weightAverage, 
				rejects,
				outfeed
			)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
	"""
	
	recordId = system.db.runPrepUpdate(sqlQuery, args = args, database = "sga_twpress", getKey=1)

	return recordId	
	
	
	
def getTagValuesData(parentPath = "", tagPath = "*", scope = "w"):
	"""
	Function that scans tag folder path defined and provides array of values list with mean calculation
	
	Parameters
	----------
	parentPath: string
		path to tag folder
	tagPath: string
		additional filttering within folder (accepts wildcard *)
	scope: string
		w - getting weigths
		t - getting thickness
		m - getting molds data
		c - getting station cycles data
	
	Returns
	-------
	data: dict
	mean: float
		two results are returned
	"""	

	tags = system.tag.browseTags(parentPath = parentPath, tagPath = tagPath, sort = "ASC")
			
	tagsToRead = []
	tagValuesData = []
	
	# loop through tags	
	for tag in tags:
		tagsToRead.append(tag.fullPath)
		
	tagValues = system.tag.readAll(tagsToRead)

	# get values and build list
	for tagValue in tagValues:
		tagValuesData.append(tagValue.value)	
		
	# if values are available, return content, otherwise None
	if len(tagValuesData) > 0:
		data = {}
		data[scope] = tagValuesData	
		mean = system.math.mean(tagValuesData)
		
	else:
		return None
	
	return data, mean
	
	
def removeStates(equipmentPath, startDate, endDate):
	collectorType = 'Equipment State'
	key = ''
	 
	data = system.mes.getTagCollectorValues(equipmentPath, collectorType, key, startDate, endDate)
	
	statesToRemove = []
	
	for row in system.dataset.toPyDataSet(data):
		timestamp = row["TimeStamp"]
		stateCode = row["State"]
		originalStateCode = row["OriginalState"]
		
		if stateCode <> 1:
			statesToRemove.append(timestamp)		
	
	
	system.mes.removeTagCollectorValues(equipmentPath, collectorType, key, statesToRemove)


def getMeasurementsAndStore(weightPaths, thicknessPaths):
	
	tagObj = system.tag.readAll(tagsToRead)
	tagValues = []
	weightsJson = {}
	
	for tag in tagObj:
		if str(tag.quality) == "Good":
			tagValues.append(tag.value)
	
	weightsJson["w"] = tagValues
	weightAverage = system.math.mean(tagValues)
	
	tagsToWrite = [
		"[.]weights",
		"[.]weightAverage",
		"[.]../../press/cycles/current/db/weights",
		"[.]../../press/cycles/current/db/weightAverage"
	]
	
	tagValues = [
		weightsJson,
		weightAverage,
		weightsJson,
		weightAverage
	]
	
	system.tag.writeAllSynchronous(tagsToWrite, tagValues)
	
	
	
def getRejectsInPeriod(eqPath, startDate, endDate):
	"""
	Function using MES tag collector to find out if there was reject
	
	Parameters
	----------
	eqPath: str
		path to press in production model
	startDate: date
		begining of check
	endDate: date
		end date of check
	
	Returns
	-------
	rejects: int
		number of rejects for given period
	"""

	collectorType = "Equipment Count"
	key = "prod_rejects"
	
	previousValue = system.mes.getTagCollectorPreviousValue(eqPath, collectorType, key, startDate)
	lastValue = system.mes.getTagCollectorLastValue(eqPath, collectorType, key)

	rejects = lastValue - previousValue
	
	return int(rejects)
	
	