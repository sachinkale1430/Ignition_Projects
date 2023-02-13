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



def getSlowestStationName(ignitionTagPath, fromTimeStamp = None):
	"""
	Function that gets slowest station name and duration 
	
	Parameters
	----------
	ignitionTagPath : tagPath
		path to equipment in tag path structure
	fromTimeStamp: date
		date used to filter stations that were updated after fromTimeStamp
		None is used when all stations are included in calculation
	
	Returns
	-------
	list
		slowestStationName: str
			name of the slowest station from press station definitions
		maxCycleTime: float
			slowest cycle time duration
	"""	
	
	parentPath = ignitionTagPath + "/signals/cycles/stations"
	stationCycleTimes = readStationCycleValues(ignitionTagPath)
	
	# collect press definition from press UDT
	stationDefinitions = system.tag.read(ignitionTagPath + "/press/param_stationDefinitions").value
	
	count = 0
	maxCycleTime = 0
	slowestStationName = ""

	for station in stationCycleTimes:
		# NOTE: name standard needs to be respected!!!
		# Like: prod_t01st05
		# getting table id, station id and actual cycle duration per station
		stationPartsData = station[0].split("_")[1]
		tableId = int(stationPartsData[2:3])
		stationId = int(stationPartsData[-2:])
		stationCycleTime = station[1]
		stationTimeStamp = station[2]
		
		# loop through station definitions
		for station in system.dataset.toPyDataSet(stationDefinitions):
			if tableId == station["table"] and stationId == station["station"] and station["includeInCalculations"]:
				stationName = station["stationName"]
							
				# check if cycle time is higher than highest cycleTime
				if stationCycleTime > maxCycleTime:
					if fromTimeStamp is None:
						maxCycleTime = stationCycleTime
						slowestStationName = stationName
					else:
						if system.date.isAfter(stationTimeStamp, fromTimeStamp):
							maxCycleTime = stationCycleTime
							slowestStationName = stationName
						
						
			count += 1	

	return slowestStationName, maxCycleTime
	
	
def readStationCycleValues(ignitionTagPath):
	"""
	Function that gets slowest station name and duration 
	
	Parameters
	----------
	ignitionTagPath : tagPath
		path to equipment in tag path structure
	
	Returns
	-------
	tagValuesData : array
		list of stations (tagname, cycle and timestamp of each station)
	"""	
	
	# get cycle times of all station specified in station tag folder
	stationCyclesPath = ignitionTagPath + "/signals/cycles/stations"
	tags = system.tag.browseTags(parentPath = stationCyclesPath)
	
	tagsToRead = []
	tagValuesData = []
	
	for tag in tags:
		tagsToRead.append(tag.fullPath)
		
	tagValues = system.tag.readAll(tagsToRead)
	
	# get values and build list
	count = 0
	for tagValue in tagValues:
		tagPath = tagsToRead[count]
		tagName = tagPath.split("/")[-1]
		
		tagValuesData.append([
			tagName,
			tagValue.value,
			tagValue.timestamp
		])
		
		count += 1
		
	return tagValuesData
	