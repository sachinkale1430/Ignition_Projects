def createBigDataJson(tagPaths,startDate,endDate, lastValue=True):
	from uuid import uuid4
		
	#logger = system.util.getLogger("patrikTest")	
	
	#logger.info(str(startDate)+" "+str(endDate)+" "+str(tagPaths))
	
	tagValues = system.tag.queryTagHistory(tagPaths,validateSCExec=False,startDate=startDate,endDate=endDate,returnSize=0)
	pyDs = system.dataset.toPyDataSet(tagValues)
	headers  =  system.dataset.getColumnHeaders(tagValues)
	
	uuid = str(uuid4())
	
	json = {"uuid":uuid}
	#print pyDs
	# Current logic of UDT (Current values)
	if lastValue:
		lastRow = pyDs[len(pyDs)-1]
		for header in headers:
			tagName =  header.split("/")[-1]
			value = lastRow[header]
			
			json[tagName] = value
	else:
		# Get everything that happened this cycle
		json["data"] = []
		for row in pyDs:
			internalJson = {}
			for header in headers:
				tagName =  header.split("/")[-1]
				value = row[header]
				
				internalJson[tagName] = value
			json["data"].append(internalJson)
	return json
			
		
def addDataToSpc(tagPaths, location, sampleDef ):
	
	tagPathString = [tag.fullPath for tag in tagPaths]
	
	tagValues = system.tag.readAll(tagPathString)
	
	sample = system.quality.sample.data.getCreateSampleByName("",sampleDef, location)
	
	for value,tagPath in zip(tagValues,tagPathString):
		tagName = tagPath.split("/")[-1]
		sample.setSampleData(1, tagName, str(value.value))
	
	sample.setApproved(1)
	system.quality.sample.data.updateSample(location,sample,1)