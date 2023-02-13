#===============================================
#===============================================
#
# json.export.* functions
#
# Functions that are used to prepare data for big-data export
# or other use
#===============================================
#===============================================



#==============================================================================================
# getTagsAsJson( tags ) 
# 
# In : list of tag paths, or a folder (attention recursive method)
# In : Dict of tag attributes check https://docs.inductiveautomation.com/display/DOC/Tag+Attributes
# Out : Json of last values known, attention recursive
#==============================================================================================
def getTagsAsJson(tags, attr={"value":"Value","quality":"Quality"}, flat=False):
	# Recursive function to find tags in sub-folders
	def getTagsRecursive(folder, attr, flat):
		# Remove the tag provider
		tmp = folder.split("]")[1]
		# DO not process sensitive folders
		if tmp in ["factory", "interfaces"]:
			return {folder:"Internal folder not exported"}
		else:
			if flat:
				search = system.tag.browseTags(folder)
				tagList = [tag.fullPath for tag in search if not (tag.isFolder() or tag.isUDT())]	
				jsonList = tagListToJson(tagList, attr, flat)
				folderList = [getTagsRecursive(tag.fullPath, attr, flat) for tag in search if tag.isFolder() or tag.isUDT()]
				for x in folderList:
					jsonList.update(x)
				return jsonList
			else:	
				search = system.tag.browseTags(folder)
				tagList = [tag.fullPath for tag in search if not (tag.isFolder() or tag.isUDT())]
				jsonList = tagListToJson(tagList, attr, flat)
				folderList = dict((tag.name,getTagsRecursive(tag.fullPath, attr, flat)) for tag in search if tag.isFolder() or tag.isUDT())
				jsonList.update(folderList)
				return jsonList
			
			
	parameterType = None

	# Treatment in two passes : 1st find which is type of "tags", 2nd the processing itself
	
	# If the parameter is string and atleast 1 tag its a folder
	if isinstance(tags, basestring) and len(system.tag.browseTags(tags)) > 0:
		parameterType = "folder"

	# if the parameter is list, its a list of tags
	elif isinstance(tags, list):
		parameterType = "tagList" 

	# Otherwise
	else:
		raise TypeError("Type error in parameter. Please give List of tag paths eg. tags=['[TagProvider]tag/path/1', '[TagProvider]tag/path/2'] or just folder to tags eg. tags='[TagProvider]tag/path/folder'")
	
	# Processing folders
	if parameterType == "folder":
		# Retrieve tags
		if flat:
			search = system.tag.browseTags(tags)
			tagList = [tag.fullPath for tag in search if not (tag.isFolder() or tag.isUDT())]
			jsonList = tagListToJson(tagList, attr, flat)
			folderList = [getTagsRecursive(tag.fullPath, attr, flat) for tag in search if tag.isFolder() or tag.isUDT()]# RECURSIVE CALL		
			for x in folderList:
				jsonList.update(x)
		else:
			search = system.tag.browseTags(tags)
			tagList = [tag.fullPath for tag in search if not (tag.isFolder() or tag.isUDT())]
			jsonList = tagListToJson(tagList, attr, flat)
			# Dict comprehension in 2.5 python. Recursive call.
			folderList = dict( (tag.name,getTagsRecursive(tag.fullPath, attr, flat)) for tag in search if tag.isFolder() or tag.isUDT())    # RECURSIVE CALL
			jsonList.update(folderList)
		return jsonList
	
	# Processing tagList	
	if parameterType == "tagList":
	 	return tagListToJson(tags)

#==============================================================================================
# getTagsHistoryAsJson( tags ) 
# 
# In : list of tag paths, or a folder (attention recursive method)
# Out : history in json, itself array for each tag, flat with arrays of historical values (value, quality, timestemp)
#==============================================================================================
def getTagsHistoryAsJson(tags,startDate, endDate, **kwargs):
	
	# Recursive function to find historical tags in sub-folders
	def browseHistoricalTags(path=""):
		array = []
		for result in system.tag.browseHistoricalTags(path).getResults():
			if result.hasChildren():
				array = array + browseHistoricalTags(result.getPath())
			else:
				array.append(str(result.getPath()))
		return array	
	
	# Verify that the format of date is the proper one
	from java.util import Date
	if not isinstance(startDate, Date) or not isinstance(endDate, Date):
		return "Please give dates in ignition format use system.date functions"
	
	# Verify that end if after start
	if system.date.isBefore(endDate, startDate):
		return "End date needs to be after start date."
		
	parameterType = None
	
	# If the parameter is string and at least 1 tag its a folder
	if isinstance(tags, str):
		gatewayName = system.tag.read("[System]Gateway/SystemName").value.lower()
		database = "factory_history"
		realtimeTagProvider = "default"
		# If we ask for root of tag provider
		if tags == "/":
			historicalPath =  ""
		else:
			historicalPath = "histprov:" + database + ":/drv:" + gatewayName + ":" + realtimeTagProvider + ":/tag:" + tags
		if len(system.tag.browseHistoricalTags(historicalPath).getResults()) > 0:
			parameterType = "folder"
	
	# if the parameter is list, its a list of tags
	elif isinstance(tags, list):
		parameterType = "tagList" 
	
	# Otherwise
	else:
		raise TypeError("Type error in parameter. Please give List of tag paths eg. tags=['[TagProvider]tag/path/1', '[TagProvider]tag/path/2'] or just folder to tags eg. tags='[TagProvider]tag/path/folder'")
	logger = system.util.getLogger("export_logger")
	
	# If we have to manage a folder
	if parameterType == "folder":
	
		json = {}
		# Recursive browsing of tags to include (=with history)
		tagPaths = browseHistoricalTags(historicalPath)  
		start_time = system.date.toMillis(system.date.now())
		# Get historical data. Attention in column format otherwise it would generate plenty of copies or holes for nothing
		data = system.tag.queryTagHistory(paths=tagPaths, returnFormat="Tall", ignoreBadQuality=True, startDate=startDate, endDate=endDate)
		#pyData = system.dataset.toPyDataSet(data)
		total = system.date.toMillis(system.date.now()) - start_time
		logger.info("Function took " + str(total / 1000)+ " sec")
		# Now we will transform data into JSON, in an array for each tag
		for row in range(data.rowCount):
			path = data.getValueAt(row, "path")
			# If tag is not yet in JSON
			if path not in json:
				# Add a key with tag name
				json[path] = [[data.getValueAt(row, "value"), data.getValueAt(row, "quality"), system.date.toMillis(data.getValueAt(row, "timestamp"))]]
			else:
				# Otherwise just append new values
				json[path].append([data.getValueAt(row, "value"), data.getValueAt(row, "quality"), system.date.toMillis(data.getValueAt(row, "timestamp"))])
		return json

	if parameterType == "tagList":
		# TODO TODO TODO
		# TODO TODO TODO
		pass


def tagListToJson(tags, attr, flat=False):
	from com.inductiveautomation.ignition.common import BasicDataset
	tmp = []
	list_attr = attr.items()
	for tag in tags:
		for item in list_attr:
			tmp.append(tag +"."+ item[1])
	tags = tmp
	readTags = system.tag.readAll(tmp)
	json = {}
	
	idx = 0
	# Dictonary with tag attributes. Key is what is going to be in json, value is ignition attribute
	# Browse the list of tags.
	while idx < len(tags):
		a_inx = idx % (len(attr) )
		if flat:
			tagName = tags[idx].split(".")[0]
		else:
			tagName = tags[idx].split(".")[0].split("/")[-1]
			
		attribute = readTags[idx]
		tmpValue = None
		
		if attribute.value or attribute.value == 0.0:
			if isinstance(attribute.value, BasicDataset):
				tmpValue = datasetToJson(attribute.value)
			else:
				tmpValue = unicode(attribute.value)
		
		if tagName in json:
			json[tagName].update({list_attr[a_inx][0]:tmpValue})
		else:
			json[tagName] = {list_attr[a_inx][0]:tmpValue}

		idx += 1
	return json

#==============================================================================================
# datasetToJson( dataset ) 
# 
# In : a dataset
# Out : json
#==============================================================================================
# MODIFICATION BY ROK: 22.1.2021
# NEW FUNCTION IN PRODUCTION BELLOW!!!
#def datasetToJson(dataset):
#	from com.inductiveautomation.ignition.common import BasicDataset
#	from com.inductiveautomation.ignition.common.script.builtin.DatasetUtilities import PyDataSet
#	# Test if really a dataset
#	if isinstance( dataset, BasicDataset) or isinstance(dataset, PyDataSet):
#		# Browse rows
#		# THIS COMMNET WILL BE REMOVED
#		jsonList = {"headers": dataset.getColumnNames(),   "values": [] }
#		for rowIdx in range(dataset.rowCount):
#			# Build lists of each line
#			jsonList["values"].append( [dataset.getValueAt(rowIdx, colIdx) for colIdx in range(dataset.columnCount)])
#		return jsonList
#	else:
#		return None
		
def datasetToJson(dataset):
	#from com.inductiveautomation.ignition.common import BasicDataset
	#from com.inductiveautomation.ignition.common.script.builtin.DatasetUtilities import PyDataSet
	from com.inductiveautomation.ignition.common import Dataset
	
	# Test if really a dataset
	#logger.info("Type of input parameter " + str(type(dataset)))
	#if isinstance( dataset, BasicDataset) or isinstance(dataset, PyDataSet):
	if issubclass(type(dataset), Dataset):
		# Browse rows
		jsonList = {"headers": dataset.getColumnNames(),   "values": [] }
		for rowIdx in range(dataset.rowCount):
			# Build lists of each line
			jsonList["values"].append( [dataset.getValueAt(rowIdx, colIdx) for colIdx in range(dataset.columnCount)])
		return jsonList
	else:
		return None	

def getTagsAsXls(path, attr={"value":"Value","quality":"Quality"}):
	
	json_list = getTagsAsJson(path, attr=attr, flat=True)
	
	#return json_list
	
	headers = ["TagPath"]
	for at in attr:
		headers.append(at)
	
	data = []
	
	for tag in json_list:
		row = []
		row.append(tag)
		for header in range(1, len(headers)):
			row.append(unicode(json_list[tag][headers[header]]))
		data.append(row)
	
	ds = system.dataset.toDataSet(headers, data)
	
	xls = system.dataset.dataSetToExcel(True, [ds])
	return xls
	
	
# faster way of preparing json history with query
# parameters
# - startMillis	- start of the period
# - endMillis - end of the period
def getTagsHistoryAsJsonBeta(startMillis, endMillis):
	
	dataSource = "factory_history"
	tagProvider = "default"
	
	startDate = system.date.fromMillis(long(startMillis))
	endDate = system.date.fromMillis(long(endMillis))
	
	year = system.date.getYear(startDate)
	month = system.date.getMonth(startDate) + 1
	
	tableName = "sqlt_data_1_"+str(year)+"_"+str(month).zfill(2)
	
	query = """
				SELECT t.id, t.tagpath, d.dataintegrity, d.t_stamp, 
				IF (t.datatype=0,d.intvalue,IF(t.datatype=1,d.floatvalue,IF(t.datatype=2,d.stringvalue,''))) value 
				FROM 
					( 
					SELECT 
						t.id,  
						t.tagpath, 
						t.datatype
					FROM  
						sqlth_te t  
					JOIN sqlth_scinfo s ON s.id = t.scid 
					JOIN sqlth_drv d ON d.ID=s.drvid AND d.provider=? 
					WHERE  
						t.retired IS NULL 
					GROUP BY 
						t.id, 
						t.tagpath, 
						t.datatype
					ORDER BY 
						t.tagpath 
					) t 
				LEFT JOIN """ +tableName+ """ d on (d.tagid = t.id) 
				WHERE 
				d.t_stamp > ?  AND d.t_stamp < ? 
				order by d.t_stamp 
		"""
	tagData = system.db.runPrepQuery(query, [tagProvider, long(startMillis), long(endMillis)] , dataSource)
	
	json = {}
		
	for idx in range(tagData.getRowCount()):
		path = tagData.getValueAt(idx, 1)
		value = tagData.getValueAt(idx, 4)
		quality = tagData.getValueAt(idx, 2)
		timestamp = tagData.getValueAt(idx, 3)
		
		if path not in json:
			json[path] = [[value, quality, timestamp]]
		else:
			json[path].append([value, quality, timestamp])
			
	return json