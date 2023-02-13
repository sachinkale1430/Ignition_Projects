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
def getTagsAsJson(tags, attr={}):
	# Recursive function to find tags in sub-folders
	def getTagsRecursive(folder, attr):
		# Remove the tag provider
		tmp = folder.split("]")[1]
		# DO not process sensitive folders
		if tmp in ["factory", "interfaces"]:
			return "Internal folder not exported"
		else:
			search = system.tag.browseTags(folder)
			tagList = [tag.fullPath for tag in search if not tag.isFolder()]
			jsonList = tagListToJson(tagList, attr)
			folderList = dict((tag.name,getTagsRecursive(tag.fullPath, attr)) for tag in search if tag.isFolder() or tag.isUDT())
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
		search = system.tag.browseTags(tags)
		tagList = [tag.fullPath for tag in search if not tag.isFolder()]
		jsonList = tagListToJson(tagList, attr)
		# Dict comprehension in 2.5 python. Recursive call.
		folderList = dict( (tag.name,getTagsRecursive(tag.fullPath, attr)) for tag in search if tag.isFolder() or tag.isUDT())    # RECURSIVE CALL
		jsonList.update(folderList)
		return jsonList
	
	# Processing tagList	
	if parameterType == "tagList":
	 	return tagListToJson(tags, attr)

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

#==============================================================================================
# tagListToJson( tags ) 
# 
# In : list of tags
# Out : json
#==============================================================================================
def tagListToJson(tags, attr):
	from com.inductiveautomation.ignition.common import BasicDataset
	import ast
	tmp = []
	list_attr = attr.items()
	for tag in tags:
		tmp.append(tag)
		for item in list_attr:
			tmp.append(tag +"."+ item[1])
	tags = tmp
	readTags = system.tag.readAll(tmp)
	json = {}
	
	idx = 0
	# Dictonary with tag attributes. Key is what is going to be in json, value is ignition attribute
	# Browse the list of tags.
	while idx < len(tags):
		a_inx = idx % (len(attr) + 1)
		if a_inx == 0:		
			# Take the tag name from path
			tagName = tags[idx].split("/")[-1]
			if readTags[idx].quality.name != "NOT_FOUND":
			
				# if value is dataset parse it.
				if isinstance(readTags[idx].value, BasicDataset):
					json[tagName] = datasetToJson(readTags[idx].value)
			
				# Otherwise if it is a string....
				elif isinstance(readTags[idx].value, basestring):
				
					try:
						# Try to see if String is valid JSON string
						jsonValue= system.util.jsonDecode(readTags[idx].value)
						json[tagName] = {"value": jsonValue, "quality": str(readTags[idx].quality)}
					except:
						# For everything else store value. 
						json[tagName] = {"value":readTags[idx].value, "quality": str(readTags[idx].quality)}
				
				else:
					# Store other primitive types. 
					json[tagName] = {"value":readTags[idx].value, "quality": str(readTags[idx].quality)}			
		else:
			tagName = tags[idx].split("/")[-1].split(".")[0]
			attribute = readTags[idx]
			if attribute.value and len(attribute.value) > 0:
				json[tagName].update({list_attr[a_inx-1][0]:attribute.value})
		idx += 1
	return json

#==============================================================================================
# datasetToJson( dataset ) 
# 
# In : a dataset
# Out : json
#==============================================================================================
def datasetToJson(dataset):
	from com.inductiveautomation.ignition.common import BasicDataset
	from com.inductiveautomation.ignition.common.script.builtin.DatasetUtilities import PyDataSet
	# Test if really a dataset
	if isinstance( dataset, BasicDataset) or (dataset, PyDataSet):
		# Browse rows
		jsonList = {"headers": list(dataset.getColumnNames()),   "values": [] }
		for rowIdx in range(dataset.rowCount):
			# Build lists of each line
			jsonList["values"].append( [dataset.getValueAt(rowIdx, colIdx) for colIdx in range(dataset.columnCount)])
		return jsonList
	else:
		return None

#==============================================================================================
# addWorkOrder 
#
# Adds a new workOrder in pfce table of workOrders
#
# Called once when a new work order has been created
# 
# In : factory (sap code), workOrder nb, material nb, 
# Out : json
# DEPRECATED NOT WORKING
#==============================================================================================
def old_addWorkOrder(factory, workOrder, material, json_erp_in):
	sql = "SELECT * FROM workorder WHERE workorder_nb = ?"
	# Test if workOrder exists already
	result = system.db.runPrepQuery(query=sql, args=[workOrder], database="factory_production")
	if result.rowCount == 0:
		sql = "INSERT INTO workorder (factory, json_erp_in, last_modification, material_nb, workorder_nb, status) VALUES (?, ?, ?, ?, ?, ?)"
		result = system.db.runPrepUpdate(query=sql, args=[factory, system.util.jsonEncode(json_erp_in), system.tag.read("[System]Gateway/CurrentDateTime").value, material, workOrder,201], database="factory_production")
		return True
	else:
		return False	

#==============================================================================================
# addProductionData 
# 
# Adds data of a piece for a specific step, workCenter, workOrder  in pfce table of workOrders
#
# Called frequently
# 
# In : factory (sap code), workOrder nb, material nb, producedLocation
# Out : json
# DEPRECATED NOT WORKING
#==============================================================================================
def old_addProductionData(workOrder, workCenter, productionStep, piece, producedLocation, jsonData):
	if not isinstance(piece, int):
		return "Please give integer number as number"
		
	if not workOrder:
		return "Please provide work order number."
	# Retrieve workOrder
	sql = "SELECT * FROM workorder WHERE workorder_nb = ?"
	result = system.db.runPrepQuery(query=sql, args=[workOrder], database="factory_production")
	
	# Return if wo does not exists
	if result.rowCount == 0:
		return False	

	# Prepare json storage with key representing workCenter/production step
	prodStepWorkCenter = productionStep + "_" + workCenter
	if isinstance(jsonData, list):
		prodStepDict = {"location": producedLocation, prodStepWorkCenter:jsonData}
	else:
		prodStepDict = {"location": producedLocation, prodStepWorkCenter:[jsonData]}
	
	# Prepare json storage for the piece 
	pieceDict = {str(piece):[prodStepDict]}
	
	prodStepJson = system.util.jsonEncode(prodStepDict)	
	pieceJson = system.util.jsonEncode(pieceDict)

	date = system.tag.read("[System]Gateway/CurrentDateTime").value
	#date = system.date.toMillis(date)
	# Update json content in dB	
	sql = "UPDATE workorder SET last_modification = ?, json_data = IF(JSON_TYPE(json_data) <=> 'ARRAY', json_data, JSON_ARRAY()), json_data = IF(JSON_LENGTH(json_data) > " + str(piece-1) + ", JSON_ARRAY_APPEND(json_data,'$["+ str(piece-1) +"].\""+ str(piece) +"\"', CAST('"+ prodStepJson +"' AS JSON) ), JSON_ARRAY_APPEND(json_data, '$', CAST('"+ pieceJson +"' AS JSON))) WHERE workorder_nb = ?"
	result = system.db.runPrepUpdate(query=sql, args=[date, workOrder], database="factory_production")
	return result>0
	