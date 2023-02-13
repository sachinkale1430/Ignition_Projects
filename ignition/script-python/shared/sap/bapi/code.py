#===============================================================================

def prepareTransaction(testEnvironment=True):

	"""
	Function to define a new BAPI transaction. This is the start of every SAP communication.
	
	Parameters
	----------
	testEnvironment : boolean 
		   default true
		flag to check if the BAPI transaction goes to T16 environment
	
	Returns
	-------
	str
		uuid that identifies this transaction.
	"""
	
	import uuid

	# Select target system
	if testEnvironment:
		tableName = "transactions_test"
	else:
		tableName = "transactions_prod"

	# Get globals
	factoryGateway = system.tag.read("[System]Gateway/SystemName").value	
	timestamp = system.date.now()
	
	# Generate unique uudi	
	generatedUuid = str(uuid.uuid4())
		
	# Add line to database
	sql = "INSERT INTO " + tableName + " (uuid, request_received_timestamp, factory_gateway, request) VALUES (?,?,?,?)"
	rows = system.db.runPrepUpdate(sql, args=[generatedUuid, timestamp, factoryGateway, system.util.jsonEncode([])], database="sga_sap_bapi")
	
	return generatedUuid

#===============================================================================

def prepareOperation(uuid, sapOperation):

	if sapOperation in shared.sap.templates.__dict__:
		template = shared.sap.templates.__dict__[sapOperation]
	else:
		return "Bapi not found"
	# Old code
	"""
	if sapOperation == "BAPI_PRODORDCONF_CREATE_TT" :
		template = shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT

	elif sapOperation == "BAPI_GOODSMVT_CREATE":
		template = shared.sap.templates.BAPI_GOODSMVT_CREATE

	elif sapOperation == "BAPI_TRANSACTION_COMMIT":
		template = shared.sap.templates.BAPI_TRANSACTION_COMMIT

	elif sapOperation == "BAPI_TRANSACTION_ROLLBACK":
		template = shared.sap.templates.BAPI_TRANSACTION_ROLLBACK

	elif sapOperation == "BAPI_PRODORDCONF_GETDETAIL": 
		template = shared.sap.templates.BAPI_PRODORDCONF_GETDETAIL
		
	elif sapOperation == "BAPI_PRODORDCONF_GETLIST":
		template = shared.sap.templates.BAPI_PRODORDCONF_GETLIST
		
	elif sapOperation == "BAPI_PRODORD_GET_DETAIL":
		template = shared.sap.templates.BAPI_PRODORD_GET_DETAIL
	
	elif sapOperation == "BAPI_PRODORDCONF_CANCEL":
		template = shared.sap.templates.BAPI_PRODORDCONF_CANCEL

	elif sapOperation == "BAPI_PRODORDCONF_CREATE_HDR":
		template = shared.sap.templates.BAPI_PRODORDCONF_CREATE_HDR
	
	elif sapOperation == "BAPI_PRODORDCONF_CREATE_TE":
		template = shared.sap.templates.BAPI_PRODORDCONF_CREATE_TE
	else:
		return "Bapi not found"
	"""	
	#template["parameterValues"]["sessionUUID"] = uuid		
	return template
	
#===============================================================================	
	
def addOperation(uuid, jsonParameters, testEnvironment=True):
	"""
	Function to add a SAP operation to transaction. Depending on the operation the jsonParameters will change. 
	
	Parameters
	----------
	uuid: str
		unique identifier for transaction
	sapOperation: str
		SAP operation that will be triggered on central server
	jsonParameters: dict
		the SAP inputs required to start the SAP operation
	testEnvironment : boolean 
		   default true
		flag to check if the BAPI transaction goes to T16 environment
	
	Returns
	-------
	str
		uuid that identifies this transaction.
	"""
	
	# Select target system
	if testEnvironment:
		tableName = "transactions_test"
	else:
		tableName = "transactions_prod"

	# Get globales
	currentTime = system.tag.read("[System]Gateway/CurrentDateTime").value
	
	# Try to update / adding the operation
	# TO DO : CHECK VALIDITY OF JSON etc...
	
	# Find id for this UUID
	checkSql = "SELECT id FROM " + tableName + " WHERE uuid = ?"	
	id = system.db.runScalarPrepQuery(checkSql,args=[uuid], database="sga_sap_bapi")
	
	# If found, try to add JSON request querry to the array of requests
	if id:
		sql = "UPDATE " + tableName + " SET request = JSON_ARRAY_APPEND(request, '$', CAST(? AS JSON)), request_received_timestamp = ? WHERE uuid = ?"
		jsonParameters["parameterValues"]["sessionUUID"] = uuid
		#Standard json to send to centrals
		requestJson = jsonParameters
		args = [system.util.jsonEncode(requestJson), currentTime, uuid]
		rows = system.db.runPrepUpdate(sql, args, database="sga_sap_bapi")
		
		return "Successfully added sap operation to database"
		
	return "The uuid is not in the database, please use beginTransaction to get uuid."

#===============================================================================

def tryTransaction(uuid, testEnvironment=True, proposedTemplate=[]):
	"""
	Function to test a SAP operations of transaction. This will send the request to central server and it will execute the BAPI commands. Depending on the SAP return
	the system will rollback the transaction or leave the session open.
	
	Parameters
	----------
	uuid: str
		unique identifier for transaction
	testEnvironment : boolean 
		   default true
		flag to check if the BAPI transaction goes to T16 environment
	
	Returns
	-------
	str
		Status what is happening with the transaction
	"""
	
	# Select target system
	if testEnvironment:
		tableName = "transactions_test"
		remoteGateway = "IDS-HUB5"
	else:
		tableName = "transactions_prod"
		remoteGateway = system.tag.read("[default]Factory/param_gw_interfaces").value

	# Get globals
	factoryCode = system.tag.read("[default]Factory/param_code").value
					
	# Get table of requests to be processed
	sql = "SELECT * FROM " + tableName + " WHERE uuid = ?"
	ds = system.db.runPrepQuery(sql, args=[uuid], database="sga_sap_bapi")
	pyDs = system.dataset.toPyDataSet(ds)
	
	if len(pyDs)>0:
			
		row = pyDs[0]
		
		# Prepare internal set of data used for communication
		pfceInternals = {"fromFactory":row["factory_gateway"],
						 "factoryCode": factoryCode,
						 "uuid":uuid,
						}
		
		# Build the main payload
		payload = {"pfceInternals":pfceInternals,
				   "requests":row["request"]}
		
		# Update status of communication to 2
		updateSql = "UPDATE "+tableName+ " SET status_communication = ?, debug = ? WHERE uuid = ?"
		rows = system.db.runPrepUpdate(updateSql, args=[2, system.util.jsonEncode(proposedTemplate) ,uuid], database="sga_sap_bapi")

		# Try the full transaction
		answer = system.util.sendRequest("pfce_sap_bapi", "tryTransaction", payload = payload, remoteServer = remoteGateway, timeoutSec=300)
		
		# Parse the result
		statusSapAction = answer["status_sap_action"]
		answers = answer["answers"]
		guiText = answer["text"]

		# Get time update 
		answerSentTime = system.tag.read("[System]Gateway/CurrentDateTime").value #system.date.now() NOT ALIGNED WITH TIMEZONE
		
		# Try to update with the feedback
		updateSql = "UPDATE "+ tableName +" SET answer = ?, answer_sent_timestamp = ?, status_communication = ?, status_sap_action = ? WHERE uuid = ?" 
		rows = system.db.runPrepUpdate(updateSql, args=[system.util.jsonEncode(answers), answerSentTime, 3,statusSapAction, uuid], database="sga_sap_bapi")
		
		return guiText

	return "No line with this UUID in table."

#===============================================================================	
# DEPRECATED, NO ?

def commitTransaction(uuid, test=True):
	"""
	Function to commit a SAP operations of transaction. This command will finish the session and commit operations of transaction (make it permanent)
	
	Parameters
	----------
	uuid: str
		unique identifier for transaction
	testEnvironment : boolean 
		   default true
		flag to check if the BAPI transaction goes to T16 environment
	
	Returns
	-------
	str
		Status what is happening with the transaction
	"""
	remoteGateway = "SGA-HUB2"
	tableName = "transactions_test"
	if not testEnvironment:
		tableName = "transactions_prod"
		remoteGateway = "SGA-HUB1"
	sql = "SELECT * FROM " + tableName + " WHERE uuid = ?"
	ds = system.db.runPrepQuery(sql, args=[uuid], database="sga_sap_bapi")
	
	pyDs = system.dataset.toPyDataSet(ds)
	
	if len(pyDs)>0:
		payload = {"uuid": uuid}
		answer = system.util.sendRequest("pfce_sap_bapi", "commitTransaction", payload = payload, remoteServer = remoteGateway)
		
		
		
def getOperations(uuid, testEnvironment=True):
	tableName = "transactions_test"
	if not testEnvironment:
		tableName = "transactions_prod"
	sql = "SELECT request, answer FROM " + tableName + " WHERE uuid = ?"
	
	ds = system.db.runPrepQuery(sql, args=[uuid], database="sga_sap_bapi")
	
	
	# Divide into two lists (requests and answers)
	requests, answers = ds[0]		
	requests = system.util.jsonDecode(requests)
	answers = system.util.jsonDecode(answers)

	# Prepare data for requests (is there always)		
	data = []
	for request in requests:
		name = request["bapi"]
		formatedRequest = system.util.jsonEncode(request, 3)
		data.append([name,formatedRequest,""])
	
	# Prepare data for answers (not always the case)
	if answers:		
		for idx,answer in enumerate(answers):
			for name in answer:
				formatedAnswer = system.util.jsonEncode(answer[name], 3)
				data[idx][2] = formatedAnswer
	
	# Build result dataset
	headers = ['operation', "request", "answer"]
	ds = system.dataset.toDataSet(headers, data)
	pyDs = system.dataset.toPyDataSet(ds)
	return pyDs
	
def getFilledConfirmationTemplateBk(ignitionTagPath, workorder, stepId = None, testPo = None, addTogether=False):
	#bapiDs = system.tag.read(ignitionTagPath + "/mes/confirmationSequence").value
	bapiDs = system.tag.read(ignitionTagPath + "/confirmationSequence").value
	
	bapiPyDs = system.dataset.toPyDataSet(bapiDs)
	filledTemplates = []
	for row in bapiPyDs:
		bapiTemplate = row["BAPI"]
		
		if stepId:
			data = getDataForWorkorderSpecificRow(ignitionTagPath, workorder, stepId)
		else:
			data = getDataForWorkorder(ignitionTagPath, workorder)
		
		
		#inputRoot = template["parameterValues"]["input"]["inputRoot"]
		#print inputRoot
		if bapiTemplate == "BAPI_PRODORDCONF_CREATE_TT":
			if addTogether:
				filledTemplate = fillTimeTicketTemplateConcatinated(data, testPo)
			else:
				filledTemplate = fillTimeTicketTemplate(data, testPo)
			filledTemplates.append(filledTemplate)	
		if bapiTemplate == "BAPI_TRANSACTION_COMMIT":
			filledTemplate = shared.sap.templates.BAPI_TRANSACTION_COMMIT
			filledTemplates.append(filledTemplate)	
		
	return filledTemplates

def getFilledConfirmationTemplate(ignitionTagPath, workorder, stepId = None, testPo = None, addTogether=False):
	
	filledTemplates = []

		
	if stepId:
		data = getDataForWorkorderSpecificRow(ignitionTagPath, workorder, stepId)
	else:
		data = getDataForWorkorder(ignitionTagPath, workorder)
	
	
	backflushTagPath  = ignitionTagPath + "/mes/sap/param_backflushDefaultBehavior"
	backflushBehavior = "NoConsumption"
	bomData = getBomForWorkorder(ignitionTagPath, workorder)
	
	if system.tag.exists(backflushTagPath):
		backflushBehavior = system.tag.read(backflushTagPath).value
	else:
		message = "Backflush tag is missing using NoConsumption as default value. Contact PFCE."
		system.util.sendMessage("pfce_sga_interfaces", "clientLogger", payload={"level":"debug", "logger":"SAP gui", "message":message})
	
	if addTogether:
		filledTemplate = fillTimeTicketTemplateConcatinatedBackflush(data, testPo,bomData, backflushBehavior)
	else:
		filledTemplate = fillTimeTicketTemplate(data, testPo)
	filledTemplates.append(filledTemplate)	
	
	filledTemplate = shared.sap.templates.BAPI_TRANSACTION_COMMIT
	filledTemplates.append(filledTemplate)	
		
	return filledTemplates
		

	
def setDataForWorkorderSpecificRow(ignitionTagPath, workorder, bapiAnswer, confirmationResult, stepIds=[],testEnvironment=True):
	resultField = "erp_confirmation_res"
	bapiField = "erp_confirmation"
	if testEnvironment:
		resultField = "erp_test_confirmation_res"
		bapiField = "erp_test_confirmation"
	
	tagPaths = 	[ignitionTagPath + "/mes/param_sapStepId",
				 ignitionTagPath + "/mes/param_sapWorkCenter"]	
			
	tagValues = system.tag.readAll(tagPaths)
	
	stepNumber = tagValues[0].value		
	workcenter = tagValues[1].value
	
	sql = """UPDATE step_result 
			SET """ + resultField +""" = ?, 
			""" + bapiField +""" = JSON_ARRAY_APPEND(IFNULL("""+bapiField+""", JSON_ARRAY()), "$",CAST(? AS JSON)) 
			WHERE workorder_id = ? and step_id = ? """
	if stepIds:
		sql += "and id in ( '" + "','".join(stepIds) + "')"
	#return sql
	args = [system.util.jsonEncode(confirmationResult), system.util.jsonEncode(bapiAnswer), workorder, stepNumber]
	
	system.db.runPrepUpdate(sql, args=args, database="factory_production")
	
def getBomForWorkorder(ignitionTagPath, workorder):
	tagPaths = 	[ignitionTagPath + "/mes/prod_sapStepId",
	ignitionTagPath + "/mes/param_sapWorkCenter"]	

	tagValues = system.tag.readAll(tagPaths)
		
	stepNumber = tagValues[0].value		
	workcenter = tagValues[1].value	
	
	
	return shared.mes.workorder.getStepMaterials(workorder, workcenter, stepNumber)

	
		
def getDataForWorkorder(ignitionTagPath, workorder):
	sql = """SELECT s.id operationNumber, 
				wc.code workcenter, 
				w.number workorder, 
				sr.material_number mat, 
				s.data->"$.duration" duration, 
				sr.quantity outfeed,
				s.data->"$.rejects" rejects,
				TRIM(BOTH '"' FROM CAST(s.data -> '$.start' AS CHAR CHARACTER SET utf8)) as startDateTime,
				TRIM(BOTH '"' FROM CAST(s.data -> '$.end' AS CHAR CHARACTER SET utf8)) as endDateTime,
				sr.unit units,
				s.workcenter_id code, s.workorder_id, s.data,
				sr.id,sr.quantity, sr.data mesData, sr.rejects jsonReject
				FROM step s 
				 join step_result sr on sr.step_id = s.id and sr.workorder_id=s.workorder_id
				 join workorder w on w.id = s.workorder_id
				 join workcenter wc on wc.id=s.workcenter_id
				where w.number = ? and wc.code = ? and s.id = ?"""
	
	tagPaths = 	[ignitionTagPath + "/mes/prod_sapStepId",
				ignitionTagPath + "/mes/param_sapWorkCenter",
				ignitionTagPath + "/mes/sap/param_confirmationTestMode"]	
	
	tagValues = system.tag.readAll(tagPaths)
	
	stepNumber = tagValues[0].value		
	workcenter = tagValues[1].value		
	confirmationTest = tagValues[2].value				
	
	if confirmationTest:
		sql += """ and (sr.erp_test_confirmation_res is null or sr.erp_test_confirmation_res->"$.result" != 2)"""
	else:
		sql += """ and (sr.erp_confirmation_res is null or sr.erp_confirmation_res->"$.result" != 2)"""
	
	args = [workorder, workcenter, stepNumber]
		
	mesPyDs = system.db.runPrepQuery(sql, args, 'factory_production')
	
	
	
	return mesPyDs

def getDataForWorkorderSpecificRow(ignitionTagPath, workorder, stepIds = []):
	sql = """SELECT s.id operationNumber, 
				wc.code workcenter, 
				w.number workorder, 
				sr.material_number mat, 
				s.data->"$.duration" duration, 
				sr.quantity outfeed,
				s.data->"$.rejects" rejects,
				TRIM(BOTH '"' FROM CAST(s.data -> '$.start' AS CHAR CHARACTER SET utf8)) as startDateTime,
				TRIM(BOTH '"' FROM CAST(s.data -> '$.end' AS CHAR CHARACTER SET utf8)) as endDateTime,
				sr.unit units,
				s.workcenter_id code, s.workorder_id, s.data,
				sr.id,sr.quantity, sr.data mesData, sr.rejects jsonReject
				FROM step s 
				left join step_result sr on sr.step_id = s.id and sr.workorder_id=s.workorder_id
				left join workorder w on w.id = s.workorder_id
				left join workcenter wc on wc.id=s.workcenter_id
				where w.number = ? and wc.code = ? and s.id = ? and (sr.erp_confirmation_res is null or sr.erp_confirmation_res->"$.result" != 2) and sr.id in """
	
	tagPaths = 	[ignitionTagPath + "/mes/prod_sapStepId",
				ignitionTagPath + "/mes/param_sapWorkCenter"]	
	
	tagValues = system.tag.readAll(tagPaths)
	sql += "( '" + "','".join(stepIds) + "')"
	#print sql
	stepNumber = tagValues[0].value		
	workcenter = tagValues[1].value						
					
	args = [workorder, workcenter, stepNumber]
		
	mesPyDs = system.db.runPrepQuery(sql, args, 'factory_production')
	
	return mesPyDs

def fillGoodsMovementTemplate(mesPyDs):
	from copy import deepcopy
	template = deepcopy(shared.sap.templates.BAPI_GOODSMVT_CREATE)
	templateInput = template["parameterValues"]["input"]["inputRoot"]["INPUT"]
	templateItem = template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMVT_ITEM"]["item"]
	
	for row in mesPyDs:
		mesData = row["mesData"]
		
		if mesData:
			mesJson = system.util.jsonDecode(mesData)
			#print mesJson["materialConsumption"]
			if "value" in mesDataJson["materialConsumption"]:
				materialConsumption =  mesDataJson["materialConsumption"]["value"]
			else:
				materialConsumption =  mesDataJson["materialConsumption"]
			matDs = system.dataset.toDataSet(mat["headers"], mat["values"])
			matPyDs = system.dataset.toPyDataSet(matDs)
			for consumedMat in matPyDs:
				materialCode = consumedMat['materialNumber']
				materialName = consumedMat['materialDesc']
				units = consumedMat['unit']
				amount = float(consumedMat['usedQuantity'])
				orderedQuantity = float(consumedMat['orderedQuantity'])
				percentage = float(consumedMat['percentage'])
				lgort = consumedMat['storage']
				batchNumber = consumedMat['batchNumber']
				materialMovement = consumedMat['materialMovement']
				plant = consumedMat['plant']
				date = consumedMat['date']
				# itemCategory = str(row['itemCategory'])
				#print list(consumedMat)
				if consumedMat['itemCategory'] == 'L':
					pass

def fillTimeTicketTemplate(mesPyDs, testPo):
	from copy import deepcopy
	template = deepcopy(shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT)
	templateItemTT = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"])
	templateItemGM = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"])
	templateItemGMlink = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"])

	#print mesPyDs
	tmpItem = []
	tmpGoodsMovement = []
	tmpLink = []
	ttCounter = 1
	goodsCounter = 1
	for row in mesPyDs:
		
		mainTemplate = deepcopy(templateItemTT[0])
		

		outfeed = row["outfeed"]
		rejects = row["jsonReject"]
		workcenter = row["workcenter"]
		operation = row["operationNumber"]
		workorder = row["workorder"]
		
		if testPo:
			workorder = testPo
		plant = system.tag.read("[default]Factory/param_code").value #find datapoint
		mesData = row["mesData"]
		
		mesDataJson = system.util.jsonDecode(mesData)
		parser = shared.utils.date.date_time_iso_8061_parser()
		startDate =  parser.parse(mesDataJson["start"])
		endDate = parser.parse(mesDataJson["end"])
		
		
		
		if "materialConsumption" in mesDataJson:
			materialConsumption =  mesDataJson["materialConsumption"]["value"]
			matDs = system.dataset.toDataSet(materialConsumption["headers"],materialConsumption["values"])
			matPyDs = system.dataset.toPyDataSet(matDs)
			
			for consumedMat in matPyDs:
				goodsTemplate = deepcopy(templateItemGM[0])
				linkTemplate = deepcopy(templateItemGMlink[0])
				
				materialCode = consumedMat['materialNumber']
				materialName = consumedMat['materialDesc']
				units = consumedMat['unit']
				amount = float(consumedMat['usedQuantity'])
				orderedQuantity = float(consumedMat['orderedQuantity'])
				percentage = float(consumedMat['percentage'])
				lgort = consumedMat['storage']
				batchNumber = consumedMat['batchNumber']
				materialMovement = consumedMat['materialMovement']
				plant = consumedMat['plant']
				date = consumedMat['date']
				# check if goodsMovement needs to be confirmed
				if consumedMat['itemCategory'] == 'L':
					#Prepare goods movement
					goodsTemplate["BATCH"] = batchNumber
					goodsTemplate["ENTRY_QNT"] = amount
					goodsTemplate["ENTRY_UOM"] = units
					goodsTemplate["MATERIAL"] = materialCode.zfill(18)
					goodsTemplate["MOVE_TYPE"] = materialMovement
					goodsTemplate["ORDERID"] = workorder.zfill(12)
					goodsTemplate["PLANT"] = plant
					goodsTemplate["STGE_LOC"] = lgort
					
					#Prepare goods movement and confimation link					
					linkTemplate["INDEX_CONFIRM"] = ttCounter
					linkTemplate["INDEX_GOODSMOV"] = goodsCounter
					
					#Add them to tmpArrays
					tmpLink.append(linkTemplate)
					tmpGoodsMovement.append(goodsTemplate)
					
					goodsCounter += 1
			
		
		duration = system.date.minutesBetween(startDate, endDate)
	
		outfeed = row["outfeed"]		
		
		date = system.date.format( system.date.now(), "yyyy-MM-dd")
		rejectNumber = 0
		if rejects:
			rejectsJson = system.util.jsonDecode(rejects)
			#Check for multiple rejects
			for rejectCode in rejectsJson:
				rejectNumber = rejectsJson[rejectCode]
				mainTemplate["DEV_REASON"] = rejectCode
				mainTemplate["SCRAP"] = rejectsJson[rejectCode]
	
		#print outfeed,rejectNumber
		mainTemplate["YIELD"] = outfeed - rejectNumber
		mainTemplate["WORK_CNTR"] = workcenter
		mainTemplate["OPERATION"] = operation
		mainTemplate["PLANT"] = plant 
		mainTemplate["ORDERID"] = workorder.zfill(12)
		
		mainTemplate["POSTG_DATE"] = date
		
		#Labor, machine and setup times
		mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
		mainTemplate["CONF_ACTIVITY1"] = "0"
		mainTemplate["CONF_ACTIVITY2"] = duration
		mainTemplate["CONF_ACTIVITY3"] = "0"
		
		tmpItem.append(mainTemplate)
		ttCounter += 1
	#templateItem = []
	
	template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"] = tmpItem
	if tmpGoodsMovement:
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"] = tmpGoodsMovement
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"] = tmpLink

	
	return template
	
			
			
def fillTimeTicketTemplateConcatinated(mesPyDs, testPo):
	from copy import deepcopy
	template = deepcopy(shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT)
	templateItemTT = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"])
	templateItemGM = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"])
	templateItemGMlink = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"])


	tmpItem = []
	tmpGoodsMovement = []
	tmpLink = []
	
	dictGoodsMovement = {}
	
	ttYield = 0
	ttReject = 0 
	ttRejectCode = ""
	ttStartDate = None
	ttEndDate = None
	ttDuration = 0
	
	ttCounter = 1
	goodsCounter = 1
	for row in mesPyDs:
				
		outfeed = row["outfeed"]
		rejects = row["jsonReject"]
		workcenter = row["workcenter"]
		operation = row["operationNumber"]
		workorder = row["workorder"]
		
		if testPo:
			workorder = testPo
		plant = system.tag.read("[default]Factory/param_code").value #find datapoint
		mesData = row["mesData"]
		
		mesDataJson = system.util.jsonDecode(mesData)
		parser = shared.utils.date.date_time_iso_8061_parser()
		startDate =  parser.parse(mesDataJson["start"])
		endDate = parser.parse(mesDataJson["end"])
		
		ttDuration +=  system.date.minutesBetween(startDate, endDate)
		
		# always take the latest, earliest date.
		if ttStartDate:
			if system.date.isBefore(startDate, ttStartDate):
				ttStartDate = startDate
		else:
			ttStartDate = startDate
		
		if ttEndDate:
			if system.date.isAfter(endDate, ttEndDate):
				ttEndDate = endDate
		else:
			ttEndDate = endDate		
				
				
		if "materialConsumption" in mesDataJson:
			materialConsumption =  mesDataJson["materialConsumption"]["value"]
			matDs = system.dataset.toDataSet(materialConsumption["headers"],materialConsumption["values"])
			matPyDs = system.dataset.toPyDataSet(matDs)
			
			for consumedMat in matPyDs:
				goodsTemplate = deepcopy(templateItemGM[0])
				linkTemplate = deepcopy(templateItemGMlink[0])
				
				materialCode = consumedMat['materialNumber']
				materialName = consumedMat['materialDesc']
				units = consumedMat['unit']
				amount = float(consumedMat['usedQuantity'])
				orderedQuantity = float(consumedMat['orderedQuantity'])
				percentage = float(consumedMat['percentage'])
				lgort = consumedMat['storage']
				batchNumber = consumedMat['batchNumber']
				materialMovement = consumedMat['materialMovement']
				plant = consumedMat['plant']
				date = consumedMat['date']
				# check if goodsMovement needs to be confirmed
				if consumedMat['itemCategory'] == 'L':
					#Prepare goods movement
					goodsTemplate["BATCH"] = batchNumber
					goodsTemplate["ENTRY_QNT"] = amount
					goodsTemplate["ENTRY_UOM"] = units
					goodsTemplate["MATERIAL"] = materialCode.zfill(18)
					goodsTemplate["MOVE_TYPE"] = materialMovement
					if materialMovement == "101":
			
						goodsTemplate["MVT_IND"] = "F"
					goodsTemplate["ORDERID"] = workorder.zfill(12)
					goodsTemplate["PLANT"] = plant
					goodsTemplate["STGE_LOC"] = lgort
					
					key = materialCode.zfill(18)+batchNumber
					
					if key in dictGoodsMovement:
						dictGoodsMovement[key]["ENTRY_QNT"] += amount
					else:		
						dictGoodsMovement[key] = goodsTemplate
					
				
		

		
		date = system.date.format(system.date.now(), "yyyy-MM-dd")
		rejectNumber = 0
		if rejects:
			rejectsJson = system.util.jsonDecode(rejects)
			# Check for multiple rejects
			if isinstance(rejectsJson, dict):
			
				for rejectCode in rejectsJson:
					rejectNumber = rejectsJson[rejectCode]
					ttReject += rejectsJson[rejectCode]
					ttRejectCode = rejectCode

			else:
				for code in rejectsJson:
					ttRejectCode = code
				ttReject += 1
		ttYield += outfeed
		
	mainTemplate = deepcopy(templateItemTT[0])
	
	for gm in dictGoodsMovement:
		linkTemplate = deepcopy(templateItemGMlink[0])
		gmTemplateInstance = dictGoodsMovement[gm]
		
		
		linkTemplate["INDEX_CONFIRM"] = ttCounter
		linkTemplate["INDEX_GOODSMOV"] = goodsCounter
		
		#Add them to tmpArrays
		tmpLink.append(linkTemplate)
		tmpGoodsMovement.append(gmTemplateInstance)
		
		goodsCounter += 1
	
	if ttYield > 0:
		# duration = system.date.minutesBetween(ttStartDate, ttEndDate)
		mainTemplate["DEV_REASON"] = ttRejectCode
		mainTemplate["SCRAP"] = ttReject
		mainTemplate["YIELD"] = ttYield - ttReject
		mainTemplate["WORK_CNTR"] = workcenter
		mainTemplate["OPERATION"] = operation
		mainTemplate["PLANT"] = plant 
		mainTemplate["ORDERID"] = workorder.zfill(12)
		
		mainTemplate["POSTG_DATE"] = date
		
		#Labor, machine and setup times
		mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
		mainTemplate["CONF_ACTIVITY1"] = "0"
		mainTemplate["CONF_ACTIVITY2"] = ttDuration
		mainTemplate["CONF_ACTIVITY3"] = "0"
	
	tmpItem.append(mainTemplate)
	
	template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"] = tmpItem
	if tmpGoodsMovement:
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"] = tmpGoodsMovement
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"] = tmpLink

	
	return template
	
def fillTimeTicketTemplateConcatinatedBackflush(mesPyDs, testPo, bomData, backflushBehavior):
	from copy import deepcopy
	template = deepcopy(shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT)
	templateItemTT = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"])
	templateItemGM = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"])
	templateItemGMlink = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"])


	tmpItem = []
	tmpGoodsMovement = []
	tmpLink = []
	
	dictGoodsMovement = {}
	
	ttYield = 0
	ttReject = 0 
	ttRejectCode = ""
	ttStartDate = None
	ttEndDate = None
	ttDuration = 0
	
	ttCounter = 1
	goodsCounter = 1
	p16Workorder = ""
	for row in mesPyDs:
				
		outfeed = row["outfeed"]
		rejects = row["jsonReject"]
		workcenter = row["workcenter"]
		operation = row["operationNumber"]
		workorder = row["workorder"]
		p16Workorder = row["workorder"]
		if testPo:
			workorder = testPo
		plant = system.tag.read("[default]Factory/param_code").value #find datapoint
		mesData = row["mesData"]
		
		mesDataJson = system.util.jsonDecode(mesData)
		parser = shared.utils.date.date_time_iso_8061_parser()
		startDate =  parser.parse(mesDataJson["start"])
		endDate = parser.parse(mesDataJson["end"])
		
		ttDuration +=  system.date.minutesBetween(startDate, endDate)
		
		# always take the latest, earliest date.
		if ttStartDate:
			if system.date.isBefore(startDate, ttStartDate):
				ttStartDate = startDate
		else:
			ttStartDate = startDate
		
		if ttEndDate:
			if system.date.isAfter(endDate, ttEndDate):
				ttEndDate = endDate
		else:
			ttEndDate = endDate		
				
				
		if "materialConsumption" in mesDataJson:
			materialConsumption =  mesDataJson["materialConsumption"]["value"]
			matDs = system.dataset.toDataSet(materialConsumption["headers"],materialConsumption["values"])
			matPyDs = system.dataset.toPyDataSet(matDs)
			
			for consumedMat in matPyDs:
				goodsTemplate = deepcopy(templateItemGM[0])
				linkTemplate = deepcopy(templateItemGMlink[0])
				
				materialCode = consumedMat['materialNumber']
				materialName = consumedMat['materialDesc']
				units = consumedMat['unit']
				amount = float(consumedMat['usedQuantity'])
				orderedQuantity = float(consumedMat['orderedQuantity'])
				percentage = float(consumedMat['percentage'])
				lgort = consumedMat['storage']
				batchNumber = consumedMat['batchNumber']
				materialMovement = consumedMat['materialMovement']
				plant = consumedMat['plant']
				date = consumedMat['date']
				# check if goodsMovement needs to be confirmed
				if consumedMat['itemCategory'] == 'L':
					#Prepare goods movement
					goodsTemplate["BATCH"] = batchNumber
					goodsTemplate["ENTRY_QNT"] = amount
					goodsTemplate["ENTRY_UOM"] = units
					goodsTemplate["MATERIAL"] = materialCode.zfill(18)
					goodsTemplate["MOVE_TYPE"] = materialMovement
					if materialMovement == "101":
			
						goodsTemplate["MVT_IND"] = "F"
					goodsTemplate["ORDERID"] = workorder.zfill(12)
					goodsTemplate["PLANT"] = plant
					goodsTemplate["STGE_LOC"] = lgort
					
					key = materialCode.zfill(18)+batchNumber
					#print amount
					if key in dictGoodsMovement:
						dictGoodsMovement[key]["ENTRY_QNT"] += amount
					else:		
						dictGoodsMovement[key] = goodsTemplate
					
				
		

		
		date = system.date.format(system.date.now(), "yyyy-MM-dd")
		rejectNumber = 0
		if rejects:
			rejectsJson = system.util.jsonDecode(rejects)
			# Check for multiple rejects
			if isinstance(rejectsJson, dict):
			
				for rejectCode in rejectsJson:
					rejectNumber = rejectsJson[rejectCode]
					ttReject += rejectsJson[rejectCode]
					ttRejectCode = rejectCode

			else:
				for code in rejectsJson:
					ttRejectCode = code
				ttReject += 1
		ttYield += outfeed
		
	mainTemplate = deepcopy(templateItemTT[0])		
	
	if ttYield > 0:
		# duration = system.date.minutesBetween(ttStartDate, ttEndDate)
		mainTemplate["DEV_REASON"] = ttRejectCode
		mainTemplate["SCRAP"] = ttReject
		mainTemplate["YIELD"] = ttYield - ttReject
		mainTemplate["WORK_CNTR"] = workcenter
		mainTemplate["OPERATION"] = operation
		mainTemplate["PLANT"] = plant 
		mainTemplate["ORDERID"] = workorder.zfill(12)
		
		mainTemplate["POSTG_DATE"] = date
		
		#Labor, machine and setup times
		mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
		mainTemplate["CONF_ACTIVITY1"] = "0"
		mainTemplate["CONF_ACTIVITY2"] = ttDuration
		mainTemplate["CONF_ACTIVITY3"] = "0"
	
	
	if len(dictGoodsMovement) > 0:
			
		for gm in dictGoodsMovement:
			linkTemplate = deepcopy(templateItemGMlink[0])
			gmTemplateInstance = dictGoodsMovement[gm]
			
			
			linkTemplate["INDEX_CONFIRM"] = ttCounter
			linkTemplate["INDEX_GOODSMOV"] = goodsCounter
			
			#Add them to tmpArrays
			tmpLink.append(linkTemplate)
			tmpGoodsMovement.append(gmTemplateInstance)
			
			goodsCounter += 1
	else:
		# TEMPORARY BACKFLUSH FUNCTIONALITY REMOVE AFTER CONFIRMATION WINDOW ORDER IS IN DEV
		if p16Workorder:
			mesWorkorder = system.mes.workorder.getMESWorkOrder(p16Workorder)
			totalQuantity = mesWorkorder.getWorkOrderQuantity()	
			for mat in bomData:
				
				if mat["itemCategory"] == "L":
					goodsTemplate = deepcopy(templateItemGM[0])
					linkTemplate = deepcopy(templateItemGMlink[0])
					goodsTemplate["BATCH"] = mat["batchNumber"]
					
					if backflushBehavior == "NoConsumption":
						goodsTemplate["ENTRY_QNT"] = 0
					else:
						if totalQuantity > 0:
							goodsTemplate["ENTRY_QNT"] = ((ttYield + ttReject) / totalQuantity) * float(mat["requiredQuantity"])
						else:
							goodsTemplate["ENTRY_QNT"] = 0
					goodsTemplate["ENTRY_UOM"] = mat["measureUnit"]
					goodsTemplate["MATERIAL"] = mat["materialNumber"].zfill(18)
					goodsTemplate["MOVE_TYPE"] = 261
					
					goodsTemplate["ORDERID"] = workorder.zfill(12)
					goodsTemplate["PLANT"] = plant
					goodsTemplate["STGE_LOC"] = mat["consumMaterialStorage"]
					
					linkTemplate["INDEX_CONFIRM"] = ttCounter
					linkTemplate["INDEX_GOODSMOV"] = goodsCounter
					
					tmpLink.append(linkTemplate)
					tmpGoodsMovement.append(goodsTemplate)
					
					goodsCounter += 1
	
	tmpItem.append(mainTemplate)
	
	template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"] = tmpItem
	if tmpGoodsMovement:
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"] = tmpGoodsMovement
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"] = tmpLink

	
	return template
	

def prepareTemplate(gmTemplates, rejects, rejectsCodes, yields, duration, mesDs, finalConfirmation, rejectCodeJson={}):
	from copy import deepcopy
	template = deepcopy(shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT)
	templateItemTT = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"])
	templateItemGM = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"])
	templateItemGMlink = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"])

	#print rejectCodeJson

	tmpItem = []
	tmpGoodsMovement = []
	tmpLink = []

	mesPyDs = system.dataset.toPyDataSet(mesDs)
	
	workcenter = ""
	operation = ""
	workorder = ""
	plant = ""
	
	gmCounter = 1
	ttCounter = 1
	
	for row in mesPyDs:
		workcenter = row["workcenter"]
		operation = row["operationNumber"]
		workorder = row["workorder"]
	
	workorderDs = shared.mes.workorder.getWorkOrders(workorder, fields=["plant"])	
	if len(workorderDs)>0:
		#Get it from iDoc
		plant = workorderDs[0]["plant"]
	else:
		# If not existant take it from tag
		plant = system.tag.read("[default]Factory/param_code").value	
			
	#Build Timeticket from window
	date = system.date.format(system.date.now(), "yyyy-MM-dd")
	
	if len(rejectCodeJson) > 0:
		cnt = 1
		for code in rejectCodeJson:
			rejNum = rejectCodeJson[code]
			mainTemplate = deepcopy(templateItemTT[0])	
			if cnt == 1:
				mainTemplate["DEV_REASON"] = code
				mainTemplate["SCRAP"] = "%.3f" % float(rejNum)
				mainTemplate["YIELD"] = "%.3f" % float(yields)
				mainTemplate["WORK_CNTR"] = workcenter
				mainTemplate["OPERATION"] = operation
				mainTemplate["PLANT"] = plant 
				mainTemplate["ORDERID"] = workorder.zfill(12)
				
				mainTemplate["POSTG_DATE"] = date
				
				#Labor, machine and setup times
				mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
				mainTemplate["CONF_ACTIVITY1"] = "0"
				mainTemplate["CONF_ACTIVITY2"] = duration
				mainTemplate["CONF_ACTIVITY3"] = "0"
				
				if finalConfirmation:
					mainTemplate["FIN_CONF"] = "X"
					mainTemplate["CLEAR_RES"] = "X"
			else:
				mainTemplate["DEV_REASON"] = code
				mainTemplate["SCRAP"] = "%.3f" % float(rejNum)
				mainTemplate["YIELD"] = 0
				mainTemplate["WORK_CNTR"] = workcenter
				mainTemplate["OPERATION"] = operation
				mainTemplate["PLANT"] = plant 
				mainTemplate["ORDERID"] = workorder.zfill(12)
				
				mainTemplate["POSTG_DATE"] = date
				
				#Labor, machine and setup times
				mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
				mainTemplate["CONF_ACTIVITY1"] = "0"
				mainTemplate["CONF_ACTIVITY2"] = "0"
				mainTemplate["CONF_ACTIVITY3"] = "0"
				
				if finalConfirmation:
					mainTemplate["FIN_CONF"] = "X"
					mainTemplate["CLEAR_RES"] = "X"
			
			cnt += 1
			tmpItem.append(mainTemplate)
	else:
		mainTemplate = deepcopy(templateItemTT[0])	
		mainTemplate["DEV_REASON"] = rejectsCodes
		mainTemplate["SCRAP"] = "%.3f" % float(rejects)
		mainTemplate["YIELD"] = "%.3f" % float(yields)
		mainTemplate["WORK_CNTR"] = workcenter
		mainTemplate["OPERATION"] = operation
		mainTemplate["PLANT"] = plant 
		mainTemplate["ORDERID"] = workorder.zfill(12)
		
		mainTemplate["POSTG_DATE"] = date
		
		#Labor, machine and setup times
		mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
		mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
		mainTemplate["CONF_ACTIVITY1"] = "0"
		mainTemplate["CONF_ACTIVITY2"] = duration
		mainTemplate["CONF_ACTIVITY3"] = "0"
		
		if finalConfirmation:
			mainTemplate["FIN_CONF"] = "X"
			mainTemplate["CLEAR_RES"] = "X"
		tmpItem.append(mainTemplate)
	# build goods movement
	for gm in gmTemplates:
		goodsTemplate = deepcopy(templateItemGM[0])
		linkTemplate = deepcopy(templateItemGMlink[0])
		
		#Prepare goods movement
		goodsTemplate["BATCH"] = gm.batchNumber
		if gm.overrideAmount > 0 :
			goodsTemplate["ENTRY_QNT"] ="%.3f" % float(gm.overrideAmount)
		else:
			goodsTemplate["ENTRY_QNT"] = "%.3f" % float(gm.getComponent("txtActualAmount").doubleValue)
		goodsTemplate["ENTRY_UOM_ISO"] = gm.units
		goodsTemplate["MATERIAL"] = gm.materialNumber.zfill(18)
		
		if gm.materialOrigin == "Produced":
			
			goodsTemplate["MOVE_TYPE"] = 101
			goodsTemplate["MVT_IND"] = "F"
		else:
			goodsTemplate["MOVE_TYPE"] = 261
		goodsTemplate["ORDERID"] = workorder.zfill(12)
		goodsTemplate["PLANT"] = plant
		goodsTemplate["STGE_LOC"] = gm.storageLocation
		
		
		
		
		linkTemplate["INDEX_CONFIRM"] = ttCounter
		linkTemplate["INDEX_GOODSMOV"] = gmCounter
		
		#Add them to tmpArrays
		tmpLink.append(linkTemplate)
		tmpGoodsMovement.append(goodsTemplate)
		
		gmCounter += 1
		
		
	
	
	template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"] = tmpItem
	if tmpGoodsMovement:
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"] = tmpGoodsMovement
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"] = tmpLink

	
	filledTemplates = []
	
	filledTemplates.append(template)
	filledTemplate = shared.sap.templates.BAPI_TRANSACTION_COMMIT
	filledTemplates.append(filledTemplate)
	
	return filledTemplates	


def checkBomQuantities(bomDs, inputMatNumber, inputBatchNumber=None):
	#print inputMatNumber,inputBatchNumber
	
	#print system.dataset.getColumnHeaders(bomDs)
	for mat in bomDs:
		#print list(mat)
		if inputBatchNumber:
			matNumber = ""
			if mat["materialNumber"]:
				matNumber = mat["materialNumber"]			
			batchNumber = "" 
			if mat["batchNumber"]:
				batchNumber = mat["batchNumber"] 		
			if inputMatNumber == matNumber and inputBatchNumber == batchNumber:
				return mat["requiredQuantity"]
		else:
			matNumber = ""
			if mat["materialNumber"]:
				matNumber = mat["materialNumber"]
			if inputMatNumber == matNumber :
				return mat["requiredQuantity"]
	return 0.0
	
	
def prepareWindowData(mesMat, bomMat, ignitionTagPath, workorder,confirmationMode,backflushConfiguration):
	from decimal import Decimal
	mesWorkorder = system.mes.workorder.getMESWorkOrder(workorder)
	totalQuantity = mesWorkorder.getWorkOrderQuantity()		
	
	tagPaths = 	[ignitionTagPath + "/mes/prod_sapStepId",
	ignitionTagPath + "/mes/param_sapWorkCenter"]	

	tagValues = system.tag.readAll(tagPaths)
		
	stepNumber = tagValues[0].value		
	workcenter = tagValues[1].value	
	
	controlKey = "PP01"
	outUnits = ""
	
	backflush = backflushConfiguration
	producedInserted = False
	
	controlKeyDs = shared.mes.workorder.getSAPProductionStepDetails(workorder, workcenter, stepNumber, fields=["controlKey","units"])
	workorderDs = shared.mes.workorder.getWorkOrders(workorder, fields=["storage","units","material","quantity","confirmationISOUnits","plant"])
	
	if len(controlKeyDs) > 0:
		if controlKeyDs[0][0]:
			controlKey = controlKeyDs[0][0]
		outUnits = controlKeyDs[0][1]
	if len(workorderDs)>0:
		#Get it from iDoc
		plant = workorderDs[0]["plant"]
	else:
		# If not existant take it from tag
		plant = system.tag.read("[default]Factory/param_code").value
	#print controlKey
	
	dictGoodsMovement = {}
		
	ttYield = 0
	ttReject = 0 
	ttRejectCode = ""
	ttRejectCodes = {}
	ttStartDate = None
	ttEndDate = None
	ttDuration = 0
	
	ttCounter = 1
	goodsCounter = 1
	p16Workorder = ""
	for row in mesMat:
	
		currentBatchGM = set()
		currentMaterials = set()
		outfeed = row["outfeed"]
		rejects = row["jsonReject"]
		workcenter = row["workcenter"]
		operation = row["operationNumber"]
		workorder = row["workorder"]
		
		#if testPo:
		#	workorder = testPo
		mesData = row["mesData"]
		if not mesData:
			continue
		#print mesData
		mesDataJson = system.util.jsonDecode(mesData)
		parser = shared.utils.date.date_time_iso_8061_parser()
		startDate =  parser.parse(mesDataJson["start"])
		endDate = parser.parse(mesDataJson["end"])
		
		ttDuration +=  system.date.minutesBetween(startDate, endDate)
		
		# always take the latest, earliest date.
		if ttStartDate:
			if system.date.isBefore(startDate, ttStartDate):
				ttStartDate = startDate
		else:
			ttStartDate = startDate
		
		if ttEndDate:
			if system.date.isAfter(endDate, ttEndDate):
				ttEndDate = endDate
		else:
			ttEndDate = endDate		
				
				
		if "materialConsumption" in mesDataJson:
			if "value" in mesDataJson["materialConsumption"]:
				materialConsumption =  mesDataJson["materialConsumption"]["value"]
			else:
				materialConsumption =  mesDataJson["materialConsumption"]
			matDs = system.dataset.toDataSet(materialConsumption["headers"],materialConsumption["values"])
			matPyDs = system.dataset.toPyDataSet(matDs)
			
			for consumedMat in matPyDs:
			
				goodsTemplate = {}
				
				materialCode = consumedMat['materialNumber']
				materialName = consumedMat['materialDesc']
				units = consumedMat['unit']
				amount = float(consumedMat['usedQuantity'])
				orderedQuantity = float(consumedMat['orderedQuantity'])
				percentage = float(consumedMat['percentage'])
				lgort = consumedMat['storage']
				batchNumber = consumedMat['batchNumber']
				materialMovement = consumedMat['materialMovement']
				plant = consumedMat['plant']
				date = consumedMat['date']
				# check if goodsMovement needs to be confirmed
				if consumedMat['itemCategory'] == 'L':
					#Prepare goods movement
					
					goodsTemplate["batchNumber"] = batchNumber
					# confMOde
					goodsTemplate["confirmationMode"] = confirmationMode
					goodsTemplate["goodsMovementSeqNum"] = goodsCounter
					goodsTemplate["materialNumber"] = materialCode
					if materialMovement == 101 or materialMovement == "101":
						goodsTemplate["materialOrigin"] = "Produced"
						producedInserted = True
					else:
						goodsTemplate["materialOrigin"] = "ActualData"
					goodsTemplate["mesAmount"] = float(amount)
					goodsTemplate["storageLocation"] = lgort
					goodsTemplate["totalWoQty"] = totalQuantity
					goodsTemplate["units"] = units
					goodsTemplate["bomAmount"] = float(checkBomQuantities(bomMat, materialCode, batchNumber))
					goodsTemplate["overrideAmount"] = 0.0
					goodsTemplate["ttQuantity"] = outfeed
					
					key = materialCode+batchNumber+goodsTemplate["materialOrigin"]
					
					if key in dictGoodsMovement:
						dictGoodsMovement[key]["mesAmount"] += float(amount)
						
						if materialCode+batchNumber not in currentBatchGM:
							dictGoodsMovement[key]["ttQuantity"] += outfeed
						
					else:		
						goodsCounter += 1
						dictGoodsMovement[key] = goodsTemplate
						
					currentBatchGM.add(materialCode+batchNumber)
					currentMaterials.add(materialCode)
		#else:
			# Add GM according to backflush 
		for mat in bomMat:
			matNumber = ""
			if mat["materialNumber"]:
				matNumber = mat["materialNumber"]
			
			
			batchNumber = "" 
			if mat["batchNumber"]:
				batchNumber = mat["batchNumber"]
			
			if matNumber in currentMaterials:
				continue
			
			itemCat = mat["itemCategory"]
			backFlushFlag = mat["backflushFlag"]
			lgort = mat["consumMaterialStorage"]
			units = mat["confirmationUnits"]
			if not units:
				units = mat["measureUnit"]
			quantity = mat["requiredQuantity"]
			# = mat["measureUnit"]
			
			if backflush == "NoConsumption":
				matOrigin = "NoConsumption"
			else:
				matOrigin = "FromBOM"
			goodsTemplate = {}
			
			# Check for stock item and backflush flag
			if itemCat == "L" and backFlushFlag:	
				# only unique material with batch and  origin		
				key = matNumber+batchNumber+matOrigin
				
				
				if key not in dictGoodsMovement:				
					goodsTemplate["batchNumber"] = batchNumber
					goodsTemplate["overrideAmount"] = 0.0
					goodsTemplate["confirmationMode"] = confirmationMode
					goodsTemplate["goodsMovementSeqNum"] = goodsCounter
					goodsTemplate["materialNumber"] = matNumber				
					
					if backflush == "NoConsumption":
						goodsTemplate["materialOrigin"] = matOrigin
						goodsTemplate["mesAmount"] = 0.0
					else:
						goodsTemplate["materialOrigin"] = matOrigin
						if totalQuantity > 0:
							goodsTemplate["mesAmount"] = (outfeed / totalQuantity) * float(quantity)
						else:
							goodsTemplate["mesAmount"] = 0.0
					goodsTemplate["storageLocation"] = lgort
					goodsTemplate["totalWoQty"] = totalQuantity
					goodsTemplate["units"] = units
					goodsTemplate["bomAmount"] = float(quantity)
					goodsTemplate["ttQuantity"] = outfeed
					
					goodsCounter+=1
					dictGoodsMovement[key] = goodsTemplate
				else:
					dictGoodsMovement[key]["bomAmount"] = float(quantity)
					dictGoodsMovement[key]["ttQuantity"] += outfeed			
				
		
		date = system.date.format(system.date.now(), "yyyy-MM-dd")
		rejectNumber = 0
		if rejects:
			rejectsJson = system.util.jsonDecode(rejects)
			# Check for multiple rejects
			if isinstance(rejectsJson, dict):
				ttRejectCodes.update(rejectsJson)
				for rejectCode in rejectsJson:
					rejectNumber = rejectsJson[rejectCode]
					ttReject += rejectsJson[rejectCode]
					ttRejectCode = rejectCode

			else:
				for code in rejectsJson:
					ttRejectCode = code
				ttReject += 1
		ttYield += outfeed
	
	
	
	
	
	
	goodsMovementHeaders = ["batchNumber","bomAmount","confirmationMode","goodsMovementSeqNum", "materialNumber",
							"materialOrigin","mesAmount","storageLocation","totalWoQty","units","ttQuantity","overrideAmount"]
	goodsMovementData = []			
	
	#print dictGoodsMovement
	#controlKey = "PP99"
	
	if controlKey == "PP99" and not producedInserted:
		storage = ""
		units = ""
		material = ""
		quantity = 0.0  
		if len(workorderDs)>0:
			storage = workorderDs[0]["storage"]
			units = workorderDs[0]["confirmationISOUnits"]
			material = workorderDs[0]["material"]
			quantity = float(workorderDs[0]["quantity"])
		goodsMovementData.append(["",quantity,confirmationMode, goodsCounter,material,"Produced", float(ttYield), storage, quantity, units, float(ttYield), 0.0])
		goodsCounter+=1
	from math import ceil

	for gm in dictGoodsMovement:
		goodMovementRow = dictGoodsMovement[gm]
		gmRow = []
		for header in goodsMovementHeaders:
			if header =="mesAmount":
				gmRow.append(round(goodMovementRow[header],3))
			else:
				gmRow.append(goodMovementRow[header])
		goodsMovementData.append(gmRow)		
		
		
	
	ttYield = ttYield - ttReject
	
	
	
	gmDs = system.dataset.toDataSet(goodsMovementHeaders,goodsMovementData)
	
	return gmDs, goodsCounter,ttYield,ttReject,ttRejectCode,totalQuantity,ttDuration,outUnits, system.util.jsonEncode(ttRejectCodes)
		