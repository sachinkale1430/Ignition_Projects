def storeDataToMes(ignitionTagPath, workorder, quantityProduced, unitSap, productionStartDate, productionEndDate, rejects=None, productionId=None, additionalJsonData=None,operator=None):
	import traceback
	from java.util import Date
	returnMessage = {"type":"info", "message":"starting message"}

    # block to intercept all errors to give gracefull error
	try:
    	# Start initial checking and sanity check
    	
		#Data type checking
		if not isinstance(ignitionTagPath, basestring):
			return getReturnMessage("error", "ignitionTagPath needs to be String datatype")
		
		if ignitionTagPath.startswith("["):
			return getReturnMessage("error", "ignitionTagPath can not start with [")	
			
		if not isinstance(workorder,basestring):
			return getReturnMessage("error", "workorder needs to be String datatype")
				
		if not isinstance(quantityProduced, float):
			return getReturnMessage("error", "quantityProduced needs to be float datatype")
			
		if not isinstance(productionStartDate, Date):
			return getReturnMessage("error", "productionStartDate needs to be java.util.Date datatype")
			
		if not isinstance(productionEndDate, Date):
			return getReturnMessage("error", "productionEndDate needs to be java.util.Date datatype")
    	
		if rejects and not isinstance(rejects, dict):
			return getReturnMessage("error", "rejects needs to be dict datatype")
			
		if productionId and not isinstance(productionId, basestring):
			return getReturnMessage("error", "productionId needs to be String datatype")
    		
		if additionalJsonData and not isinstance(additionalJsonData, dict):
			return getReturnMessage("error", "additionalJsonData needs to be dict datatype")
		
		# ignitionTagPath sanity checks	
		if system.tag.exists(ignitionTagPath+"/mes"):
			
			if not system.tag.exists(ignitionTagPath+"/MESProcessTags"):
				return getReturnMessage("error", "MESPRocessTags folder missing in: "+ ignitionTagPath )

			
			tagValues = system.tag.readAll([ignitionTagPath+"/mes/param_sapWorkCenter",
											ignitionTagPath+"/mes/prod_sapStepid",
											ignitionTagPath+"/mes/prod_productCode"])
			workcenter = tagValues[0].value
			stepId = tagValues[1].value
			materialNumber = tagValues[2].value
			if not workcenter or not stepId or stepId == "0":
				return getReturnMessage("error", "param_sapWorkCenter or param_sapStepid is not filled in MES udt: "+ ignitionTagPath +"/mes")	
				
		else:
			return getReturnMessage("error", "ignitionTagPath: "+ ignitionTagPath +"/mes tag path does not exists.")
		# quantity sanity checks
		if quantityProduced <= 0 :
			return getReturnMessage("error", "quantityProduced has to be greater than 0")
		# unit sanity checks
		if len(unitSap)>3 or len(unitSap)==0:
			return getReturnMessage("error", "unitSap can only have 3 characters or less.")
		# unitSap sanity checks
		if len(unitSap)==0:
			return getReturnMessage("error", "unitSap is a mandatoryfield.")
		# sanity checks for rejects
		if rejects:
			if not all(type(rejects[reject])==int or type(rejects[reject])==float for reject in rejects):
				return getReturnMessage("error", "Rejects dict has non number value. "+str(rejects))
			# Sum all rejects
			countRejects = sum(rejects[reject] for reject in rejects)
			# Check if there is more rejects than produced quantity
			if countRejects > quantityProduced:
				return getReturnMessage("error", "There are more rejects than produced quantity ")
		if not system.date.isBefore(productionStartDate, productionEndDate):
			return getReturnMessage("error", "The producionStartDate needs to be before productionEndDate")
		# Sanity checks for workorder
		try:
			mesWorkorder =  system.mes.workorder.getMESWorkOrder(workorder)
		except:
			return getReturnMessage("error", "Workorder does not exist in MES or its closed in SAP")
		# Sanity checks for productionId
		if productionId and len(productionId)>20:
			return getReturnMessage("error", "ProductionId needs to have 20 characters or lower.")
		
		if productionId and checkDuplicateInDb(workorder, stepId, productionId):
			return getReturnMessage("error", "ProductionId for this stepId and workorder is already taken. Please use another.")
		
		#### MAIN FUNCTION
		# Get factory name from gateway codification
		factoryName = system.tag.read("[System]Gateway/SystemName").value[4:7]
		autoGen = False
		if not productionId:
			autoGen = True
			productionId = autoGenerateId(factoryName)
		# Get plant from SAP workorder 
		sapWorkorder = shared.mes.workorder.getWorkOrders(workorder,fields=["plant"])
		if len(sapWorkorder) == 0:
			return getReturnMessage("error", "Workorder does not exist in MES or its closed in SAP")
		plantCode = sapWorkorder[0][0]
		
		facId = checkAddFactory(plantCode)
		checkAddWorkCenter(workcenter, facId)
		
		# Get json from MESProcessTags
		jsonPieceData = shared.json.export.getTagsAsJson("[default]" + ignitionTagPath + "/MESProcessTags",attr={"value":"Value","quality":"Quality", "type":"TagType"})
		# Appends additional jsonData
		if additionalJsonData:
			jsonPieceData.update(additionalJsonData)
		# Get operator
		if not operator:
			try:
				operator = system.security.getUsername()
			except:
				operator = "MESGateway"
		
		# Add workorder 
		resultAddWorkOrder = shared.json.store.add_work_order(plantCode, workorder)
		# Store data to step_results
		
		if not rejects:
			rejects = {}
		
		resultAddProduction = shared.json.store.add_production_data(workcenter, workorder, stepId, jsonPieceData, productionId, 
															  productionStartDate, productionEndDate, operator, materialNumber,
															  rejects=rejects, unit=unitSap, piece_quantity=quantityProduced)
		
		# Check if autoUpdateRunData exists			
		if system.tag.exists(ignitionTagPath+"/mes/broker/param_autoUpdateRunData"):
			# make the Json and write it to prod_runData
			sendToBroker = system.tag.read(ignitionTagPath+"/mes/broker/param_autoUpdateRunData").value
			if sendToBroker:
				shared.broker.dataPreparation.prepareJsonForMqtt(workcenter, workorder, stepId, jsonPieceData, productionId, 
																 productionStartDate, productionEndDate, operator, materialNumber, 
																 ignitionTagPath=ignitionTagPath, piece_quantity=quantityProduced, unit=unitSap, rejects=rejects)
				
		else:
			return getReturnMessage("error", "/mes/broker/param_autoUpdateRunData does not exist MES UDT please update it.")
		if autoGen:
			return getReturnMessage("info", "Data stored successfully", additionalData={"auto-gen-id":productionId})
		else:
			return getReturnMessage("info", "Data stored successfully")
	except:
		return getReturnMessage("error", "Unexpected error traceback: "+traceback.format_exc())
	return returnMessage

def checkAddFactory(factoryCode):
	sql = "SELECT id from factory where code = ?"
	
	returnDs = system.db.runPrepQuery(sql, args=[factoryCode], database="factory_production")
	if len(returnDs) == 0:
		insertSql = "INSERT INTO factory (code) VALUES (?)"
		id = system.db.runPrepUpdate(insertSql, args=[factoryCode], database="factory_production",getKey=True)
	else:
		id = returnDs[0][0]
	return id
		
def checkAddWorkCenter(workcenter, factory):
	sql = "SELECT id from workcenter where code = ? and factory_id = ?"
	
	returnDs = system.db.runPrepQuery(sql, args=[workcenter, factory], database="factory_production")
	if len(returnDs) == 0:
		insertSql = "INSERT INTO workcenter (code,factory_id) VALUES (?,?)"
		system.db.runPrepUpdate(insertSql, args=[workcenter, factory], database="factory_production")

def autoGenerateId(factory):
	import string
	#get last batch from step_result
	lastBatch = getLastBatch(factory)
	# first time using autoGen
	if not lastBatch:
		incrementPart = factory+"1111111"
		
		return incrementPart
	else:
		incrementPart = lastBatch[3:]
		
	# Make an alphabet set
	alphabet = list(string.digits)+list(string.ascii_uppercase)
	# throw 0 out 
	alphabet.remove("0")
	#get maxLength of alphabet
	maxLength = len(alphabet)
	incrementNext = True
	tmpBatch = ""
	# Go over the set and increment what is necessary
	for t in incrementPart[::-1]:
		if incrementNext:
			nextChar = alphabet.index(t) + 1
			print nextChar
			if nextChar >= maxLength:
				nextChar = 0
				incrementNext = True
			else:
				incrementNext = False
			
			tmpBatch += alphabet[nextChar]
		else:
			tmpBatch += t
		
	incrementPart = tmpBatch[::-1]
	
	return factory + incrementPart

def getLastBatch(factory):
	sql = 'SELECT max(id) FROM step_result WHERE id REGEXP "'+factory+'[A-Z1-9]{7}"'
	lastBatch = system.db.runScalarQuery(sql, database="factory_production")
	return lastBatch
def checkDuplicateInDb(workorder, stepId, productionId):
	sql ="""
	SELECT 1
	FROM step s 
	 join step_result sr on sr.step_id = s.id and sr.workorder_id=s.workorder_id
	 join workorder w on w.id = s.workorder_id
	 join workcenter wc on wc.id=s.workcenter_id
	where w.number = ? and s.id = ? and sr.id = ?
	"""
	
	check_piece_number = system.db.runPrepQuery(query=sql, args=[workorder, stepId, productionId], database="factory_production")
		
	if len(check_piece_number) > 0:
		return True
	return False
		
def logMessageOnGateway(message, logger, level):
	payload = {"message":message, "logger":logger, "level":level}
	system.util.sendMessage("pfce_sga_interfaces", "clientLogger", payload)

def getReturnMessage(returnType, message,additionalData=None):
	currentMessage = {"type":"info", "message":"starting message"}
	currentMessage["type"] = returnType
	currentMessage["message"] = message
	if additionalData:
		currentMessage.update(additionalData)
	logMessageOnGateway(system.util.jsonEncode(currentMessage), "MESDataStore", returnType)
	return currentMessage
	