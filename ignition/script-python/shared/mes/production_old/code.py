# ==============================================
# shared.mes.production
# Last update: 2019-02-22
# Modified by: Rok Zupan
# ==============================================


def beginProductionRun(workOrder, materialName, equipmentPath):
	"""
	Start oee production run
	Begin an OEE operation for the specified work order. 
	The operations objects must have previously been created prior to calling this function.
	
	Parameters
	----------
	workOrder : str
		workOrder on which we want to record the batch
	materialNumber : str
		material SAP number
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	result : bool
		Returns True / False
	"""

	result = system.mes.oee.beginOEERun(workOrder, materialName, equipmentPath)

	return result
	
	
	
	
def endProductionRun(equipmentPath):
	"""
	End running production run
	End the production segment that is currently running at the specified equipment. 
	This function is only used if a single operation is running at the specified equipment. 
	If multiple operations are running at the specified equipment, use the indexCellProduct function.
	
	Parameters
	----------
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	result : bool
		Returns True / False
	"""

	result = system.mes.oee.endOEEProduction(equipmentPath)

	return result
	
	
	
	
def abortLastProductionRun(equipmentPath):
	"""
	Abort the changeover or production segment that is currently running at the specified equipment. 
	If multiple operations are running at the specified equipment, the last one started will be aborted. 
	
	Parameters
	----------
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	result : bool
		Returns True / False
	"""

	result = system.mes.oee.abortRun(equipmentPath)
	
	return result
	 



def endAllProductionRuns(equipmentPath):
	"""
	Ending all production runs.
	In case multiple runs are ongoing, we loop and end them
	
	Parameters
	----------
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	None
	"""

	result = system.mes.oee.getOEEAllActiveSegments(equipmentPath) # Returns MES object links for all of the active Response Segment objects that are currently running at the specified equipment.
	
	
	for run in result:
		obj = run.getMESObject()
		system.mes.endSegment(obj)
	


	
def abortAllProductionRuns(equipmentPath):
	"""
	Aborting all production runs.
	In case multiple runs are ongoing, we loop and abort them
	
	Parameters
	----------
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	None
	"""

	result = system.mes.oee.getOEEAllActiveSegments(equipmentPath) # Returns MES object links for all of the active Response Segment objects that are currently running at the specified equipment.
	
	
	for run in result:
		obj = run.getMESObject()
		system.mes.abortSegment(obj)
			


	
def endProductionChangeover(equipmentPath):
	"""
	End production changeover
	End the changeover segment that is currently running at the specified equipment. 
	After the changeover segment is ended, the production segment will begin.		
	
	Parameters
	----------
	equipmentPath : str
		equipment path based on MES production tree
		
	Returns
	-------
	result : bool
		True / False
	"""

	result = system.mes.oee.endOEEChangeover(equipmentPath)
	
	
	return result
	
	
	
	
# Only cells and cell groups at the first level under the line can utilize indexing. 
# All members of a cell group will have the same product code and operation UUID. 
# The indexing feature is not available within a cell (i.e. a cell can only have one product code running at a time) or within a cell group.
def indexProduct(equipmentPath, skipChangeover):
	result = system.mes.oee.indexCellProduct(equipmentPath, skipChangeover)
	return result



	
# when production run is ongoing, we can query active segment
def getActiveSegment(equipmentPath):
	result = system.mes.oee.getOEEActiveSegment(equipmentPath)
	return result




# get work order details
def getWorkOrder(workOrderName):
	result = system.mes.workorder.getMESWorkOrder(workOrderName)
	return result



		
def emptyCurrentOperation():
	data = {
		"beginDateTime": "",
		"workOrder": "",
		"materialName": "",
		"materialDesc": "",
		"stepDescription": "",
		"actualQuantity": "",
		"remainingQuantity": "",
		"woQuantity": "",
		"dueDate": "",
		"isClosed": ""
	}
	
	return data




#================================================================
# get current operations of running segments.
# this function is taking all production line runs and put them in json
# combined with getWorkOrder function
#================================================================		
def getCurrentOperation(equipmentPath, workCenter):
	results = system.mes.getCurrentOperations(equipmentPath)
	
	data = []
	
	if len(results) > 0:
		for result in results:
			beginDateTime = result.getPropertyValue("BeginDateTime")
			wo = result.getWorkOrderLink().getMESObject()
			runningWO = wo.getName()
		
			woData = getWorkOrder(runningWO)
			
			fields = ["name","quantity","units","material","materialDescription","type","basicStartDate","storage","description","basicStartDate","basicFinishDate","toLotSize"]
			workOrderData = shared.mes.workorder.getWorkOrders(fields = fields, workOrderName = runningWO)
			
			dataset = shared.mes.workorder.getSAPProductionStepDetails(workOrderName=runningWO, workCenter=workCenter[:4], fields=["stepDescription"])
			if dataset.rowCount > 0:
				stepDesc = dataset[0]["stepDescription"] #returned result is only 1
			else:
				stepDesc = "[\"\"]"
				
			stepDesc = eval(stepDesc)
			units = workOrderData[0]["units"]
			
			data.append({
					"beginDateTime": beginDateTime,
					"workOrder": runningWO,
					"materialName": woData.getMaterialRefName(),
					"materialDesc": workOrderData[0]["materialDescription"],
					"stepDescription": stepDesc,
					"actualQuantity": woData.getActualQuantity(),
					"remainingQuantity": woData.getRemainingQuantity(),
					"woQuantity": woData.getWorkOrderQuantity(),
					"units": units,
					"dueDate": woData.getDueDate(),
					"isClosed": woData.isClosed()
					})
	else:
		data = []
		data.append(emptyCurrentOperation())
						
	return system.util.jsonEncode(data)
	


	
#================================================================
# translate production runs from function getCurrentOperation to dataset
# used to build it for table drop
#================================================================
def getCurrentOperationAsDataset(equipmentPath, workCenter): # ignitionTagPath is tags path
	results = system.util.jsonDecode(getCurrentOperation(equipmentPath, workCenter))
	
	headers = [
			"beginDateTime",
			"workOrder",
			"materialName",
			"materialDesc",
			"stepDescription",
			"actualQuantity",
			"remainingQuantity",
			"woQuantity",
			"dueDate",
			"isClosed"
	]

	data = []
	for result in results:
		data.append([
			result["beginDateTime"],
			result["workOrder"],
			result["materialName"],
			result["materialDesc"],
			result["stepDescription"],
			result["actualQuantity"],
			result["remainingQuantity"],
			result["woQuantity"],
			result["dueDate"],
			result["isClosed"]
		])

	dataDS = system.dataset.toDataSet(headers, data)
	
	return dataDS
	



#================================================================
# function to get single step from all production runs based on work order
#================================================================		
def getCurrentSubOperation(equipmentPath, workCenter, workOrder):
	results = system.util.jsonDecode(getCurrentOperation(equipmentPath, workCenter))
	
	if workOrder <> "":
		for result in results:
			if result["workOrder"] == workOrder: 
				data = {
					"beginDateTime": result["beginDateTime"],
					"workOrder": result["workOrder"],
					"materialName": result["materialName"],
					"materialDesc": result["materialDesc"],
					"stepDescription": result["stepDescription"],
					"actualQuantity": result["actualQuantity"],
					"remainingQuantity": result["remainingQuantity"],
					"woQuantity": result["woQuantity"],
					"units": result["units"],
					"dueDate": result["dueDate"],
					"isClosed": result["isClosed"]
					}
				break
			else:
				data = emptyCurrentOperation()
	else:
		data = emptyCurrentOperation()
					
	return system.util.jsonEncode(data)


			
	
#=============================================================
# general assigning material to equipment
#=============================================================
def configMaterialOperation(materialName, equipmentPath, config):
    print 'Configuring %s for %s with parameters %s' % (materialName, equipmentPath, str(config))
    retVal = ''
    matLink = system.mes.getMESObjectLinkByName('MaterialDef', materialName)
    operList = system.mes.oee.createMaterialProcessSegment(matLink, equipmentPath)
    for opSeg in operList:
        if opSeg.getMESObjectTypeName() == 'OperationsSegment':
            matProp = opSeg.getComplexProperty('Material', 'Material Out')
            if matProp:
                matProp.setRate(config['Schedule Rate'])
                matProp.setUnits(config['Units'])
                matProp.setRatePeriod(config['Rate Period'])
                print "Schedule Rate set to %i %s per %s" % (matProp.getRate(), matProp.getUnits(), matProp.getRatePeriod())
     
            count = opSeg.getComplexPropertyCount('ProductionSettings')
            # There is a Production setting property for the line and each lineCell. This script will set
            # the standard rate to be the same for all cells as well as the line, but you could filter for the line or a specific cell
            for complexPropNum in range(count):
                productionSettings = opSeg.getComplexProperty('ProductionSettings', complexPropNum)
                productionSettings.setOEERate(config['Standard Rate'])
                productionSettings.setOutfeedUnits(config['Units'])
                if matProp:
                    print "OEE Rate set to %i %s per %s on %s" % (
                        productionSettings.getOEERate(), matProp.getUnits(), matProp.getRatePeriod(),
                        productionSettings.getEquipmentRef().getMESObject().getName())
                opSeg.setPropertyValue('ProductionSettings', productionSettings)
            retVal = "%s enabled on %s" % (materialName, equipmentPath)
    system.mes.saveMESObjects(operList)
    return retVal




#=============================================================
# assign material to equipment
# before starting the production run, we assign material parameters
#=============================================================
def assignMaterialToEquipment(equipmentPath, materialName):
	config = {'Standard Rate':10, 'Schedule Rate':10, 'Rate Period':'Hour', 'Units':'PCE'}
	result = configMaterialOperation(materialName, equipmentPath, config)	
	return result





#=============================================================
# get product dimensions from description
# TEMPORARY IN TESTING: Get material specification from PO
# with this we can check dimensions of the wheel
#=============================================================
def getMaterialDimensionFromDescription(description):
	import re
	
	re1='.*?'	# Non-greedy match on filler
	re2='\\d+'	# Uninteresting: int
	re3='.*?'	# Non-greedy match on filler
	re4='(\\d+)'	# Integer Number 1
	re5='.*?'	# Non-greedy match on filler
	re6='(\\d+)'	# Integer Number 2
	re7='.*?'	# Non-greedy match on filler
	re8='(\\d+)'	# Integer Number 3
	
	rg = re.compile(re1+re2+re3+re4+re5+re6+re7+re8,re.IGNORECASE|re.DOTALL)
	m = rg.search(description)
	
	result = {}
	if m:
		result["diameter"] = m.group(1)
		result["thickness"] = m.group(2)
		result["bore"] = m.group(3)
	
	return result




#=============================================================	
# get production line from equipment path
#=============================================================
def recursiveSearchForLine(eqPath):
	mesObject = system.mes.getProductionItemByEquipmentPath(eqPath)
	if str(mesObject.productionType) == "LINE":
		return mesObject.getPath()
	return recursiveSearchForLine(mesObject.getParentItemPath())


	

#=============================================================	
# get production line from equipment path
# usefull when we need to extract cell under the line
#=============================================================
def searchForPrimaryCellInLine(eqPath, indexCellName):
#equipmentPath should be Line path
	mesObject = system.mes.loadMESObjectByEquipmentPath(eqPath)
	childs = mesObject.getChildCollection()
	
	for child in childs.getList():
		name = child.getMESObject().getName()
		equipmentPath = child.getMESObject().getPropertyValue("EquipmentPath")
		cellOrder = child.getMESObject().getPropertyValue("EquipmentCellOrder")
		
		if name == indexCellName:
			return equipmentPath


		

#=============================================================	
# get production live data based od PO
# get live data analysis on request. Used to update PCE outfeed and rejects in realtime.
#=============================================================
def getLiveDataFromStation(eqPath, workOrder):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("getLiveDataFromStation")
	datapoints = [
				"Outfeed-prod_outfeed",
				"Infeed-prod_infeed",
				"Reject-prod_rejects",
				"Work Order",
				"Equipment Path"
				]
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.addParameter('workOrder')
	analysis_setting.setFilterExpression("Equipment Path = @path AND Work Order = @workOrder")
	analysis_setting.setGroupBy("Equipment Path")
		 
	start_date = system.date.addDays(system.date.now(), -14)
	end_date = system.date.now()
	params = {'path':eqPath, 'workOrder': workOrder}
	
	data = system.mes.analysis.executeAnalysis(start_date, end_date, analysis_setting, params).getDataset()
	
	liveData = {}
	if data.rowCount>0:
		for row in range(data.rowCount):
			liveData["outfeed"] = data.getValueAt(row, "Outfeed-prod_outfeed")
			liveData["infeed"] = data.getValueAt(row, "Infeed-prod_infeed")
			liveData["rejects"] = data.getValueAt(row, "Reject-prod_rejects")
			#liveData["wo"] = data.getValueAt(row, "Work Order")
			#liveData["eqPath"] = data.getValueAt(row, "Equipment Path")
	else:
		liveData["outfeed"] = 0
		liveData["infeed"] = 0
		liveData["rejects"] = 0

	return liveData
		



def storeBackToBasicsLog(content, user, workcenter, workOrder):
	"""
	Function to store back to basics confirmation to log
	
	Parameters
	----------
	content : json
		can be whatever data
	user : string
	workcenter: string
		SAP Workcenter
	workOrder: strain
		SAP Workorder
		
	Returns
	-------
	result : int
		Returns affected rows. 1 = insert successful
	"""

	sqlQuery = """
		INSERT INTO backToBasicsLog 
			(content, timestamp, user, workcenter, workOrder) 
		VALUES 
			(?, ?, ?, ?, ?)
	"""
	
	sqlResult = system.db.runPrepUpdate(sqlQuery, [content, system.date.now(), user, workcenter, workOrder], "factory_production")
	
	return sqlResult