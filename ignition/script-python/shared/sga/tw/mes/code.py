def storeToMes(ignitionTagPath):

	tagsToRead = [
		ignitionTagPath+"/mes/prod_workOrder",
		ignitionTagPath+"/press/analysis/scopeProductionRun/prod_goodWheels",
		ignitionTagPath+"/press/analysis/scopeProductionRun/prod_rejectedWheels",
		ignitionTagPath+"/press/analysis/scopeProductionRun/param_startDate",
		ignitionTagPath+"/press/analysis/scopeProductionRun/param_endDate",
		ignitionTagPath+"/MESProcessTags/prod_operator",
		ignitionTagPath+"/mes/param_baseFolder",
		ignitionTagPath+"/mes/sap/param_confirmationTestMode",
		ignitionTagPath+"/mes/sap/param_confirmationEnabled"
		
	]
	
	tagObj = system.tag.readAll(tagsToRead)
	
	workorder = tagObj[0].value
	goodWheels = float(tagObj[1].value) 
	rejectedWheels = float(tagObj[2].value) 
	productionStartDate = tagObj[3].value 
	productionEndDate = tagObj[4].value 
	operator = tagObj[5].value
	baseFolder = tagObj[6].value
	confirmationTestMode = tagObj[7].value
	confirmationEnabled = tagObj[8].value
	unitSap = "PCE"
	additionalJsonData = shared.sga.tw.streamData.buildRunStreamData(ignitionTagPath)
	
	quantityProduced = rejectedWheels + goodWheels
	
	buildMaterialConsumptionTag(ignitionTagPath, workorder, quantityProduced)
	
	#(STEP RESULTS)
	
	rejects = shared.sga.tw.rejection.formatTWRejectJson(ignitionTagPath)

	shared.mes.store.storeDataToMes(baseFolder, workorder, quantityProduced, unitSap, productionStartDate, productionEndDate, operator=operator, rejects=rejects, productionId=None, additionalJsonData=additionalJsonData)
	
	batchMaterials = batchManagedMaterials(ignitionTagPath, quantityProduced,productionStartDate, productionEndDate)
	
	
	if confirmationEnabled:
		bapiTemplates = prepareTemplates(ignitionTagPath, batchMaterials)
		# Prepare transaction
		uuid = shared.sap.bapi.prepareTransaction(confirmationTestMode)
		
		for bapi in bapiTemplates:
			shared.sap.bapi.addOperation(uuid, bapi, confirmationTestMode)
		
		log = shared.sap.bapi.tryTransaction(uuid, confirmationTestMode,bapiTemplates)
		# TO DO LOGS
	# 

def batchManagedMaterials(ignitionTagPath, quantityProduced,productionStartDate, productionEndDate):
	ds = system.tag.read(ignitionTagPath + "/press/batchManagement/prod_batchAssignmentHistory").value

	pyDs = system.dataset.toPyDataSet(ds)
	
	json = {}
	materialList = []
	batchList = []
	
	minutesBetween = system.date.minutesBetween(productionStartDate, productionEndDate)
	
	avgRate = quantityProduced / minutesBetween if minutesBetween > 0  else 0
	
	#print avgRate
	rowCount = ds.getRowCount()
	#print system.dataset.getColumnHeaders(ds)
	tmpData = {}
	for x in pyDs:
		matNumber = x["Material"]
		if x["Material"] in json:
			json[matNumber].append(x)
		else:
			json[matNumber] = [x]
		
	for key in json:
		#print key
		materialArray = json[key]
	
		if len(materialArray) == 1:
			#print materialArray[0]
			material = materialArray[0]
			startDate = system.date.parse(material["dateFormat"])
			endDate = productionEndDate
			
			minutesBetween = system.date.minutesBetween(startDate,endDate)
			avgQuantityProduced = round(minutesBetween * avgRate)
			
			prepArray = [material["Material"],material["Batch name"],startDate,productionEndDate,avgQuantityProduced]
			if material["Material"] in tmpData:
				tmpData[material["Material"]].append(prepArray)
			else:
				tmpData[material["Material"]] = [prepArray]
			#tmpData.append([material["Material"],material["Batch name"],startDate,productionEndDate,avgQuantityProduced])
		else:
			for x in zip(materialArray,materialArray[1:] + [None]):
				#print x
				if x[1]:
					
					material = x[0]
					material1 = x[1]

					startDate = system.date.parse(material["dateFormat"])
					endDate = system.date.parse(material1["dateFormat"])
					minutesBetween = system.date.minutesBetween(startDate,endDate)
					avgQuantityProduced = round(minutesBetween * avgRate)
					prepArray = [material["Material"],material["Batch name"],startDate,endDate,avgQuantityProduced]
					if material["Material"] in tmpData:
						tmpData[material["Material"]].append(prepArray)
					else:
						tmpData[material["Material"]] = [prepArray]
					
					#tmpData.append([material["Material"],material["Batch name"],startDate,endDate,avgQuantityProduced])
				else:
					material = x[0]
					
					startDate = system.date.parse(material["dateFormat"])
					endDate = productionEndDate
					minutesBetween = system.date.minutesBetween(startDate,endDate)
					avgQuantityProduced = round(minutesBetween * avgRate)
					prepArray = [material["Material"],material["Batch name"],startDate,productionEndDate,avgQuantityProduced]
					if material["Material"] in tmpData:
						tmpData[material["Material"]].append(prepArray)
					else:
						tmpData[material["Material"]] = [prepArray]
					
					#tmpData.append([material["Material"],material["Batch name"],startDate,productionEndDate,avgQuantityProduced])
	
	#print system.util.jsonEncode(tmpData,4)
	return tmpData
		
	
def prepareTemplates(ignitionTagPath, batchMaterials):
	import math
	tagsToRead = [
		ignitionTagPath+"/mes/prod_workOrder",
		ignitionTagPath+"/press/analysis/scopeProductionRun/prod_goodWheels",
		ignitionTagPath+"/press/analysis/scopeProductionRun/prod_rejectedWheels",
		ignitionTagPath+"/press/analysis/scopeProductionRun/param_startDate",
		ignitionTagPath+"/press/analysis/scopeProductionRun/param_endDate",
		ignitionTagPath+"/MESProcessTags/prod_operator",
		ignitionTagPath+"/mes/param_baseFolder",
		ignitionTagPath+"/mes/param_sapWorkCenter",
		ignitionTagPath+"/mes/prod_sapStepId",
		ignitionTagPath+"/press/rejectionDeclaration/param_defaultRejectCode"
	]
	
	tagObj = system.tag.readAll(tagsToRead)
	
	mainWorkorderName = tagObj[0].value
	goodWheels = float(tagObj[1].value) 
	
	rejectedWheels = float(tagObj[2].value)
	rejectCodeJson = shared.sga.tw.rejection.formatTWRejectJson(ignitionTagPath)
	 
	productionStartDate = tagObj[3].value 
	productionEndDate = tagObj[4].value 
	operator = tagObj[5].value
	baseFolder = tagObj[6].value
	mainWorkcenter = tagObj[7].value
	mainStepId = tagObj[8].value
	param_defaultRejectCode = tagObj[9].value
	
	unitSap = "PCE"
	
	quantityProduced = rejectedWheels + goodWheels
	
	machineDuration = system.date.minutesBetween(productionStartDate,productionEndDate)
	
	workorderDs = shared.mes.workorder.getWorkOrders(workOrderName=mainWorkorderName)
	mainMaterialWorkorderDs = shared.mes.workorder.getStepMaterials( workOrderName=mainWorkorderName,workCenter=mainWorkcenter)
	
	date = system.date.format(system.tag.read("[System]Gateway/CurrentDateTime").value, "yyyy-MM-dd")
	
	if len(workorderDs) > 0:
		from copy import deepcopy
		template = deepcopy(shared.sap.templates.BAPI_PRODORDCONF_CREATE_TT)
		templateItemTT = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"])
		templateItemGM = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"])
		templateItemGMlink = deepcopy(template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"])
	
		tmpItem = []
		tmpGoodsMovement = []
		tmpLink = []
		
		
		workorder = workorderDs[0]
		
		#If you have child orders for MIX (CAI)
		mixChildOrders =  workorder["childOrders"].split(",")
		
		#If there are NO child order for mix (EIB, AMB...)
		#mixChildOrders = []
		
		totalQuantity = workorder["totalOrderQuantity"]
		#print totalQuantity
		percent = quantityProduced / float(totalQuantity)
		
		plant = workorder["plant"]
		
		gmCounter = 1
		refDoc = 1
		ttCounter = 1
		
		#Multiple reject code management		
		if len(rejectCodeJson) > 0:
			cnt = 1
			if param_defaultRejectCode in rejectCodeJson:
				
				mainTemplate = deepcopy(templateItemTT[0])
				mainTemplate["DEV_REASON"] = param_defaultRejectCode
				mainTemplate["SCRAP"] = float(rejectCodeJson[param_defaultRejectCode])
				mainTemplate["YIELD"] = goodWheels
				mainTemplate["WORK_CNTR"] = mainWorkcenter
				mainTemplate["OPERATION"] = mainStepId
				mainTemplate["PLANT"] = plant 
				mainTemplate["ORDERID"] = mainWorkorderName.zfill(12)
				
				mainTemplate["POSTG_DATE"] = date
				
				#Labor, machine and setup times
				mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
				mainTemplate["CONF_ACTIVITY1"] = "0"
				mainTemplate["CONF_ACTIVITY2"] = machineDuration
				mainTemplate["CONF_ACTIVITY3"] = "0"
				
				#if finalConfirmation:
				mainTemplate["FIN_CONF"] = "X"
				mainTemplate["CLEAR_RES"] = "X"
				cnt += 1
				tmpItem.append(mainTemplate)
				ttCounter += 1
			for code in rejectCodeJson:
				if code != param_defaultRejectCode:
					rejNum = rejectCodeJson[code]
					mainTemplate = deepcopy(templateItemTT[0])	
					if cnt == 1:
						mainTemplate["DEV_REASON"] = code
						mainTemplate["SCRAP"] = rejNum
						mainTemplate["YIELD"] = goodWheels
						mainTemplate["WORK_CNTR"] = mainWorkcenter
						mainTemplate["OPERATION"] = mainStepId
						mainTemplate["PLANT"] = plant 
						mainTemplate["ORDERID"] = mainWorkorderName.zfill(12)
						
						mainTemplate["POSTG_DATE"] = date
						
						#Labor, machine and setup times
						mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
						mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
						mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
						mainTemplate["CONF_ACTIVITY1"] = "0"
						mainTemplate["CONF_ACTIVITY2"] = machineDuration
						mainTemplate["CONF_ACTIVITY3"] = "0"
						
						#if finalConfirmation:
						mainTemplate["FIN_CONF"] = "X"
						mainTemplate["CLEAR_RES"] = "X"
					else:
						mainTemplate["DEV_REASON"] = code
						mainTemplate["SCRAP"] = rejNum
						mainTemplate["YIELD"] = 0
						mainTemplate["WORK_CNTR"] = mainWorkcenter
						mainTemplate["OPERATION"] = mainStepId
						mainTemplate["PLANT"] = plant 
						mainTemplate["ORDERID"] = mainWorkorderName.zfill(12)
						
						mainTemplate["POSTG_DATE"] = date
						
						#Labor, machine and setup times
						mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
						mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
						mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
						mainTemplate["CONF_ACTIVITY1"] = "0"
						mainTemplate["CONF_ACTIVITY2"] = "0"
						mainTemplate["CONF_ACTIVITY3"] = "0"
						
						#if finalConfirmation:
						mainTemplate["FIN_CONF"] = "X"
						mainTemplate["CLEAR_RES"] = "X"
					
					cnt += 1
					ttCounter += 1
					tmpItem.append(mainTemplate)
		else:
			mainTemplate = deepcopy(templateItemTT[0])	
			#mainTemplate["DEV_REASON"] = rejectsCodes
			#mainTemplate["SCRAP"] = rejects
			mainTemplate["YIELD"] = goodWheels
			mainTemplate["WORK_CNTR"] = mainWorkcenter
			mainTemplate["OPERATION"] = mainStepId
			mainTemplate["PLANT"] = plant 
			mainTemplate["ORDERID"] = mainWorkorderName.zfill(12)
			
			mainTemplate["POSTG_DATE"] = date
			
			#Labor, machine and setup times
			mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
			mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
			mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
			mainTemplate["CONF_ACTIVITY1"] = "0"
			mainTemplate["CONF_ACTIVITY2"] = machineDuration
			mainTemplate["CONF_ACTIVITY3"] = "0"
			
			#if finalConfirmation:
			mainTemplate["FIN_CONF"] = "X"
			mainTemplate["CLEAR_RES"] = "X"
			tmpItem.append(mainTemplate)
			
			ttCounter += 1
		# build goods movement
		
		# Good movement for main WO
		if len(mainMaterialWorkorderDs) > 0:
			for mainMat in mainMaterialWorkorderDs:
				
				#print list(mainMat)
				
				#print mat
				itemCat = mainMat["itemCategory"]
				backFlushFlag = mainMat["backflushFlag"]
				batchManaged = mainMat["managedByBatch"]
			
				isoUnits = mainMat["confirmationUnits"]
				reqQuantity = mainMat["requiredQuantity"]
				materialNumber = mainMat["materialNumber"]
				storageLocation = mainMat["consumMaterialStorage"]
			
				if itemCat == "L" and backFlushFlag:
					# Add batch managed materials if they are backflushed and inserted into code
					if materialNumber in batchMaterials:
						for batchmaterial in batchMaterials[materialNumber]:
							goodsTemplate = deepcopy(templateItemGM[0])
							linkTemplate = deepcopy(templateItemGMlink[0])
							#print batchmaterial
							matQuantity = batchmaterial[4]
							#if gm.overrideAmount > 0 :
							goodsTemplate["ENTRY_QNT"] ="%.3f" % math.ceil(matQuantity)
							goodsTemplate["ENTRY_UOM_ISO"] = isoUnits
							goodsTemplate["MATERIAL"] = materialNumber.zfill(18)
							goodsTemplate["BATCH"] = batchmaterial[1]
							
							goodsTemplate["MOVE_TYPE"] = 261
							goodsTemplate["ORDERID"] = mainWorkorderName.zfill(12)
							goodsTemplate["PLANT"] = plant
							goodsTemplate["STGE_LOC"] = storageLocation
							goodsTemplate["REF_DOC_IT"] = str(ttCounter).zfill(4)
							
							
							linkTemplate["INDEX_CONFIRM"] = 1
							linkTemplate["INDEX_GOODSMOV"] = gmCounter
							linkTemplate["INDEX_GM_DEPEND"] = 0
							
							#Add them to tmpArrays
							tmpLink.append(linkTemplate)
							tmpGoodsMovement.append(goodsTemplate)
							gmCounter += 1
					else:
						goodsTemplate = deepcopy(templateItemGM[0])
						linkTemplate = deepcopy(templateItemGMlink[0])
						matQuantity = float(reqQuantity) * percent
						#if gm.overrideAmount > 0 :
						goodsTemplate["ENTRY_QNT"] ="%.3f" % math.ceil(matQuantity)
						goodsTemplate["ENTRY_UOM_ISO"] = isoUnits
						goodsTemplate["MATERIAL"] = materialNumber.zfill(18)
						
						
						goodsTemplate["MOVE_TYPE"] = 261
						goodsTemplate["ORDERID"] = mainWorkorderName.zfill(12)
						goodsTemplate["PLANT"] = plant
						goodsTemplate["STGE_LOC"] = storageLocation
						goodsTemplate["REF_DOC_IT"] = str(ttCounter).zfill(4)
						
						
						linkTemplate["INDEX_CONFIRM"] = 1
						linkTemplate["INDEX_GOODSMOV"] = gmCounter
						linkTemplate["INDEX_GM_DEPEND"] = 0
						
						#Add them to tmpArrays
						tmpLink.append(linkTemplate)
						tmpGoodsMovement.append(goodsTemplate)
						
						gmCounter += 1
				
			
		
		tmpItem.append(mainTemplate)
		mainTTWoCounter = ttCounter
		ttCounter += 1
		
		#print percent
		# Processing for child collective order
		for x in mixChildOrders:
			mainTemplate = deepcopy(templateItemTT[0])	
			
			workOrderName = x.lstrip("0")
			
			workorderMixDs = shared.mes.workorder.getWorkOrders(workOrderName=workOrderName)
			
			if len(workorderMixDs) > 0:
				
				mixWorkorder = workorderMixDs[0]
				
				totalMixQuantity = mixWorkorder["totalOrderQuantity"]
				mixPoQnty = float(totalMixQuantity) * percent
				mainTemplate["YIELD"] = "%.3f" % mixPoQnty
				# TW always have mix on 0010
				mainTemplate["WORK_CNTR"] = "MIX"
				mainTemplate["OPERATION"] = "0010"
				mainTemplate["PLANT"] = plant 
				mainTemplate["ORDERID"] = workOrderName.zfill(12)
				
				mainTemplate["POSTG_DATE"] = date
				
				#Labor, machine and setup times
				mainTemplate["CONF_ACTI_UNIT1"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT2"] = "MIN"
				mainTemplate["CONF_ACTI_UNIT3"] = "MIN"
				mainTemplate["CONF_ACTIVITY1"] = "0"
				mainTemplate["CONF_ACTIVITY2"] = "0"
				mainTemplate["CONF_ACTIVITY3"] = "0"
				
				#if finalConfirmation:
				mainTemplate["FIN_CONF"] = "X"
				mainTemplate["CLEAR_RES"] = "X"
			
				
			
				
				#print workOrderName
				materials = shared.mes.workorder.getStepMaterials( workOrderName=workOrderName,workCenter="MIX")
				# Consumed materials
				for mat in materials:
					goodsTemplate = deepcopy(templateItemGM[0])
					linkTemplate = deepcopy(templateItemGMlink[0])
					#print mat
					itemCat = mat["itemCategory"]
					backFlushFlag = mat["backflushFlag"]
					batchManaged = mat["managedByBatch"]
				
					isoUnits = mat["confirmationUnits"]
					reqQuantity = mat["requiredQuantity"]
					materialNumber = mat["materialNumber"]
					storageLocation = mat["consumMaterialStorage"]
				
					if itemCat == "L" and backFlushFlag:
						
						
						matQuantity = float(reqQuantity) * percent
						#if gm.overrideAmount > 0 :
						goodsTemplate["ENTRY_QNT"] ="%.3f" % math.ceil(matQuantity)
						goodsTemplate["ENTRY_UOM_ISO"] = isoUnits
						goodsTemplate["MATERIAL"] = materialNumber.zfill(18)
						
						
						goodsTemplate["MOVE_TYPE"] = 261
						goodsTemplate["ORDERID"] = workOrderName.zfill(12)
						goodsTemplate["PLANT"] = plant
						goodsTemplate["STGE_LOC"] = storageLocation
						goodsTemplate["REF_DOC_IT"] = str(ttCounter).zfill(4)
						
						
						linkTemplate["INDEX_CONFIRM"] = ttCounter
						linkTemplate["INDEX_GOODSMOV"] = gmCounter
						linkTemplate["INDEX_GM_DEPEND"] = 0
						
						
						tmpLink.append(linkTemplate)
						tmpGoodsMovement.append(goodsTemplate)
						
						gmCounter += 1
				# Add produced material	
				
				goodsTemplate = deepcopy(templateItemGM[0])
				linkTemplate = deepcopy(templateItemGMlink[0])
				#workorderMixDs
				goodsTemplate["ENTRY_QNT"] ="%.3f" % mixPoQnty
				goodsTemplate["ENTRY_UOM_ISO"] = mixWorkorder["confirmationISOUnits"]
				goodsTemplate["MATERIAL"] = mixWorkorder["material"].zfill(18)
				
				
				goodsTemplate["MOVE_TYPE"] = 101
				goodsTemplate["MVT_IND"] = "F"
				goodsTemplate["ORDERID"] = workOrderName.zfill(12)
				goodsTemplate["PLANT"] = plant
				goodsTemplate["STGE_LOC"] = mixWorkorder["storage"]
				goodsTemplate["REF_DOC_IT"] = str(ttCounter).zfill(4)
				
				
				linkTemplate["INDEX_CONFIRM"] = ttCounter
				linkTemplate["INDEX_GOODSMOV"] = gmCounter
				linkTemplate["INDEX_GM_DEPEND"] = mainTTWoCounter
				
				gmCounter +=1
				# Consume produced mix material
						
				tmpLink.append(linkTemplate)
				tmpGoodsMovement.append(goodsTemplate)
				
				
				goodsTemplate = deepcopy(templateItemGM[0])
				linkTemplate = deepcopy(templateItemGMlink[0])
				
				goodsTemplate["ENTRY_QNT"] ="%.3f" % mixPoQnty
				goodsTemplate["ENTRY_UOM_ISO"] = mixWorkorder["confirmationISOUnits"]
				goodsTemplate["MATERIAL"] = mixWorkorder["material"].zfill(18)
				
				
				goodsTemplate["MOVE_TYPE"] = 261
				
				goodsTemplate["ORDERID"] = mainWorkorderName.zfill(12)
				goodsTemplate["PLANT"] = plant
				goodsTemplate["STGE_LOC"] = mixWorkorder["storage"]
				goodsTemplate["REF_DOC_IT"] = str(mainTTWoCounter).zfill(4)
				
				
				linkTemplate["INDEX_CONFIRM"] = mainTTWoCounter
				linkTemplate["INDEX_GOODSMOV"] = gmCounter
				linkTemplate["INDEX_GM_DEPEND"] = 0
				
				
				
						
				tmpLink.append(linkTemplate)
				tmpGoodsMovement.append(goodsTemplate)
				
				tmpItem.append(mainTemplate)
				
				gmCounter +=1
				ttCounter +=1
				
			
	template["parameterValues"]["input"]["inputRoot"]["TABLES"]["TIMETICKETS"]["item"] = tmpItem
	if tmpGoodsMovement:
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["GOODSMOVEMENTS"]["item"] = tmpGoodsMovement
		template["parameterValues"]["input"]["inputRoot"]["TABLES"]["LINK_CONF_GOODSMOV"]["item"] = tmpLink

	
	filledTemplates = []
	

	filledTemplates.append(template)
	filledTemplate = shared.sap.templates.BAPI_TRANSACTION_COMMIT
	filledTemplates.append(filledTemplate)
	
	
	return filledTemplates
	
def buildMaterialConsumptionTag(ignitionTagPath, workorder, outfeed):
	import math
	bomMat = shared.sap.bapi.getBomForWorkorder(ignitionTagPath, workorder)
	
	mesWorkorder = system.mes.workorder.getMESWorkOrder(workorder)
	totalQuantity = mesWorkorder.getWorkOrderQuantity()	
	if totalQuantity > 0:
		factor = outfeed / totalQuantity
	else:
		factor = 0.0
	plant = system.tag.read('factory/param_code').value
	headers = ["materialNumber","materialDesc","percentage","orderedQuantity","usedQuantity","date","unit","plant","storage","batchNumber","materialMovement","itemCategory"]
	data = []
	
	for mat in bomMat:
		tmpRow = []
		matNumber = ""
		if mat["materialNumber"]:
			matNumber = mat["materialNumber"]
		
		
		batchNumber = "" 
		if mat["batchNumber"]:
			batchNumber = mat["batchNumber"]
	
		itemCat = mat["itemCategory"]
		backFlushFlag = mat["backflushFlag"]
		lgort = mat["consumMaterialStorage"]
		units = mat["confirmationUnits"]
		if not units:
			units = mat["measureUnit"]
		quantity = mat["requiredQuantity"]
	
		
		# Check for stock item and backflush flag
		if itemCat == "L" and backFlushFlag:
			if mat['batchNumber']:
				batchNumber = mat['batchNumber']
			else:
				batchNumber = ""
			materialConsumptionQty =  math.ceil((outfeed / totalQuantity) * float(quantity))
			date = system.date.now()
			tmpRow.append(matNumber)
			tmpRow.append(mat['materialDescription'])
			tmpRow.append(str(round(factor,4)))
			tmpRow.append(str(quantity))
			tmpRow.append(str(round(float(materialConsumptionQty),3)))
			tmpRow.append(str(date))
			tmpRow.append(str(units))
			tmpRow.append(plant)
			tmpRow.append(lgort)
			tmpRow.append(batchNumber)
			tmpRow.append('261')
			tmpRow.append(itemCat)
			data.append(tmpRow)
				
				
	
	matConsumptionDs = system.dataset.toDataSet(headers, data)	
	
	if not system.tag.exists(ignitionTagPath+"/MESProcessTags/materialConsumption"):
		system.tag.addTag(parentPath=ignitionTagPath+"/MESProcessTags",name="materialConsumption",tagType="MEMORY", dataType="DataSet")
	
	system.tag.write(ignitionTagPath+"/MESProcessTags/materialConsumption",matConsumptionDs)		
	#print system.util.jsonEncode(dictGoodsMovement,4)