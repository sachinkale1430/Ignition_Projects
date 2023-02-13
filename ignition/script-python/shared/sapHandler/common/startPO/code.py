def startProductionRun(ignitionTagPath, equipmentPath, materialName, workorder, operationStep=None):
	"""
	Function to start production run based on configuration in MES UDT.
	
	Parameters
	----------
	ignitionTagPath : str
		Tag folder path where MES udt is located
	equipmentPath : str
		Production model equipmenth path
	materialName: bool
		Material to be run
	workorder: str
		Workorder number to be started 

	Returns
	-------
	dict,str
		Result if PO was started. 
	"""	
	
	import traceback
	
	logStrings = "Start PO sequence \n"
	result = False
	try:
	#TODO Optimize reads
	
		if system.tag.exists(ignitionTagPath + "/mes/sap/param_handlerVersion"):
			version = system.tag.read(ignitionTagPath + "/mes/sap/param_handlerVersion").value
			logStrings += "Handler version detected: " + str(version) + "\n"
		else:
			version = 1
			logStrings += "Handler Version missing, setting to version: " + str(version)+ "\n"
			
		if system.tag.exists(ignitionTagPath + "/mes/sap/param_handlerChangeoverMode"):
			segmentMode = system.tag.read(ignitionTagPath + "/mes/sap/param_handlerChangeoverMode").value
			logStrings += "Changeover mode detected: " + str(segmentMode) + "\n"
		else:		
			segmentMode = "skipChangeover"
			logStrings += "Changeover mode missing setting to version: " + str(segmentMode) + "\n"
		
		
		mainScriptFolder = shared.sapHandler.getDict()
		
		versionName = "v"+str(version)
		
		if versionName not in mainScriptFolder:
			#Old version not included in new versioning system
			workcenter = system.tag.read(ignitionTagPath + "/mes/param_sapWorkCenter").value
			shared.mes.production.assignMaterialToEquipment(equipmentPath, materialName, workorder, workcenter)			
			result = shared.mes.production.beginProductionRun(workorder, materialName, equipmentPath)
			changeoverResult = shared.mes.production.endProductionChangeover(equipmentPath)
			result = True
			
		else:	
			try:
				machineScriptFolder = mainScriptFolder[versionName]
				workcenter = system.tag.read(ignitionTagPath + "/mes/param_sapWorkCenter").value
				if operationStep is None:
					operationStep = system.tag.read(ignitionTagPath + "/mes/prod_sapStepId").value
				
				
				
				
				sapTimes = machineScriptFolder.calculate.getSapTimeFromWorkorder(workorder,operationStep, workcenter)	
				logStrings += "SAP times from workorder " + workorder + " JSON: " + system.util.jsonEncode(sapTimes) + "\n"
				
				sapCoefficients = machineScriptFolder.calculate.getCoefficients(equipmentPath,materialName)
				scheduleMode = sapCoefficients["usedSchedule"]
				logStrings += "Coefficients used for this workorder " + workorder + " and material: " + materialName + " JSON: " + system.util.jsonEncode(sapCoefficients) + "\n"
				
				calcTimes = machineScriptFolder.calculate.getCalculatedSapTime(sapTimes, scheduleMode, sapCoefficients)
				logStrings += "Calculated times JSON: " + system.util.jsonEncode(calcTimes) + "\n"
				
				
		
				customPropertyJson = {"calcMachineTime":{"type":"String", "value":str(calcTimes["calcMachine"])},
									"calcSetupTime":{"type":"String", "value":str(calcTimes["calcSetup"])},
									"calcLaborTime":{"type":"String", "value":str(calcTimes["calcLabor"])},
									"operationQuantity":{"type":"String", "value":str(sapTimes["operationQuantity"])},
									"calcScheduleRate":{"type":"String", "value":str(calcTimes["scheduleRate"])},
									"calcStandardRate":{"type":"String", "value":str(calcTimes["standardRate"])},
									"operationUnits":{"type":"String", "value":str(sapTimes["operationUnits"])},
									"targetOEE":{"type":"String", "value":str(sapCoefficients["targetOEE"])}}
				 
				
				
				matLink = system.mes.getMESObjectLinkByName('MaterialDef', materialName)
				operList = system.mes.oee.createMaterialProcessSegment(matLink, equipmentPath)
				logStrings += "Creating links between material and equipment" + "\n"
				tmpOperList = system.mes.object.list.createList()
				
				for opSeg in operList:
					if opSeg.getMESObjectTypeName() == 'OperationsSegment':
						tmpObj = machineScriptFolder.apiHandlers.setMaterialResourceProperty(opSeg, customPropertyJson)
						
						tmpObj = machineScriptFolder.apiHandlers.setProductionSettingsProperty(tmpObj, customPropertyJson)
						
						# Issue with changeover with fixed duration...
						#tmpObj = machineScriptFolder.apiHandlers.setEndTriggerProperty(mesObject, properties,changeOver = True)
						logStrings += "Adding internal properties to operation segment "+tmpObj.getName() + "\n"
						tmpOperList.add(tmpObj)
					else:
						tmpOperList.add(opSeg)
				
				logStrings += "Saving operation segments" + "\n"
				system.mes.saveMESObjects(tmpOperList)
				
				logStrings += "Starting OEE run" + "\n" 
				resSegment = system.mes.oee.beginOEERun(workorder, materialName,equipmentPath)
				
				resObj = shared.sapHandler.common.customProperties.setCustomProperties(resSegment,customPropertyJson)
	
				system.mes.saveMESObject(resObj)
	
				if segmentMode == "skipChangeover":
					logStrings += "Skipping changeover run" + "\n" 
					system.mes.oee.endOEEChangeover(equipmentPath)
					
				result = True
			except:
				logStrings += "Error occured: " + traceback.format_exc() + "\n"
				logStrings += "Attempting to start PO with version 1 handler\n"
				
				workcenter = system.tag.read(ignitionTagPath + "/mes/param_sapWorkCenter").value
				shared.mes.production.assignMaterialToEquipment(equipmentPath, materialName, workorder, workcenter)			
				result = shared.mes.production.beginProductionRun(workorder, materialName, equipmentPath)
				changeoverResult = shared.mes.production.endProductionChangeover(equipmentPath)
				result = True
	except:
		logStrings += "Error occured: " + traceback.format_exc() + "\n"
		result = False
		
	if system.tag.exists(ignitionTagPath + "/mes/sap/log_handlerDebug"):
		system.tag.write(ignitionTagPath + "/mes/sap/log_handlerDebug", logStrings)
	
	return result 