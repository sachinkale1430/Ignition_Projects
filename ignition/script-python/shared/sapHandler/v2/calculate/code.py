def getSapTimeFromWorkorder(workorder, operationStep, workCenter):
	"""
	Simple Function to get SAP times from iDoc.
	
	Parameters
	----------
	workorder : str
		workorder from which to get SAP times
	operationStep : str
		SAP Operation step from which to get SAP times
	workCenter: str
		SAP Work center from which to get SAP times

	
	Returns
	-------
	dict
		Dict properties for Response segment
	"""
	json ={}

	ds = shared.mes.workorder.getSAPProductionStepDetails(workOrderName=workorder, workCenter=workCenter, operationNumber=operationStep, 
													fields=["baseOperationQuantity","operationUnits","operationQuantity","setupTime", "setupTimeUnit", "machineTime", "machineTimeUnit","laborTime", "laborTimeUnit"])
	
	for row in ds:
		
		json = {"sapLabor":{"time":row["laborTime"],"units":row["laborTimeUnit"]},
				"sapSetup":{"time":row["setupTime"],"units":row["setupTimeUnit"]},
				"sapMachine":{"time":row["machineTime"],"units":row["machineTimeUnit"]},
				"operationQuantity":row["operationQuantity"],
				"operationUnits":row["operationUnits"],
				"baseQuantity":row["baseOperationQuantity"]
				}								
	return json

def getCalculatedSapTime(sapTimes, scheduleMode, sapCoefficients):
	"""
	Function to get calculated time from given schedule mode and sap coefficients
	
	Parameters
	----------
	sapTimes : dict
		Dict from function getSapTimeFromWorkorder
	scheduleMode : str
		Schedule mode from function getCoefficients
	sapCoefficients: dict
		Dict from function getCoefficients
	version: str default 2.0
		Version of the interface

	Returns
	-------
	Decimal,Decimal, Decimal
		Labor time calculated, machineTime calculated and setupTime calculated
	"""
	from decimal import Decimal
	
	def convertToMinutes(sapTime, sapTimeUnits):
		if sapTimeUnits == "MIN":
			return Decimal(sapTime)
		if sapTimeUnits == "H" or sapTimeUnits == "HUR":
			return Decimal(sapTime) / 60.0 if sapTime != 0 else 0
		if sapTimeUnits == "S" or sapTimeUnits == "SEC":
			return Decimal(sapTime) * 60.0 
			
	def sapTimesValidator(sapTimes, category):
		if category in sapTimes:
			if "time" in sapTimes[category] and "units" in sapTimes[category]:
				time = sapTimes[category]["time"]
				units = sapTimes[category]["units"]
				return convertToMinutes(time,units)
		return None
	
	def calcLaborTime(laborTime,scheduleMode,opQuantity,opBaseQuantity, coefficient=1.0):
				
		if scheduleMode == "scaled":
			if opBaseQuantity:
				if opBaseQuantity > 0:
					return ((laborTime / Decimal(opBaseQuantity)) * Decimal(opQuantity)) * Decimal(coefficient)
		return (laborTime / Decimal(opBaseQuantity)) * Decimal(opQuantity)
		
	
	def calcMachineTime(machineTime,scheduleMode,opQuantity,opBaseQuantity, coefficient=1.0):
		if scheduleMode == "scaled":
			if opBaseQuantity:
				if opBaseQuantity > 0:
					return ((machineTime / Decimal(opBaseQuantity)) * Decimal(opQuantity)) * Decimal(coefficient)
		else:
			return (machineTime / Decimal(opBaseQuantity)) * Decimal(opQuantity)
		
	
	def calcSetupTime(setupTime,scheduleMode,opQuantity,opBaseQuantity, coefficient=1.0):
		if scheduleMode == "scaled":
			if opBaseQuantity:
				# due to ticket  SDSGA-4257 
				return setupTime  * Decimal(coefficient)
		else:
			return setupTime  * Decimal(coefficient)

	
	def calcScheduleRate(operationQuantity, calcMachine):	
		scheduleRate = Decimal("10.0")
		if calcMachineTime != 0:
			scheduleRate = Decimal(operationQuantity) / Decimal(calcMachine)
		return scheduleRate
		
	
	def calcStandardRate(targetOEE,scheduledRate, cycleTime):
		standardRate = Decimal("10.0")
		if cycleTime:
			if cycleTime != "0": 
				return Decimal("60.0") / Decimal(cycleTime)
		else:
			if targetOEE != "0":
				return 	Decimal(scheduledRate) / Decimal(targetOEE)
		return standardRate
		
	#Validator for sapTimes
	
	laborTime = sapTimesValidator(sapTimes, "sapLabor")
	setupTime  = sapTimesValidator(sapTimes, "sapSetup")
	machineTime = sapTimesValidator(sapTimes, "sapMachine")
	operationQuantity = sapTimes["operationQuantity"]
	operationBaseQuantitiy = sapTimes["baseQuantity"]
	
	targetOEE = sapCoefficients["targetOEE"]
	cycleTime = sapCoefficients["cycleTime"]
	#print sapCoefficients
	
	if scheduleMode == "scaled":
		setupCoefficient = Decimal(sapCoefficients["setupCoefficient"])
		laborCoefficient = Decimal(sapCoefficients["laborCoefficient"])
		machineCoefficient = Decimal(sapCoefficients["machineCoefficient"])
	else:
		setupCoefficient = Decimal("1.0")
		laborCoefficient = Decimal("1.0")
		machineCoefficient = Decimal("1.0")
	#print sapTimes
	
	
	
	if laborTime is None or setupTime is None or machineTime is None:
		return "SAP times given are in wrong format."
	
	calcLabor = calcLaborTime(laborTime, scheduleMode,operationQuantity,operationBaseQuantitiy, laborCoefficient)
	calcMachine = calcMachineTime(machineTime, scheduleMode,operationQuantity,operationBaseQuantitiy, machineCoefficient)
	calcSetup = calcSetupTime(setupTime, scheduleMode,operationQuantity,operationBaseQuantitiy, setupCoefficient)
	scheduleRate = calcScheduleRate(operationQuantity, calcMachine)
	standardRate = calcStandardRate(targetOEE,scheduleRate, cycleTime)
	
	returnJson = {"calcLabor":calcLabor,
				  "calcMachine":calcMachine,
				  "calcSetup":calcSetup,
				  "scheduleRate":scheduleRate,
				  "standardRate":Decimal(str(standardRate))}
	
	return returnJson
	
def getCoefficients(equipmentPath, materialName):
		"""
		Function to get coefficients from the MES objects
		
		Parameters
		----------
		equipmentPath : str
			Equipment where the PO is going to be started
		materialName : str
			Material number of the PO started
		version: str default 2.0
			Version of the interface
	
		Returns
		-------
		dict,str
			Containing coefficients as setup in factory configurator and schedule mode
		"""
		def coeffiecientLogic(machineCustomProp, materialMachineCustomProp):
			#TODO optimize it 
			supportedScheduleModes = ["scaled", "standard"]
			usedSchedule = "standard"
			if materialMachineCustomProp:
				if materialMachineCustomProp["sapSchedulingMode"]["value"] in supportedScheduleModes:
					materialScheduleMode = materialMachineCustomProp["sapSchedulingMode"]["value"]
				else:
					materialScheduleMode = "standard"
			else:
				materialScheduleMode = "standard"
			#test	
			if materialScheduleMode == "scaled":
				laborCoefficient = materialMachineCustomProp["sapCoefLaborTime"]["value"] if materialMachineCustomProp["sapCoefLaborTime"]["value"] else 1.00
				machineCoefficient = materialMachineCustomProp["sapCoefMachineTime"]["value"] if materialMachineCustomProp["sapCoefMachineTime"]["value"] else 1.00
				setupCoefficient = materialMachineCustomProp["sapCoefSetupTime"]["value"] if materialMachineCustomProp["sapCoefSetupTime"]["value"] else 1.00
				usedSchedule = materialScheduleMode
			else:
				if machineCustomProp["sapSchedulingMode"]["value"] in supportedScheduleModes:
					machineScheduleMode = machineCustomProp["sapSchedulingMode"]["value"]
				else:
					machineScheduleMode = "standard"
					
				if machineScheduleMode == "scaled":
					laborCoefficient = machineCustomProp["sapCoefLaborTime"]["value"] if machineCustomProp["sapCoefLaborTime"]["value"] else 1.00
					machineCoefficient = machineCustomProp["sapCoefMachineTime"]["value"] if machineCustomProp["sapCoefMachineTime"]["value"] else 1.00
					setupCoefficient = machineCustomProp["sapCoefSetupTime"]["value"] if machineCustomProp["sapCoefSetupTime"]["value"] else 1.00
					usedSchedule = machineScheduleMode
				else:
					laborCoefficient = 1.0
					machineCoefficient = 1.0
					setupCoefficient = 1.0
					usedSchedule = machineScheduleMode
			return laborCoefficient, machineCoefficient, setupCoefficient, usedSchedule
		
		def getCycleTimeTargetOEE(machineCustomProp, materialMachineCustomProp):
			#Default values
			cycleTime = None
			targetOEE = 0.7
			if machineCustomProp:
				cycleTime = machineCustomProp["cycleTime"]["value"] if machineCustomProp["cycleTime"]["value"] else None
				targetOEE = machineCustomProp["targetOEE"]["value"] if machineCustomProp["targetOEE"]["value"] else "0.7"
			if materialMachineCustomProp:
				cycleTime = materialMachineCustomProp["cycleTime"]["value"] if materialMachineCustomProp["cycleTime"]["value"] else cycleTime
				targetOEE = materialMachineCustomProp["targetOEE"]["value"] if materialMachineCustomProp["targetOEE"]["value"] else targetOEE
			return cycleTime,targetOEE
		
		customProperties = ["sapCoefMachineTime", "sapCoefLaborTime","sapCoefSetupTime","sapSchedulingMode","targetOEE","cycleTime"]
		
		machineCustomProp, materialMachineCustomProp = getCustomPropertiesFromEquipmentMaterial(equipmentPath, materialName, customProperties)
		
		returnJson = {"laborCoefficient": 1.0, 
						  "machineCoefficient":1.0,
						  "setupCoefficient":1.0,
						  "cycleTime":None,
						  "targetOEE":"0.7",
						  "usedSchedule":"standard"
						  }	
		
		
		laborCoefficient, machineCoefficient, setupCoefficient, usedSchedule = coeffiecientLogic(machineCustomProp,materialMachineCustomProp)
		cycleTime,targetOEE = getCycleTimeTargetOEE(machineCustomProp, materialMachineCustomProp)
		
		returnJson["laborCoefficient"] = laborCoefficient
		returnJson["machineCoefficient"] = machineCoefficient
		returnJson["setupCoefficient"] = setupCoefficient
		returnJson["targetOEE"] = targetOEE
		returnJson["usedSchedule"] = usedSchedule
		returnJson["cycleTime"] = cycleTime
		
		return returnJson

def getCustomPropertiesFromEquipmentMaterial(equipmentPath, materialName, customProperties):
	"""
	Function to get custom property from equipmentPath and material
	
	Parameters
	----------
	equipmentPath : str
		Equipment where the PO is going to be started
	materialName : str
		Material number of the PO started
	customProperties: List 
		list of custom properties to get

	Returns
	-------
	dict,str
		Custom properties from machine and machine/material
	"""	
	equipmentPath = str(equipmentPath)
	
	# checks if equipment exists
	try:
		equipmentObject = system.mes.loadMESObjectByEquipmentPath(equipmentPath)
	except:
		return "No such equipment: "+equipmentPath 
	machineCustomProp = shared.sapHandler.common.customProperties.getCustomProperties(equipmentObject, customPropertyNames=customProperties)
	
	materialMachineCustomProp = {}
	
	materialName = str(materialName)
	# This checks if materials exists
	try:
		materialObject = system.mes.getMESObjectLinkByName("MaterialDef",materialName)
		matName = materialObject.getName()
		eqPath = equipmentObject.getEquipmentPath()
		eqPath =eqPath.replace("\\", ":")
		opDefName = matName+"-"+eqPath
		opDef = system.mes.loadMESObject(opDefName,"OperationsDefinition")
		materialMachineCustomProp = shared.sapHandler.common.customProperties.getCustomProperties(opDef,customProperties)
				
	except:
		pass
	return machineCustomProp, materialMachineCustomProp