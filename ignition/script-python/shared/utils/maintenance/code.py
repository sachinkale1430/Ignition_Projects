def MESFixLine(linePath):
	system.mes.invalidateCache()
	activeOperations = system.mes.getCurrentOperations(linePath)

	for op in activeOperations:
		op.abort()
		#op.end()

	fixList = ["Equipment Operation UUID", "Equipment Product Code", "Equipment Work Order", "Equipment Mode"]

	lineObjLink = system.mes.getMESObjectLink(system.mes.loadMESObjectByEquipmentPath(linePath).UUID)
	childList = []

	def getChildren(objLink):
		obj = objLink.getMESObject()
		objectLinks = obj.getChildCollection().getList()
		childList.extend(objectLinks)
	
		for objectLink in objectLinks:
			if objectLink.getMESObjectType().getName() == "LineCellGroup":
				getChildren(objectLink)

	getChildren(lineObjLink)

	for fix in fixList:
		value = ''
		
		if fix == "Equipment Mode":
			value = 1
		timeStamp = system.date.now()
		system.mes.addTagCollectorValue(linePath, fix, '', timeStamp, value)
		
		for child in childList:
			childObj = child.getMESObject()
			path = childObj.getEquipmentPath()
	
			try:
				system.mes.addTagCollectorValue(path, fix, '', timeStamp, value)
			except:
				system.mes.updateTagCollectorValue(path, fix, '', timeStamp, value)
				
				
def MESDebugLine(linePath):
	fixList = ["Equipment Operation UUID", "Equipment Product Code", "Equipment Work Order", "Equipment Mode"]
	
	lineObjLink = system.mes.getMESObjectLink(system.mes.loadMESObjectByEquipmentPath(linePath).UUID)
	childList = []
	
	def getChildren(objLink):
		obj = objLink.getMESObject()
		objectLinks = obj.getChildCollection().getList()
		childList.extend(objectLinks)
	
		for objectLink in objectLinks:
			if objectLink.getMESObjectType().getName() == "LineCellGroup":
				getChildren(objectLink)
	
	getChildren(lineObjLink)
	
	def returnInfo(eqPath, collectorType):
		try:
			lastTimeStamp = system.mes.getTagCollectorLastTimeStamp(eqPath, fix, '')
			lastValue = system.mes.getTagCollectorLastValue(eqPath, fix, '')   
			return [eqPath, collectorType, str(lastValue), lastTimeStamp]   
		except:
			return [eqPath, collectorType, None, None]
		 
	headers = ["Equipment Path", "Collector Type", "Last Value", "Last Time Stamp"]
	data = []
	
	for fix in fixList:
		print returnInfo(linePath, fix)
		data.append(returnInfo(linePath, fix))
		 
		for child in childList:
			childObj = child.getMESObject()
			path = childObj.getEquipmentPath()
			print returnInfo(path, fix)
			data.append(returnInfo(path, fix))
	return system.dataset.toDataSet(headers, data)