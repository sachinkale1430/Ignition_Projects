def getStoredData(startDate, endDate, workCenter):
	if workCenter == "All stations":
		sqlQuery = """
		SELECT 
			workcenter.code as Location,
			workorder.number as ProductionOrder,
			workorder.main_material_nb as ProductCode,
			step_result.id as PieceNumber,
			step_result.step_id as Step,
			step.workcenter_id,
			step.workorder_id,
			TRIM(BOTH '"' FROM CAST(step_result.data -> '$.start' AS CHAR CHARACTER SET utf8)) as StartTime,
			TRIM(BOTH '"' FROM CAST(step_result.data -> '$.end' AS CHAR CHARACTER SET utf8)) as EndTime,
			step_result.operator as Operator,
			step_result.data as ProcessData 
			FROM workcenter  
			INNER JOIN step 
			ON workcenter.id = step.workcenter_id 
			INNER JOIN step_result 
			ON step.id = step_result.step_id AND step.workorder_id = step_result.workorder_id 
			INNER JOIN workorder 
			ON step_result.workorder_id = workorder.id 
		WHERE  
		STR_TO_DATE(step_result.data ->>"$.start", '%Y-%m-%dT%T') >= ? AND
		STR_TO_DATE(step_result.data ->>"$.end", '%Y-%m-%dT%T') <= ?
		ORDER BY StartTime DESC
		"""
		sqlResult = system.db.runPrepQuery(sqlQuery, [startDate, endDate], "factory_production")

	else:
		sqlQuery = """
			SELECT 
				workcenter.code as Location,
				workorder.number as ProductionOrder,
				workorder.main_material_nb as ProductCode,
				step_result.id as PieceNumber,
				step_result.step_id as Step,
				step.workcenter_id,
				step.workorder_id,
				TRIM(BOTH '"' FROM CAST(step_result.data -> '$.start' AS CHAR CHARACTER SET utf8)) as StartTime,
				TRIM(BOTH '"' FROM CAST(step_result.data -> '$.end' AS CHAR CHARACTER SET utf8)) as EndTime,
				step_result.operator as Operator,
				step_result.data as ProcessData 
				FROM workcenter  
				INNER JOIN step 
				ON workcenter.id = step.workcenter_id 
				INNER JOIN step_result 
				ON step.id = step_result.step_id AND step.workorder_id = step_result.workorder_id 
				INNER JOIN workorder 
				ON step_result.workorder_id = workorder.id 
			WHERE workcenter.code = ? AND 
			STR_TO_DATE(step_result.data ->>"$.start", '%Y-%m-%dT%T') >= ? AND
			STR_TO_DATE(step_result.data ->>"$.end", '%Y-%m-%dT%T') <= ?
			ORDER BY StartTime DESC
			"""
		
		sqlResult = system.db.runPrepQuery(sqlQuery, [workCenter, startDate, endDate], "factory_production")
	
	sqlResultPy = system.dataset.toPyDataSet(sqlResult)
	
	headers = [
		"Location",
		"Production order",
		"Product code",
		"PCE. number",
		"Step",
		"workcenter_id",
		"workorder_id",
		"Start time",
		"End time",
		"Operator",
		"ProcessData"
		]
		
	data = []
	
	for row in sqlResultPy:
		location = row["Location"]
		po = row["ProductionOrder"]
		productCode = row["ProductCode"]
		PCENumber = row["PieceNumber"]
		step = row["Step"]
		workcenterId = row["workcenter_id"]
		workorderId = row["workorder_id"]
		startTime = row["StartTime"]
		endTime = row["EndTime"]
		operator = row["Operator"]
		processData = row["ProcessData"]
		
		data.append([
			location,
			po,
			productCode,
			PCENumber,
			step,
			workcenterId,
			workorderId,
			#startTime,
			#endTime,
			shared.utils.date.translate_iso_8061_time(startTime),
			shared.utils.date.translate_iso_8061_time(endTime),
			operator,
			processData
			])	
	
	dataDS = system.dataset.toDataSet(headers, data)

	return dataDS	
	

def getRecordedPCE(ignitionTagPath):
	workCenter = system.tag.read(ignitionTagPath + "/mes/param_sapWorkCenter").value 
	step_id = system.tag.read(ignitionTagPath + "/mes/param_sapStepId").value
	workorder = system.tag.read(ignitionTagPath + "/mes/prod_workOrder").value

	sqlQuery = """
		SELECT 
			workorder.number as ProductionOrder,
			workorder.main_material_nb as ProductCode,
			step_result.id as PieceNumber,
			step_result.step_id as Step,
			step.workcenter_id,
			step.workorder_id
			FROM workcenter  
			INNER JOIN step 
			ON workcenter.id = step.workcenter_id 
			INNER JOIN step_result 
			ON step.id = step_result.step_id AND step.workorder_id = step_result.workorder_id 
			INNER JOIN workorder 
			ON step_result.workorder_id = workorder.id 
		WHERE workcenter.code = ? AND step_result.step_id = ? AND workorder.number = ?
		ORDER BY step_result.id
		"""
		
	sqlResult = system.db.runPrepQuery(sqlQuery, [workCenter, step_id, workorder], "factory_production")
	sqlResultPy = system.dataset.toPyDataSet(sqlResult)	
	
	return sqlResultPy

	


#===========================================================
# 
#===========================================================
def getNextMissingPCE(ignitionTagPath):	
	getPCE = getRecordedPCE(ignitionTagPath)
	
	if len(getPCE) > 0:
		PCECounter = 1
		for row in getPCE:
			if row["PieceNumber"] <> PCECounter:
				nextMissingPCE = PCECounter
				break
			else:
				nextMissingPCE = PCECounter + 1
			PCECounter += 1
	else:
		nextMissingPCE = 1
	
	return float(nextMissingPCE)
