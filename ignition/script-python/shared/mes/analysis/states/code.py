def getStatesHistory(startDate, endDate, eqPath):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("getStatesHistory")
	datapoints = [
		"Work Order",
		"State Begin Time",
		"State Duration",
		"Equipment Original State Value",
		"Equipment State Name",
		"Equipment State Type"
		]
			
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path")
	analysis_setting.setGroupBy("Work Order,State Begin Time,Equipment State Name,Equipment Original State Value")
	analysis_setting.setOrderBy("State Begin Time")
	
	params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()
	
	data = {
		"statesHistory": analysisData
	}
	
	return data


	
def getAllStatesHistory(startDate, endDate):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("getStatesHistory")
	datapoints = [
		"Equipment Name",
		"Work Order",
		"State Begin Time",
		"State Duration",
		"Equipment Original State Value",
		"Equipment State Name",
		"Equipment State Type"
		]
			
	analysis_setting.setDataPoints(datapoints)
	#analysis_setting.addParameter('path')
	#analysis_setting.setFilterExpression("Equipment Path = @path")
	analysis_setting.setGroupBy("State Begin Time,Equipment State Name,Work Order,Equipment Original State Value,Equipment Name")
	analysis_setting.setOrderBy("State Begin Time")
	
	#params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting).getDataset()
	
	data = {
		"statesHistory": analysisData
	}
	
	return data
	
	
	