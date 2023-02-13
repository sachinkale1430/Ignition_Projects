#===============================================
# OEE BONDEND CALCULATION
# ATTENTION - Calculation is the following
# oee = quality * availability
# availability = pressing time / workstation runtime (based on planning schedule)
#===============================================
def getOEEDataSGABonded(startDate, endDate, eqPath, forTagValue):
	# Get pressing state
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("getPressingState")
	datapoints = [
		"Equipment State Path",
		"Equipment State Name",
		"State Duration"
		]
			
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path AND Equipment State Name = 'Pressing'")
	analysis_setting.setGroupBy("Equipment State Path,Equipment State Name")
	analysis_setting.setOrderBy("State Duration")
	
	params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()
	
	# get total pressing time in seconds
	if analysisData.getValueAt(0, "State Duration") > 0:
		pressingStateDuration = analysisData.getValueAt(0, "State Duration") * 60
	else:
		pressingStateDuration = 0
	# get total workstation runtime
	workstationScheduledRuntime = shared.utils.schedule.getScheduleTotalPeriod(startDate, endDate, eqPath)
	
	if workstationScheduledRuntime > 0:
		#calculate availability
		oeeAvailability = pressingStateDuration / workstationScheduledRuntime
	else:
		oeeAvailability = 0
	
	#get ISA-95 data (because we still need quality)
	oeeMESData = getOEEData(startDate, endDate, eqPath)
	
	oeePerformance = oeeMESData["oeePerformance"]
	
	oeeQuality = oeeMESData["oeeQuality"]
	
	oee = oeeAvailability * oeeQuality
	
	if forTagValue == True:
		tagOee = "%.3f" % oee
		tagAvailability = "%.3f" % oeeAvailability 
		tagData = str(tagOee) + "/" + str(tagAvailability)
		#tagData = str(round(float(oee), 3)) + "/" + str(round(float(oeeAvailability),3))
		#tagData = str(round(float(oee), 3)) + "/" + str(round(float(oeeAvailability),3))
		#tagData = str(format(float(oee), '.3f')) + "/" + str(format(float(oeeAvailability), '.3f'))
		
		return tagData
	else:
		data = {
			"oee": oee,
			"oeeAvailability": oeeAvailability,
			"oeePerformance": oeePerformance,
			"oeeQuality": oeeQuality
		}
		
		return data
	
#===============================================
# CLASICC CALCULATION
#===============================================
# get general OEE components
#===============================================
def getOEEData(startDate, endDate, eqPath):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("generalOEE")
	
	datapoints = [
		"OEE",
		"OEE Availability",
		"OEE Performance",
		"OEE Quality"
		]
	
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path")
	
	params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()

	oee = analysisData.getValueAt(0, "OEE")
	oeeAvailability = analysisData.getValueAt(0, "OEE Availability")
	oeePerformance = analysisData.getValueAt(0, "OEE Performance")
	oeeQuality = analysisData.getValueAt(0, "OEE Quality")
	
	data = {
		"oee": oee,
		"oeeAvailability": oeeAvailability,
		"oeePerformance": oeePerformance,
		"oeeQuality": oeeQuality
	}
	
	return data
	

	
#===============================================
# get general OEE components by interval
#===============================================
def getOEETimelineData(startDate, endDate, eqPath):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("generalOEE")
	datapoints = [
		#"OEE",
		#"OEE Availability",
		#"OEE Performance",
		#"OEE Quality",
		"oeeLast8h",
		"oeeAvailabilityLast8h",
		"OEE Quality",
		"OEE Performance",
		"Fifteen Minute Interval"
		]
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path")
	analysis_setting.setGroupBy("oeeAvailabilityLast8h,oeeLast8h,Fifteen Minute Interval")

	
	params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()

	analysisDataPy = system.dataset.toPyDataSet(analysisData)
	
	header = ["t_stamp", "value"]
	oee = []
	oeeAvailability = []
	oeePerformance = []
	oeeQuality = []
	
	for row in analysisDataPy:
		oee.append([row["Fifteen Minute Interval"], float(row["oeeLast8h"])])	
		oeeAvailability.append([row["Fifteen Minute Interval"], float(row["oeeAvailabilityLast8h"])])	
		oeePerformance.append([row["Fifteen Minute Interval"], float(row["OEE Performance"])])	
		oeeQuality.append([row["Fifteen Minute Interval"], float(row["OEE Quality"])])	
		
	oeeDS = system.dataset.toDataSet(header, oee)
	oeeAvailabilityDS = system.dataset.toDataSet(header, oeeAvailability)
	oeePerformanceDS = system.dataset.toDataSet(header, oeePerformance)
	oeeQualityDS = system.dataset.toDataSet(header, oeeQuality)
	
	data = {
		"oee": oeeDS,
		"oeeAvailability": oeeAvailabilityDS,
		"oeePerformance": oeePerformanceDS,
		"oeeQuality": oeeQualityDS
	}
	
	return data


#===============================================
# get general OEE components by interval 1h
#===============================================
def getOEEHourData(startDate, endDate, eqPath):
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("generalOEE")
	datapoints = [
		"oeeLast8h",
		"oeeAvailabilityLast8h",
		"OEE Performance",
		"OEE Quality",
		"Top of Hour Interval"
		]
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path")
	analysis_setting.setGroupBy("Top of Hour Interval,oeeLast8h,oeeAvailabilityLast8h")

	
	params = {'path':eqPath}
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()

	analysisDataPy = system.dataset.toPyDataSet(analysisData)
	
	header = ["label", "value"]
	oee = []
	oeeAvailability = []
	oeePerformance = []
	oeeQuality = []
	
	for row in analysisDataPy:
		hour = system.date.format(row["Top of Hour Interval"], "H")
		oee.append([hour, float(row["oeeLast8h"]) * 100])	
		oeeAvailability.append([row["Top of Hour Interval"], float(row["oeeAvailabilityLast8h"]) * 100])	
		oeePerformance.append([row["Top of Hour Interval"], float(row["OEE Performance"]) * 100])	
		oeeQuality.append([row["Top of Hour Interval"], float(row["OEE Quality"]) * 100])	
		
	oeeDS = system.dataset.toDataSet(header, oee)
	oeeAvailabilityDS = system.dataset.toDataSet(header, oeeAvailability)
	oeePerformanceDS = system.dataset.toDataSet(header, oeePerformance)
	oeeQualityDS = system.dataset.toDataSet(header, oeeQuality)
	
	data = {
		"oee": oeeDS,
		"oeeAvailability": oeeAvailabilityDS,
		"oeePerformance": oeePerformanceDS,
		"oeeQuality": oeeQualityDS
	}
	
	return data
	
	
#===============================================
# get station OEE period definition
#===============================================
def getStationOeePeriod(ignitionTagPath):
	# setting default value = last 8 h
	endDate = system.date.now()
	startDate = system.date.addHours(endDate, -8)
				
	if system.tag.exists(ignitionTagPath + "/oee/param_oeePeriodCode"):
		stationPeriod = system.tag.read(ignitionTagPath + "/oee/param_oeePeriodCode").value
		
		if stationPeriod == "currentShift":
			equipmentPath = system.tag.read(ignitionTagPath + "/mes/param_mesObject").value
			equipmentPath = equipmentPath.replace("[global]\\", "")
		
			startDate = system.tag.read("[MES]" + equipmentPath + "/Shift/Shift Begin Date").value
			endDate = system.tag.read("[MES]" + equipmentPath + "/Shift/Shift End Date").value
			
	return startDate, endDate	