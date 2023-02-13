def changeState(ignitionTagPath, stateCode):
	stateTagPath = ignitionTagPath + "/mes/oee_state"
	
	if system.tag.exists(stateTagPath):
		system.tag.write(stateTagPath, stateCode)
	else:
		logger("PFCE - Schedule", "Cannot write state code " + str(stateCode) + " to path " + str(stateTagPath))
		
		
def getCustomScheduleStartEnd(scheduleType):
	customSchedules = system.tag.read("[default]Factory/param_customSchedules").value
	
	for schedule in system.dataset.toPyDataSet(customSchedules):
		if schedule["scheduleType"] == scheduleType:
			startDate = schedule["startDate"]
			endDate = schedule["endDate"]
			
			startTime = system.date.format(startDate, "HH:mm:ss")
			endTime = system.date.format(endDate, "HH:mm:ss")
			currentDate = system.date.format(system.date.now(), "YYYY-MM-dd")
			
			startDate = system.date.parse(currentDate + " " + startTime, "yyyy-M-dd HH:mm:ss")
			endDate = system.date.parse(currentDate + " " + endTime, "yyyy-M-dd HH:mm:ss")
			
			if system.date.isBetween(system.date.now(), startDate, endDate):
				return schedule["stateCode"]
				
	return None		