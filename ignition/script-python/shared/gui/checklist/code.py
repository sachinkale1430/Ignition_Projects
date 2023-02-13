def runChecklist(uuid, ignitionTagPath):
	"""
	Starts up checklist application with its UUID. For now get it from database explorer.
	
	Parameters
	----------
	uuid: str
		checklist uuid
	"""
	windows = ["_application/SGA/_common/header","_application/SGA/checklist/details"]

	currentWindows = list(system.gui.getOpenedWindowNames())
	params = {"uuid": uuid, "windows": currentWindows, "ignitionTagPath": ignitionTagPath}

	system.util.retarget("sga_checklist",params=params,windows=windows)

def runChecklistPerShift(uuid, currentMachine):
	"""
	Starts up checklist application with its UUID checks if user already run checklist . For now get it from database explorer.
	
	Parameters
	----------
	uuid: str
		checklist uuid
	currentMachine: str
		name of machine checklist is being ran
	"""
	
	username = system.security.getUsername()
	
	sql = "SELECT feedbackId FROM feedback JOIN machine ON(feedback.machineId=machine.machineId) WHERE startTimestamp >= NOW() - INTERVAL 8 HOUR AND machine.name = ? AND feedback.user = ?"
		
	res = system.db.runPrepQuery(sql, args=[currentMachine, username], database="sga_checklist")
	
	if len(res)==0:
		runChecklist(uuid)