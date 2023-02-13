# ==============================================
# shared.mes.batch
# Last update: 2019-01-04
# Modified by: Rok Zupan
# ==============================================

def createBatchFromSAP(
	sapBatchNb, materialNb, qty, batchType, eventType, 
	user, code="", details="", workOrder="", comment="", unit="PCE"):
	
	"""
	Function for storing data at the end of production step.
	
	Parameters
	----------
	sapBatchNb : str
		batch number under which the data will be tracked
	materialNb : str
		material SAP number
	qty : str
		quantity that represents consumption
	batchType : str
		batch type like JUMBO, GLASS, ABRASIVE etc..
	eventType : str
		event type like creation, usage, waste, ...
	user : str
		user that triggered this event
	price : float
		material price
	code : str
		SAP reject code
	details : JSON
		details meant for any additional data that needs to be stored
	workOrder : str, optional
		workOrder on which we want to record the batch
	comment : str, optional
		comment text
	units : str, optional
		If piece units are in diffrent format change this. 
		Default "PCE"
	
	Return
	------
	if insertation of new True else False	
	"""

	result = checkBatch(sapBatchNb)
	
	if len(result) < 1:
		eventType = "creation"		
		
		batchId = insertBatch(batchType, 1, materialNb, qty, sapBatchNb, 2, comment=comment, unit=unit)

		
		insertBatchEvent(batchId, qty, eventType, user, workOrder, code=code, details=details)
		#updateBatch(batchId, qty, comment=comment)
		
		return True
	
	else:
		return False


	

def createBatchFromIgnition(
	sapBatchNb, materialNb, qty, batchType, eventType, 
	user, code="", details="", workOrder="", comment="", unit="PCE"):
	"""
	Function for storing data at the end of production step.
	
	Parameters
	----------
	sapBatchNb : str
		batch number under which the data will be tracked
	materialNb : str
		material SAP number
	qty : str
		quantity that represents consumption
	batchType : str
		batch type like JUMBO, GLASS, ABRASIVE etc..
	eventType : str
		event type like creation, usage, waste, ...
	user : str
		user that triggered this event
	code : str
		SAP reject code
	details : JSON
		details meant for any additional data that needs to be stored
	workOrder : str, optional
		workOrder on which we want to record the batch
	comment : str, optional
		comment text
	units : str, optional
		If piece units are in diffrent format change this. 
		Default "PCE"
	
	Raises
	------
	TypeError	
	
	Example call:
	"""
	
	eventTypes = ["usage", "waste", "finished", "allocation", "addition"]
	
	result = checkBatch(sapBatchNb)
	print "*****"
	print sapBatchNb
	if len(result) < 1:
		eventType = "creation"		
		statusId = 2
		batchId = insertBatch(batchType, 1, materialNb, qty, sapBatchNb, statusId, comment=comment, unit=unit)
	
	else:
		if eventType in eventTypes:
			if eventType == "waste":
				if len(code) < 1:
					raise TypeError("Reject code needs to be defined!")					
			
				qty = 0 - qty		
				statusId = 2
			
			elif eventType == "usage":
				qty = 0 - qty
				statusId = 7
			
			elif eventType == "allocation":
				qty = result.getValueAt(0,"remainingQty")
				statusId = 5
			
			elif eventType == "addition":
				qty = qty
				statusId = 2
				
			else:								
				statusId = 2

			batchId = result.getValueAt(0, "batchId")
			
		else:
			raise TypeError("Event type not correct. Allowed types are: " + (', '.join(eventTypes)))
	
	if insertBatchEvent(batchId, qty, eventType, user, workOrder, code=code, details=details):
		updateBatch(batchId, qty, statusId, comment=comment, eventType=eventType)

			

def insertBatch(batchType, enabled, materialNb, remainingQty, sapBatchNb, statusId, comment, unit):
	"""
	Function for storing batch at the creation
	
	Parameters
	----------
	batchType : str
		batch number under which the data will be tracked
	enabled : str
		deletion flag
	materialNb : str
		material SAP number
	remainingQty : str
		remaining quantity of the batch. in this case = total qty 
	sapBatchNb : str
		batch number under which the data will be tracked
	comment : str
		comment coming from main function call
	units : str
		units coming from main function call
		
	Returns
	-------
	int
		Returns db unuque id
	"""
	
	enabled = 1

	sqlQuery = """INSERT INTO batch (batchType, comment, enabled, materialNb, remainingQty, sapBatchNb, statusId, units) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
	"""
	
	batchId = system.db.runPrepUpdate(sqlQuery, [batchType, comment, enabled, materialNb, remainingQty, sapBatchNb, statusId, unit], "factory_production", getKey = 1)
	
	return batchId



def updateBatch(updateBatchId, qty, statusId, comment, eventType):
	"""
	Function to update main batch data (batch table)
	
	Parameters
	----------
	updateBatchId : int
		updateBatchId is id from database
	qty : float
		quantity
	comment : str
		comment coming from main function call
		
	Returns
	-------
	Returns True
	"""
	
	sqlQuery = """
		SELECT
			SUM(qty) 
		FROM 
			batchEvents 
		WHERE 
			batchId = ? AND 
			type <> 'allocation' AND 
			(details->>'$.wasteType' != "geometrical" or details->>'$.wasteType' is null) AND 
			enabled = 1
	"""
	
	totalQty = system.db.runScalarPrepQuery(sqlQuery, [updateBatchId], "factory_production")
	
	#if totalQty > 0:
	#	statusId = 3
	#elif totalQty <= 0:
	#	statusId = 1
	#else:
	#	statusId = 4
		
	if totalQty is not None:	
		sqlQuery = "UPDATE batch SET remainingQty = ?, statusId = ?, comment = ? WHERE id = ?"
		args = [totalQty, statusId, comment, updateBatchId]
	else:
		sqlQuery = "UPDATE batch SET statusId = ?, comment = ? WHERE id = ?"
		args = [statusId, comment, updateBatchId]
	
	sqlResult = system.db.runPrepUpdate(sqlQuery, args, "factory_production")
	
	return True	



def updateBatchEnable(sapBatchNb, enabled = 1):
	"""
	Function to enable / disable batch
	
	Parameters
	----------
	sapBatchNb : str
		batch number under which the data will be tracked
	enabled: int
		used to set enabled value
		
	Returns
	-------
	None
	"""
	
	sqlQuery = "UPDATE batch SET enabled= ? WHERE sapBatchNb = ?"
	sqlResult = system.db.runPrepUpdate(sqlQuery, [enabled, sapBatchNb], "factory_production")



def checkBatch(sapBatchNb):
	"""
	Function for geting batch entry in events
	
	Parameters
	----------
	workOrder : str
		corresponding SAP work order
	sapBatchNb : str
		batch number under which the data will be tracked
	
	Returns
	-------
	dataset
		Returns dataset result of DB query
	"""
	
	sqlQuery = """
		SELECT 
			batchEvents.id as id,
			batchEvents.batchId as batchId,
			batchEvents.workOrder as workOrder,
			batchEvents.type as type,
			batch.remainingQty as remainingQty,
			batch.sapBatchNb as batchNumber
		FROM batchEvents 
		INNER JOIN batch ON batch.id = batchEvents.batchId
		WHERE  
			batch.sapBatchNb = ?
		ORDER BY
			batchEvents.timestamp DESC
		LIMIT 1
	"""
	
	sqlResult = system.db.runPrepQuery(sqlQuery, [sapBatchNb], "factory_production")
	
	return sqlResult
	
	
	
def insertBatchEvent(batchId, qty, type, user, workOrder, code, details):
	"""
	Function to insert batch event into batchEvents table
	
	Parameters
	----------
	batchId : str
		batchId is id from database
	qty : float
		quantity that represents consumption
	type : str
		types that are allowed: creation, allocation, usage, waste and finished
	user : str
		user that triggered event
	workOrder : str
		workOrder on which we want to record the batch
	code : str
		if event type is waste, rejection code needs to be provided
	details : json
		json data with custom structure (can be whatever)
		
	Returns
	-------
		Returns True
	"""	
	if len(details) < 1:
		details = system.util.jsonEncode("{}")
		
	timestamp = system.tag.read("[System]Gateway/CurrentDateTime").value
	enabled = 1
	
	sqlQuery = """INSERT INTO 
					batchEvents 
						(batchId, code, details, enabled, qty, timestamp, type, user, workOrder) 
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	"""

	sqlResult = system.db.runPrepUpdate(sqlQuery, [batchId, code, details, enabled, qty, timestamp, type, user, workOrder], "factory_production")	
	
	return True
	


def getLastBatchName(factoryCode): 
	"""
	Function that will read last batch number that is stored in db
	
	Parameters
	----------
	factoryCode : str
		factory code presenting 3 char lenght
		
	Returns
	-------
	sqlResult: str
		Last batch name
	"""	

	sqlQuery = """
		SELECT 
			sapBatchNb 
		FROM 
			batch
		WHERE 
			sapBatchNb LIKE '""" + factoryCode + """%' 
		ORDER BY 
			sapBatchNb DESC
		LIMIT 1	
	"""
			
	sqlResult = system.db.runScalarQuery(sqlQuery, "factory_production")
	
	return sqlResult



def buildBatchName(factoryCode):
	"""
	Function that builds batch name
	
	Parameters
	----------
	factoryCode : str
		factory code presenting 3 char lenght
	
	Returns
	-------
		batchNumber: str
		Returns newly generated batch number
		Ex: CON190200A (CON = Conflans, 19 = year, 02 = week in year, 00A - incremental value)
		max incremental value = ZZZ = 26*26*26 = 17576
	"""	
	
	A_UPPERCASE = ord('A')
	ALPHABET_SIZE = 26
	
	
	def _decompose(number):
	    """Generate digits from `number` in base alphabet, least significants
	    bits first.
	
	    Since A is 1 rather than 0 in base alphabet, we are dealing with
	    `number - 1` at each iteration to be able to extract the proper digits.
	    """
	
	    while number:
	        number, remainder = divmod(number - 1, ALPHABET_SIZE)
	        yield remainder


	
	def base_10_to_alphabet(number):
	    """
	    Convert a decimal number to its base alphabet representation
		"""
	
	    return ''.join(
	            chr(A_UPPERCASE + part)
	            for part in _decompose(number)
	    )[::-1]

	
	
	def base_alphabet_to_10(letters):
	    # Convert an alphabet number to its decimal representation
	
	    return sum(
	            (ord(letter) - A_UPPERCASE + 1) * ALPHABET_SIZE**i
	            for i, letter in enumerate(reversed(letters.upper()))
	    )
	

	getLastBatch = getLastBatchName(factoryCode)

	
	if getLastBatch:
		lastBatchWeek = getLastBatch[5:7]
		weekInYear = system.date.format(system.tag.read("[System]Gateway/CurrentDateTime").value, "w") # get week in the year like 23	
		weekInYear = weekInYear.zfill(2)

		if weekInYear <> lastBatchWeek:
			lastBatchNumber = base_alphabet_to_10('AAA')
			newBatchNumber = lastBatchNumber
		else:
			getLastBatchAsChar = ''.join(i for i in getLastBatch[-3:] if not i.isdigit()) # remove digits
			lastBatchNumber = base_alphabet_to_10(getLastBatchAsChar)
			newBatchNumber = lastBatchNumber + 1
	else:
		newBatchNumber = 1
		
	
	if len(factoryCode) <> 3:
		raise TypeError("Factory lenght not correct. Should be 3 char long!")
	
	
	year = system.date.format(system.tag.read("[System]Gateway/CurrentDateTime").value, "yy")
	weekInYear = system.date.format(system.tag.read("[System]Gateway/CurrentDateTime").value, "w") # get week in the year like 23	
	weekInYear = weekInYear.zfill(2)
	
	
	batchNb = base_10_to_alphabet(newBatchNumber)
	
	
	if len(batchNb) > 3:
		raise TypeError("Generated batch number is to high!")
	else:
		batchNb = batchNb.zfill(3)
	
	
	batchNumber = factoryCode + str(year) + str(weekInYear) + str(batchNb)
	
	
	return batchNumber


