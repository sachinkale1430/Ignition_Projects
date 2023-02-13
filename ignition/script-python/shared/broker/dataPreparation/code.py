def prepareJsonForMqtt(workcenter, workorder, step, piece_data, piece_number, piece_start_time, piece_end_time, operator, material_number,rejects=[], unit="PCE", piece_quantity=1, ignitionTagPath = ""):
    brokerPath = ignitionTagPath + "/mes/broker/prod_runData"
    if system.tag.exists(brokerPath):
    
	    dateparser = shared.utils.date.date_time_iso_8061_parser()
	    
	    mesObject = system.tag.read(ignitionTagPath + "/mes/param_mesObject").value
	    
	    
	    basicStructure = {
	        # Piece number or batch id
	        "batchid": piece_number,
	        #General info about the piece
	        "workorder": workorder,
	        "step": step,
	        "unit": unit,
	        "material_number": material_number,
	        "operator": operator,
	        # Time regarding when piece start and stopped time ISO 8601 format
	        "timeStart": dateparser.format(piece_start_time),
	        "timeStop": dateparser.format(piece_end_time),
	        # Quantity and rejects
	        "yield":piece_quantity,
	        "rejects":rejects,
	        # Mes path and object
	        "mesUdtPath":ignitionTagPath,
	        "mesObjectPath": mesObject,
	        # Additional data stored for this batchid
	        "batchData": piece_data
	        
	    }
	    system.tag.write(ignitionTagPath + "/mes/broker/prod_runData", system.util.jsonEncode(basicStructure))

def cycleDataPreparation(ignitionTagPath, startCycleDateTime, cycleNumber, endCycleDateTime=system.date.now(), productionOrder="", additionalCycleData ={} ):
	
	"""
	Global function for cycle data preparation to stream with BDB.
	
	#Header (Global part which will allow Big data broker team to evaluate and packet)
	#- productionOrder Production order (kept only for verification)
	#- cycleNumber Number of this cycle (1… n)
	#- cycleStart When cycle was started
	#- cycleStop When cycle was stopped
	#- cycleDuration Duration of this cycle
	#Application specific data
	
	
	
	Input:
		ignitionTagPath
			string
		startCycleDateTime
			date
		cycleNumber
			int
	
	- Optional parameters
		endCycleDatetime
			date
				Default: system.date.now()
		productionOrder
			string
				Default: empty string
		additionalCycleData
			dict
				Default: Empty json “{}”
	Output:
		Write to tag
	
	JSON build:
	
	"""
	
	
	#Build json#
	
	cycleDataJson = {}
	
	
	cycleDataJson["productionOrder"] = 		productionOrder
	cycleDataJson["cycleNumber"] = 			cycleNumber
	cycleDataJson["cycleStart"] = 			system.date.format(startCycleDateTime, "yyyy-MM-dd HH:mm:ss")
	cycleDataJson["cycleStop"] = 			system.date.format(endCycleDateTime, "yyyy-MM-dd HH:mm:ss")
	cycleDataJson["cycleDuration"] = 		system.date.millisBetween(startCycleDateTime, endCycleDateTime) / 1000.0
	cycleDataJson["additionalCycleData"] = 	additionalCycleData
	
	cycleDatajson = system.util.jsonEncode(cycleDataJson)
	
	#Write tag here
	system.tag.writeBlocking([ignitionTagPath + "/mes/broker/prod_cycleData"],[cycleDatajson])
   
  