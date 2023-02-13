def add_work_order(factory, work_order):
	"""
	Function for adding workorder to the database
	
	Parameters
	----------
	factory : str
		Factory ERP number or name for which workorder was issued for
	work_order : str
		Workorder ERP number
	
	Returns
	-------
	int
		1 if the work_order was added, 0 if it was not.
	"""
	
	workorder_check = system.db.runPrepQuery(query="SELECT id FROM workorder WHERE number = ?", args= [work_order], database="factory_production")
	
	if len(workorder_check) > 0:
		#$raise TypeError("Work order with number %s is already added" % str(work_order))
		return ""
	factory_ds = system.db.runPrepQuery(query="SELECT id FROM factory WHERE name = ? or code = ?", args=[factory, factory], database="factory_production")
	
	if len(factory_ds) == 0:
		raise TypeError("Factory with name or code %s does not exists in the database" % str(factory)) 
	
	work_order_ds = system.db.runPrepQuery(query="SELECT jsonString->'$.*' FROM sap_idocs_received WHERE v_aufnr = ? order by id limit 1", args=[work_order], database="factory_sap")
	
	if len(work_order_ds) == 0:
		raise TypeError("Workorder with that name %s does not exists in the database" % str(work_order))
	
	work_order_json = system.util.jsonDecode(work_order_ds[0][0])
	
	work_order_header = work_order_json[0]["IDOC"]["E1AFKOL"]
	main_material = work_order_header["MATNR"]
	
	factory_id = factory_ds[0][0]
	
	
	check = system.db.runPrepUpdate(query="INSERT INTO workorder (factory_id, number, main_material_nb, status, erp_workorder) VALUES (?, ? ,? ,?, ?)", args=[factory_id, work_order, main_material, 1, system.util.jsonEncode(work_order_json[0])], database="factory_production")
	
	return check 
	
def append_production_data(workcenter, workorder, step, piece_data, piece_number, piece_end_time, rejects=[]):
	"""
	Function for modifying the piece data in database. 
	WARNING: If same key in piece_data it overrides existing key in step_result.
	
	Parameters
	----------
	workcenter : str
		Workcenter where piece is worked on
	workorder : str
		Workorder from ERP which piece came from
	step : str
		ERP identifier for step eg. "0050"
	piece_data : dict
		Data you want to save for this piece.
	piece_number : int
		Piece identifier.
	piece_end_time : java.date
		Operation end on piece
	rejects : list,dict, optional
		If singular piece reject use following structure ["reject_code"] eg. ["TT50"]
		If batch pieces reject use following structure {"reject_code":number_of_piece_rejected} eg. {"TT50":100}
		Warning: You can not unreject the piece or change the reject code.
	"""
	reject_number = 0
	# Sanity checks	
	#if piece_number <= 0:
	#	raise TypeError("Piece number needs to be over 0" % str(piece_number))
	
	# Get foreign keys from database
	workorder_id, factory_id  = get_workorder_id(workorder)		
	workcenter_id = get_workcenter_id(workcenter, factory_id)
	
	check_piece_number = system.db.runPrepQuery(query="SELECT id, data, quantity, rejects FROM step_result WHERE id=? AND step_id=? AND workorder_id=?", args=[piece_number, step, workorder_id], database="factory_production")


	if len(check_piece_number) <= 0:
		 raise TypeError("This piece number / batch number does not exists. Use add_production_data function")
	
	piece_id = check_piece_number[0][0]
	
	piece_number_json = system.util.jsonDecode(check_piece_number[0][1])
	piece_quantity = check_piece_number[0][2]
	piece_rejects = check_piece_number[0][3]
	
	
	# Single piece reject handling	
	if piece_quantity == 1:
		if piece_rejects and len(rejects) > 0:
			raise TypeError("This piece is already rejected.")
		elif len(rejects) > 0:
			reject_number = 1
	# Batch pieces reject handling 
	if piece_quantity > 1:
		# TODO MAKE BETTER REJECT HANDLING FOR BATCH PIECE
		if len(rejects) > 0:
			if isinstance(rejects, dict):
				reject_number = sum(rejects[x] for x in rejects)
			else:
				raise TypeError("Reject parameter needs to be a Dict type")
	
	
	get_step = system.db.runPrepQuery(query="SELECT data FROM step WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[step, workorder_id, workcenter_id], database="factory_production")
	
	if len(get_step) <= 0: 
		raise TypeError("step does not exists.")
	
	
	# Modify the step table to match data
	step_json = system.util.jsonDecode(get_step[0][0])


	dateparser = shared.utils.date.date_time_iso_8061_parser()
	
	start_date = dateparser.parse(step_json["start"])
	end_date = dateparser.parse(step_json["end"])
	

	previous_duration = system.date.millisBetween(start_date, end_date)
	step_json["duration"] = step_json["duration"] - previous_duration + system.date.millisBetween(start_date, piece_end_time)
	step_json["rejects"] = step_json["rejects"] + reject_number
	
	# check if piece end time is after end of step
	if system.date.isAfter(piece_end_time, end_date):
		step_json["end"] = dateparser.format(piece_end_time)
	step_check = system.db.runPrepUpdate(query="UPDATE step SET data = ? WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[system.util.jsonEncode(step_json), step, workorder_id, workcenter_id], database="factory_production")		
	piece_start = piece_number_json["start"]
	piece_number_json.update(piece_data)
	piece_number_json["start"] = piece_start
	piece_number_json["end"] = dateparser.format(piece_end_time)
	
	
	if reject_number == 0:
		step_results_sql_args = [system.util.jsonEncode(piece_number_json), None, piece_number, step, workorder_id]
	else:
		step_results_sql_args = [system.util.jsonEncode(piece_number_json), system.util.jsonEncode(rejects), piece_number, step, workorder_id,]
	#print system.util.jsonEncode(piece_number_json)
	#print step_results_sql_args
	step_result_check = system.db.runPrepUpdate(query="UPDATE step_result SET data = ?, rejects = ? WHERE id = ? AND step_id = ? AND workorder_id = ?", args=step_results_sql_args, database="factory_production")
	
	
	

def add_production_data(workcenter, workorder, step, piece_data, piece_number, piece_start_time, piece_end_time, operator, material_number,rejects=[], step_description="", unit="PCE", piece_quantity=1):
	"""
	Function for adding piece data to database
	
	Parameters
	----------
	workcenter : str
		Workcenter where piece was worked on
	workorder : str
		Workorder from ERP which piece came from
	step : str
		ERP identifier for step eg. "0050"
	piece_data : dict
		Data you want to save for this piece.
	piece_number : int
		Piece identifier.
	piece_start_time : java.date
		Operation start on piece
	piece_end_time : java.date
		Operation end on piece
	operator : str
		User that added data to the database. Use system.security.getUsername()
	material_number : str
		Material number for piece. May be diffrent from main material number (mixing...)
	rejects : list,dict, optional
		If singular piece reject use following structure ["reject_code"] eg. ["TT50"]
		If batch pieces reject use following structure {"reject_code":number_of_piece_rejected} eg. {"TT50":100}
	step_description : str, optional
		Add step description if needed (Only 255 characters)
	units : str, optional
		If piece units are in diffrent format change this. 
		Default "PCE"
	piece_quantity : int, optional
		Change this argument to number of pieces for this data. Used for batching. 
		Default is one piece (1)
		
	Raises
	------
	TypeError	
	"""
	reject_number = 0
	
	# Sanity checks	
	#if piece_number <= 0:
	#	raise TypeError("Piece number needs to be over 0" % str(piece_number))
	
	# Get foreign keys from database
	workorder_id, factory_id  = get_workorder_id(workorder)		
	workcenter_id = get_workcenter_id(workcenter, factory_id)
	
	check_piece_number = system.db.runPrepQuery(query="SELECT id FROM step_result WHERE id=? AND step_id=? AND workorder_id=?", args=[piece_number, step, workorder_id], database="factory_production")

	if len(check_piece_number) > 0:
		raise TypeError("This piece number / batch number already exists for this specific workorder.")
		#raise TypeError("This piece number / batch number already exists for this specific workorder '%s' and step '%s'." % str(workorder), str(step))
	
	#if piece_quantity < 0:
	#	raise TypeError("Quantity must be over 0")
	# Single piece reject handling	
	if piece_quantity == 1:
		if len(rejects) > 0:
			reject_number = 1
	
	# Batch pieces reject handling 
	if piece_quantity > 1 or piece_quantity == 0:
		if len(rejects) > 0:
			if isinstance(rejects, dict):
				reject_number = sum(rejects[x] for x in rejects)
			else:
				raise TypeError("Reject parameter needs to be a Dict type")
				
	
	check_step = system.db.runPrepQuery(query="SELECT data FROM step WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[step, workorder_id, workcenter_id], database="factory_production")
	
	dateparser = shared.utils.date.date_time_iso_8061_parser()
	
	# Check if step exist
	if len(check_step) == 0:
		# Automatic duration calculation
		duration = system.date.millisBetween(piece_start_time, piece_end_time)
		step_json = {"start": dateparser.format(piece_start_time), 
					 "end": dateparser.format(piece_end_time), 
					 "duration":duration, 
					 "outfeed": piece_quantity, 
					 "rejects": reject_number}
		
		system.db.runPrepUpdate(query="INSERT INTO step (id, workorder_id, workcenter_id, data) VALUES (?, ?, ?, ?)", args=[step, workorder_id, workcenter_id, system.util.jsonEncode(step_json)], database="factory_production")
	else:
	 	# Update data json field in step with new data
		database_step_json = system.util.jsonDecode(check_step[0][0])
		duration = database_step_json["duration"] + system.date.millisBetween(piece_start_time, piece_end_time)
		outfeed = database_step_json["outfeed"] + piece_quantity
		rejects_number1 = database_step_json["rejects"] + reject_number
		start_time = database_step_json["start"]
		step_json = {"start": start_time,
		 			 "end": dateparser.format(piece_end_time), 
		 			 "duration":duration,
		 			 "outfeed":outfeed, 
		 			 "rejects":rejects_number1}
		
		system.db.runPrepUpdate(query="UPDATE step SET data=? WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[system.util.jsonEncode(step_json), step, workorder_id, workcenter_id], database="factory_production")
	
	
	# Add dates in piece_data
	piece_data["start"] = dateparser.format(piece_start_time)
	piece_data["end"] = dateparser.format(piece_end_time)
 	
 	if reject_number == 0:
		step_results_sql_args = [piece_number, step, workorder_id, piece_quantity, operator, material_number, unit, system.util.jsonEncode(piece_data), None]
	else:
		step_results_sql_args = [piece_number, step, workorder_id, piece_quantity, operator, material_number, unit, system.util.jsonEncode(piece_data), system.util.jsonEncode(rejects)]
	system.db.runPrepUpdate(query="INSERT INTO step_result (id, step_id, workorder_id, quantity, operator, material_number, unit, data, rejects) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", args=step_results_sql_args, database="factory_production")
	
	


def get_workcenter_id(workcenter_code, factory_id):
	"""
	Function to get internal key for workcenter id
	
	Parameters
	----------
	workcenter_code : str
		Workcenter ERP code
	factory_id : int 
		factory internal key identification
		
	Returns
	-------
	int
		Returns internal key for workcenter
		
	Raises
	------
	TypeError
		if workcenter does not exists in the system
		
	"""
	workcenter_check = system.db.runPrepQuery(query="SELECT id FROM workcenter WHERE code = ? AND factory_id = ?", args=[workcenter_code, factory_id], database="factory_production")
	
	if len(workcenter_check) == 0:
		raise TypeError("Work center with code %s does not exist" % str(workcenter_code))
		
	return workcenter_check[0][0]
	
def get_workorder_id(workorder_number):
	"""
	Function to get internal key for workorder
	
	Parameters
	----------
	workorder_number : str
		Workorder ERP number
	
	Returns
	-------
	list
		Returns a list of internal workorder id and internal factory id
	
	Raises
	------
	TypeError
		if work order number does not exists in database
	"""
	
	workorder_check = system.db.runPrepQuery(query="SELECT id, factory_id FROM workorder WHERE number = ?", args= [workorder_number], database="factory_production")
	
	if len(workorder_check) == 0:
		raise TypeError("Work order with number %s does not exist" % str(workorder_number))
		
	return workorder_check[0]
	
	
	
def add_confirmation_data(workcenter, workorder, step, confJson, batchNumber):
	"""
	Function for adding confirmation data to database
	
	Parameters
	----------
	workcenter : str
		Workcenter where confirmation was done
	workorder : str
		Workorder from ERP which was confirmad
	step : str
		ERP identifier for step eg. "0050" on which confirmation was done
	confJson : str
		confirmation json with all the data
		
	Raises
	------
	TypeError	
	"""
	
	# Get foreign keys from database
	workorder_id, factory_id  = get_workorder_id(workorder)		
	workcenter_id = get_workcenter_id(workcenter, factory_id)
	
			
	if batchNumber:
		check_step_result = system.db.runPrepQuery(query="SELECT data FROM step_result WHERE step_id = ? AND workorder_id = ? AND id = ?", args=[step, workorder_id, batchNumber], database="factory_production")
			
		dateparser = shared.utils.date.date_time_iso_8061_parser()

		# Check if step exist
		if len(check_step_result) == 0:
			return system.db.runPrepUpdate(query="INSERT INTO step_result (id, step_id,workorder_id, erp_confirmation) VALUES (?, ?, ?, ?)", args=[batchNumber, step, workorder_id, system.util.jsonEncode(confJson)], database="factory_production")
		else:
			# Update data json field in step with new data
			return system.db.runPrepUpdate(query="UPDATE step_result SET erp_confirmation=? WHERE id = ? AND step_id=? AND workorder_id = ?", args=[system.util.jsonEncode(confJson), batchNumber,step, workorder_id], database="factory_production")			
	
	else:
		check_step = system.db.runPrepQuery(query="SELECT data FROM step WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[step, workorder_id, workcenter_id], database="factory_production")
		
		dateparser = shared.utils.date.date_time_iso_8061_parser()
		
		# Check if step exist
		if len(check_step) == 0:
			returnsystem.db.runPrepUpdate(query="INSERT INTO step (id, workorder_id, workcenter_id, erp_confirmation) VALUES (?, ?, ?, ?)", args=[step, workorder_id, workcenter_id, system.util.jsonEncode(confJson)], database="factory_production")
		else:
			# Update data json field in step with new data
			return system.db.runPrepUpdate(query="UPDATE step SET erp_confirmation=? WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[system.util.jsonEncode(confJson), step, workorder_id, workcenter_id], database="factory_production")
			
				
				
def add_cancelation_data(workcenter, workorder, step, cancelJson, batchNumber):
	"""
	Function for adding cancelation data to database
	
	Parameters
	----------
	workcenter : str
		Workcenter where confirmation was done
	workorder : str
		Workorder from ERP which was confirmad
	step : str
		ERP identifier for step eg. "0050" on which confirmation was done
	cancelJson : str
		cancelation json with all the data
		
	Raises
	------
	TypeError	
	"""
	
	# Get foreign keys from database
	workorder_id, factory_id  = get_workorder_id(workorder)		
	workcenter_id = get_workcenter_id(workcenter, factory_id)
	
			
	if batchNumber:
		check_step_result = system.db.runPrepQuery(query="SELECT data FROM step_result WHERE step_id = ? AND workorder_id = ? AND id = ?", args=[step, workorder_id, batchNumber], database="factory_production")
			
		dateparser = shared.utils.date.date_time_iso_8061_parser()

		# Check if step exist
		if len(check_step_result) == 0:
			return system.db.runPrepUpdate(query="INSERT INTO step_result (id, step_id,workorder_id, erp_cancel) VALUES (?, ?, ?, ?)", args=[batchNumber, step, workorder_id, system.util.jsonEncode(cancelJson)], database="factory_production")
		else:
			# Update data json field in step with new data
			return system.db.runPrepUpdate(query="UPDATE step_result SET erp_cancel=? WHERE id = ? AND step_id=? AND workorder_id = ?", args=[system.util.jsonEncode(cancelJson), batchNumber,step, workorder_id], database="factory_production")			
	
	else:
		check_step = system.db.runPrepQuery(query="SELECT data FROM step WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[step, workorder_id, workcenter_id], database="factory_production")
		
		dateparser = shared.utils.date.date_time_iso_8061_parser()
		
		# Check if step exist
		if len(check_step) == 0:
			returnsystem.db.runPrepUpdate(query="INSERT INTO step (id, workorder_id, workcenter_id, erp_cancel) VALUES (?, ?, ?, ?)", args=[step, workorder_id, workcenter_id, system.util.jsonEncode(cancelJson)], database="factory_production")
		else:
			# Update data json field in step with new data
			return system.db.runPrepUpdate(query="UPDATE step SET erp_cancel=? WHERE id = ? AND workorder_id = ? AND workcenter_id = ?", args=[system.util.jsonEncode(cancelJson), step, workorder_id, workcenter_id], database="factory_production")