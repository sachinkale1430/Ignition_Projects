def getProductionOrder(productionOrder):
	"""
	Get latest production order from the sap_idocs_received table
	
	Parameters
	----------
	productionOrder : str
		  Production order number from SAP (AUFNR)
	
	Returns
	-------
	str	
		Returns json string or an error message
	"""
	sql = """
		SELECT jsonString
		FROM sap_idocs_received 
		WHERE id = ( 
			SELECT max(id) FROM sap_idocs_received 
			where v_aufnr = ?
			group by v_aufnr)
	"""
	if productionOrder:
		if isinstance(productionOrder, basestring):
			productionOrder = productionOrder.lstrip("0")
		else:
			productionOrder = str(productionOrder)
		
		json = system.db.runScalarPrepQuery(sql, args=[productionOrder], database="factory_sap")
		
		if json:
			json = system.util.jsonDecode(json)
			return system.util.jsonEncode(json,3)
		else:
			return "No such production order available in factory."
		
	
	return "Please specify production order number (AUFNR in SAP)"
				
	
	
def getProductionOrderHistory(productionOrder, showDetails=False):
	"""
	Get all idocs for the particular production order from the sap_idocs_received table
	
	Parameters
	----------
	productionOrder : str
		Production order number from SAP (AUFNR)
	showDetails: bool
		if true, also return raw data of idocs. 
		Warning: It could be large depending on production order.
	
	Returns
	-------
	List	
		Returns list of all idocs for this particular production order
	"""
	
	sql = """
		SELECT iDocNumber iDoc, jsonString json, timestamp, jsonString->"$.ZLOIPRO04.IDOC.E1AFKOL.E1JSTKL[*].STAT" status
		FROM sap_idocs_received 
		WHERE v_aufnr = ?
		order by id
	"""
	
	sqlNoDetails ="""
			SELECT iDocNumber iDoc, timestamp, jsonString->"$.ZLOIPRO04.IDOC.E1AFKOL.E1JSTKL[*].STAT" status
			FROM sap_idocs_received 
			WHERE v_aufnr = ?
			order by id
		"""
	
	
	if productionOrder:
		if isinstance(productionOrder, basestring):
			productionOrder = productionOrder.lstrip("0")
		else:
			productionOrder = str(productionOrder)
		
		if showDetails:
			data = system.db.runPrepQuery(sql, args=[productionOrder], database="factory_sap")
		else:
			data = system.db.runPrepQuery(sqlNoDetails, args=[productionOrder], database="factory_sap")
		
		if len(data) > 0:
			json = []
			for row in data:
				if showDetails:
					tmpJson = {"timestamp":row["timestamp"], 
							   "iDocNumber":row["iDoc"], 
							   "jsonString":system.util.jsonDecode(row["json"]),
							   "status":system.util.jsonDecode(row["status"])}
				else:
					tmpJson = {"timestamp":row["timestamp"], 
											   "iDocNumber":row["iDoc"], 
											   "status":system.util.jsonDecode(row["status"])}
				json.append(tmpJson)
			return json
		else:
			return "No such production order available in factory."
		
	
	return "Please specify production order number (AUFNR in SAP)"
	
	
def getIdoc(idocNumber):
	# To be discussed...
	pass
	
	