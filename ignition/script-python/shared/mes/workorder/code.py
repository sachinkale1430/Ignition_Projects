#===============================================
#===============================================
#
# mes.workorder.*
#
# Functions in relation with MESWorkOrder management
#
# V1.2
# Change log:
# - updated function getStepMaterials added itemCategory (marks which material need to be confirmed to SAP)
# - updated function getSAPProductionStepDetails added controlKey (control key indicates goods movement to confirm)
#
# V1.3
# Change log:
# - updated function getStepMaterials for price information:
#   (priceUnit,movingPrice,currency,standardPrice)
#
# V1.4
# Change log:
# - updated function getStepMaterials for bachNumber
#
# V1.5
# Change log:
# - Updated function getWorkOrders to add managedByBatch if main material is managed by batch
#
# V1.6
# Change log:
# - Added 'order by' functionallity to getStepMaterials
# - Added short operation description to getSAPPproductionStepDetail
#
# V1.7
# Change log:
# - updated function getStepMaterials for backflush flag
#  
# V1.7
# Change log:
# - Added getWorkOrderAionData function to get Aion data.
# - Added aionFields to the getSAPProductionStepDetail fields 
#
# V1.8
# Change log:
# - Modifiction of stepMaterials and getSAPProductionStepDetails with ISO UOM
# - Modification of data retrival for materialDescription and measureUnits
#===============================================
#===============================================


#==========================================================
# this fuction will get ENABLED Work orders from MES
# Example:
# Input parameter: searchText
# Output parameter: array of enabled work orders
#==========================================================
def getOrdersByText(searchText):
	querystr = 'SELECT mwo.Name workOrder FROM pfce_MESJsonProperty mj LEFT JOIN MESProperty mp ON (mj.MESPropertyUUID=mp.MESPropertyUUID) LEFT JOIN MESWorkOrder mwo ON (mwo.MESWorkOrderUUID=mp.MESObjectUUID) WHERE mj.jsonString->"$.E1AFLTH[*].E1AFLTP[*].TDLINE" like ?'
	text = str('%"'+searchText+'"%')
	result = system.db.runPrepQuery (query=querystr,args=[text], database="mes_analysis")
	return result


#==========================================================
# Returns key SAP data from MES PFCE table
# All parameters are optional
# No param = all WO, all fields
#==========================================================
def getWorkOrders( workOrderName=None, workOrderNameFilter=None, equipmentPathFilter=None, materialNameFilter=None, fields=None):

	# options
	options={}
	options["name"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.AUFNR" AS CHAR CHARACTER SET utf8))"""
	#options["description"]= """TRIM(BOTH '"' FROM CAST(jsonString->"$.E1AFLTH[*].E1AFLTP[*].TDLINE" AS CHAR CHARACTER SET utf8))"""
	options["description"]= """IFNULL(jsonString->"$.E1AFLTH[*].E1AFLTP[*].TDLINE", "[]") """
	options["quantity"] = 	"""CAST(jsonString->"$.BMENGE" AS DECIMAL)"""
	options["units"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.BMEINS" AS CHAR CHARACTER SET utf8))"""
	options["material"] = 	"""TRIM(BOTH '"' FROM CAST(jsonString->"$.MATNR" AS CHAR CHARACTER SET utf8))"""
	options["materialDescription"] = """IFNULL(CAST(JSON_UNQUOTE(jsonString->"$.MATNR_EXTERNAL") AS CHAR CHARACTER SET utf8), "")"""
	options["type"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.AUART" AS CHAR CHARACTER SET utf8))"""
	options["category"] = 	"""TRIM(BOTH '"' FROM CAST(jsonString->"$.AUTYP" AS CHAR CHARACTER SET utf8))"""
	options["dueDate"] = 	"""TRIM(BOTH '"' FROM CAST(jsonString->"$.GLTRS" AS CHAR CHARACTER SET utf8))"""
	options["plant"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.WERKS" AS CHAR CHARACTER SET utf8))"""

	# Needs some codification
	options['priorityofVacancyAssignment'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.APRIO' AS CHAR CHARACTER SET utf8))"""
	options['dateofBOMExplosionRoutingTransfer'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.AUFLD' AS CHAR CHARACTER SET utf8))"""
	options['seqNumberOrder'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.CY_SEQNR' AS CHAR CHARACTER SET utf8))"""
	options['mRPcontrollerfortheorder'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.DISPO' AS CHAR CHARACTER SET utf8))"""
	options['productionSupervisor'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.FEVOR' AS CHAR CHARACTER SET utf8))"""
	options['schedulingMarginKeyforFloats'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.FHORI' AS CHAR CHARACTER SET utf8))"""
	options['releaseperiod'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.FREIZ' AS CHAR CHARACTER SET utf8))"""
	options['actualReleaseDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.FTRMI' AS CHAR CHARACTER SET utf8))"""
	options['scheduledReleaseDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.FTRMS' AS CHAR CHARACTER SET utf8))"""
	options['totalOrderQuantity'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GAMNG' AS CHAR CHARACTER SET utf8))"""
	options['totalScrapQuantityInTheOrder'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GASMG' AS CHAR CHARACTER SET utf8))"""
	options['confirmedOrderFinishDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GETRI' AS CHAR CHARACTER SET utf8))"""
	options['confirmedOrderFinish'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GEUZI' AS CHAR CHARACTER SET utf8))"""
	options['actualFinishDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GLTRI' AS CHAR CHARACTER SET utf8))"""
	options['basicFinishDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GLTRP' AS CHAR CHARACTER SET utf8))"""
	options['basicFinishTime'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GLUZP' AS CHAR CHARACTER SET utf8))"""
	options['scheduledFinishTime'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GLUZS' AS CHAR CHARACTER SET utf8))"""
	options['unitOfMeasure'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GMEIN' AS CHAR CHARACTER SET utf8))"""
	options['actualStartDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSTRI' AS CHAR CHARACTER SET utf8))"""
	options['basicStartDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSTRP' AS CHAR CHARACTER SET utf8))"""
	options['scheduledStartDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSTRS' AS CHAR CHARACTER SET utf8))"""
	options['basicStartTime'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSUZP' AS CHAR CHARACTER SET utf8))"""
	options['scheduledStartTime'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSUZS' AS CHAR CHARACTER SET utf8))"""
	options['scrapConfirmedForOrder'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.IASMG' AS CHAR CHARACTER SET utf8))"""
	options['yieldConfirmedFromOrderConfirmation'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.IGMNG' AS CHAR CHARACTER SET utf8))"""
	options['itemNumberInSalesOrder'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.KDPOS' AS CHAR CHARACTER SET utf8))"""
	options['dateForRoutingTransfer'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLAUF' AS CHAR CHARACTER SET utf8))"""
	options['groupCounter'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLNAL' AS CHAR CHARACTER SET utf8))"""
	options['taskListUnitOfMeasure'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLNME' AS CHAR CHARACTER SET utf8))"""
	options['keyforTaskListGroup'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLNNR' AS CHAR CHARACTER SET utf8))"""
	options['taskListType'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLNTY' AS CHAR CHARACTER SET utf8))"""
	options['toLotSize'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLSVB' AS CHAR CHARACTER SET utf8))"""
	options['fromLotSize'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PLSVN' AS CHAR CHARACTER SET utf8))"""
	options['workBreakdownStructureElement'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.PSPEL' AS CHAR CHARACTER SET utf8))"""
	options['totalConfirmedReworkQuantity'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.RMNGA' AS CHAR CHARACTER SET utf8))"""
	options['alternativeBOM'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.STLAL' AS CHAR CHARACTER SET utf8))"""
	options['bOMUsage'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.STLAN' AS CHAR CHARACTER SET utf8))"""
	options['billOfMaterial'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.STLNR' AS CHAR CHARACTER SET utf8))"""
	options['schedulingType'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.TERKZ' AS CHAR CHARACTER SET utf8))"""
	options['floatBeforeProduction'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.VORGZ' AS CHAR CHARACTER SET utf8))"""
	options['storage'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.E1AFPOL.LGORT' AS CHAR CHARACTER SET utf8))"""
	options['managedByBatch'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.ZE1AFKOL.XCHPF' AS CHAR CHARACTER SET utf8))"""
	
	options['confirmationISOUnits'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.ZE1AFKOL.H_ISOCODE_UM' AS CHAR CHARACTER SET utf8))"""
	options['materialInstance'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.ZE1AFKOL.INSTANCE' AS CHAR CHARACTER SET utf8))"""
	options['productGroup'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.ZE1AFKOL.SATNR' AS CHAR CHARACTER SET utf8))"""
	

	# BUILD SELECT
	#===================

	# If no specific field is required, get all of them
	if fields==None:
		fields = options.keys()
	else:
		# Check that there is no mistake in field requests
		for field in fields:
			if field not in options.keys():
				raise ValueError("getWorkOrders: syntax error in the field name : " + field)

	# Build select clause
	myList = []
	for field in fields:
		# Add "clause" "space" "name of column"
		if field in options.keys():
			myList.append( options[field] + " " + field )
	selectClause = ",".join( myList )

	# Now WORK-ORDERS
	#====================

	# If there is a specific WO requested
	if workOrderName:
		if isinstance(workOrderName, basestring):
			inClause = "=\"" + workOrderName + "\""
		else:
			inClause = ' in (' + ",".join(workOrderName) + ')'
	else:
		# Create filter
		filter = system.mes.workorder.createMESWorkOrderFilter()
		# Optionnaly add a filter on workOrder name
		if workOrderNameFilter:
			filter.setWorkOrderNameFilter(workOrderNameFilter)
		# Optionnaly add a filter on equipmentPath
		if equipmentPathFilter:
			filter.setEquipmentPathFilter(equipmentPathFilter)
		# Optionnaly add a filter on material name
		if materialNameFilter:
			filter.setMaterialNameFilter(materialNameFilter)
		# Retrieve list of work-orders links from filter
		workOrders = system.mes.workorder.getMESWorkOrderObjectLinkList(filter)
		# Make it a list of workOrder "names"
		list_workOrders = [row.getName() for row in workOrders]
		inClause = ' in (' + ",".join(list_workOrders) + ')'

	# Retrieve data
	sql_query = "SELECT " + selectClause + """ FROM pfce_MESJsonProperty where jsonString->"$.AUFNR" """ + inClause
	dataset = system.db.runPrepQuery(sql_query, database="mes_analysis")

	return dataset

#==========================================================
# Returns production step data from MES PFCE table
# workOrderName mandatory
# other parameters not mandatory

# delete after testing
#==========================================================
def getProductionStep_old(workOrderName=None, workCenter=None, operationNumber=None, fields=None):

	# options
	options={}
	options["operationNumber"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) AS CHAR CHARACTER SET utf8))"""
	options["stepDescription"]= 	"""IFNULL(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL_TX[*].TDLINE')), "[]") """
	options["workCenter"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) AS CHAR CHARACTER SET utf8))"""
	options["operationQuantity"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].MGVRG')) AS CHAR CHARACTER SET utf8))"""
	options["materialDescription"] = """IFNULL(TRIM(BOTH '"' FROM CAST(jsonString->"$.MATNR_EXTERNAL" AS CHAR CHARACTER SET utf8)), "") """
	options["operationStatus"] =	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].STAT')) AS CHAR CHARACTER SET utf8))"""

	options["shortText"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSADV')) AS CHAR CHARACTER SET utf8))"""
	options["earliestStartDate"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSADV')) AS CHAR CHARACTER SET utf8))"""
	options["earliestStartTime"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSAVZ')) AS CHAR CHARACTER SET utf8))"""
	options["earliestFinishDate"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSEDD')) AS CHAR CHARACTER SET utf8))"""
	options["earliestFinishTime"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSEDZ')) AS CHAR CHARACTER SET utf8))"""
	options["confirmationNumber"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL.ZZRUECK')) AS CHAR CHARACTER SET utf8))"""
	options["processingTime"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].BEARZ')) AS CHAR CHARACTER SET utf8))"""
	options["processingTimeUnit"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].BEAZE')) AS CHAR CHARACTER SET utf8))"""
	options["waitTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].LIEGZ')) AS CHAR CHARACTER SET utf8))"""
	options["waitTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].LIGZE')) AS CHAR CHARACTER SET utf8))"""
	options["setupTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].RUEST')) AS CHAR CHARACTER SET utf8))"""
	options["setupTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].RSTZE')) AS CHAR CHARACTER SET utf8))"""
	options["interoperationTime"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].TRANZ')) AS CHAR CHARACTER SET utf8))"""
	options["queueTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].WARTZ')) AS CHAR CHARACTER SET utf8))"""
	options["queueTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].WRTEZ')) AS CHAR CHARACTER SET utf8))"""

	# BUILD SELECT
	#===================

	# If no specific field is required, get all of them
	if fields==None:
		fields = options.keys()
	else:
		# Check that there is no mistake in field requests
		for field in fields:
			if field not in options.keys():
				raise ValueError("getWorkOrders: syntax error in the field name : " + field)

	# Build select clause
	myList = []
	for field in fields:
		# Add "clause" "space" "name of column"
		if field in options.keys():
			myList.append( options[field] + " " + field )
	selectClause = ",".join( myList )

	inClause = ""
	if workCenter:
		inClause = inClause + """AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) = """ +"\""+ workCenter + "\""
	if operationNumber:
		inClause = inClause + """AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) = """ +"\""+ operationNumber + "\""

	# Retrieve data

	sql_query = " SELECT "+ selectClause + """

					FROM pfce_MESJsonProperty
					JOIN (
						SELECT  0 AS idx UNION
						SELECT  1 AS idx UNION
						SELECT  2 AS idx UNION
						SELECT  3 AS idx UNION
						SELECT  4 AS idx UNION
						SELECT  5 AS idx UNION
						SELECT  6 AS idx UNION
						SELECT  7 AS idx UNION
						SELECT  8 AS idx UNION
						SELECT  9 AS idx UNION
						SELECT  10 AS idx UNION
						SELECT  11 AS idx UNION
						SELECT  12 AS idx UNION
						SELECT  13 AS idx UNION
						SELECT  14 AS idx UNION
						SELECT  15 AS idx UNION
						SELECT  16 AS idx UNION
						SELECT  17 AS idx UNION
						SELECT  18 AS idx UNION
						SELECT  19 AS idx UNION
						SELECT  20 AS idx UNION
						SELECT  21 AS idx UNION
						SELECT  22 AS idx UNION
						SELECT  23 AS idx UNION
						SELECT  24 AS idx UNION
						SELECT  25 AS idx UNION
						SELECT  26 AS idx UNION
						SELECT  27 AS idx UNION
						SELECT  28 AS idx UNION
						SELECT  29 AS idx UNION
						SELECT  30 AS idx UNION
					  	SELECT  31
					  ) AS indexes
						WHERE jsonString->"$.AUFNR" = ? AND
							JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, ']')) IS NOT NULL """ + inClause


	dataset = system.db.runPrepQuery(query=sql_query,args=[workOrderName], database="mes_analysis")
	return dataset

#==========================================================
# Returns production step data from MES PFCE table
# workOrderName not mandatory searching with like
# workCenter - ARBPL - not mandatory
# operationNumber - VORNR - not mandatory
# fields - fields that we want to have
# order - custom order else default no order
# limit - custom limit
#==========================================================
def getSAPProductionStepDetails(workOrderName=None, workCenter=None, operationNumber=None, fields=None, order=None, limit=None):

	# options
	options={}
	options["workOrderName"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.AUFNR" AS CHAR CHARACTER SET utf8))"""
	options["operationNumber"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) AS CHAR CHARACTER SET utf8))"""
	options["stepDescription"]= 	"""IFNULL(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL_TX[*].TDLINE')), "[]") """
	options["workCenter"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) AS CHAR CHARACTER SET utf8))"""
	options["operationQuantity"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].MGVRG')) AS CHAR CHARACTER SET utf8))"""
	options["operationUnits"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].MEINH')) AS CHAR CHARACTER SET utf8))"""
	options["material"] = 	"""TRIM(BOTH '"' FROM CAST(jsonString->"$.MATNR" AS CHAR CHARACTER SET utf8))"""
	## Changed by 2021-02-15 Patrik Pusnik SDSGA-3966 
	#options["materialDescription"] = """IFNULL(TRIM(BOTH '"' FROM CAST(jsonString->"$.MATNR_EXTERNAL" AS CHAR CHARACTER SET utf8)), "") """
	options["materialDescription"] = """IFNULL(CAST(JSON_UNQUOTE(jsonString->"$.MATNR_EXTERNAL") AS CHAR CHARACTER SET utf8), "")"""
	## Changed by Izi&Patrik - Synchornizing all instances
	#options["operationStatus"] =	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].STAT')) AS CHAR CHARACTER SET utf8))"""
	options["operationStatus"] =	"""IFNULL(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1JSTVL[*].STAT')), "[]") """
	options["shortText"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].LTXA1')) AS CHAR CHARACTER SET utf8))"""
	options["earliestStartDate"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSADV')) AS CHAR CHARACTER SET utf8))"""
	options["earliestStartTime"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSAVZ')) AS CHAR CHARACTER SET utf8))"""
	options["earliestFinishDate"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSEDD')) AS CHAR CHARACTER SET utf8))"""
	options["earliestFinishTime"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].FSEDZ')) AS CHAR CHARACTER SET utf8))"""
	options["confirmationNumber"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL.ZZRUECK')) AS CHAR CHARACTER SET utf8))"""
	options["processingTime"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].BEARZ')) AS CHAR CHARACTER SET utf8))"""
	options["processingTimeUnit"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].BEAZE')) AS CHAR CHARACTER SET utf8))"""
	options["waitTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].LIEGZ')) AS CHAR CHARACTER SET utf8))"""
	options["waitTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].LIGZE')) AS CHAR CHARACTER SET utf8))"""
	
	options["setupTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGW01')) AS CHAR CHARACTER SET utf8))"""
	options["setupTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGE01')) AS CHAR CHARACTER SET utf8))"""
	options["machineTime"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGW02')) AS CHAR CHARACTER SET utf8))"""
	options["machineTimeUnit"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGE02')) AS CHAR CHARACTER SET utf8))"""
	options["laborTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGW03')) AS CHAR CHARACTER SET utf8))"""
	options["laborTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VGE03')) AS CHAR CHARACTER SET utf8))"""
	
	options["interoperationTime"] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].TRANZ')) AS CHAR CHARACTER SET utf8))"""
	options["queueTime"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].WARTZ')) AS CHAR CHARACTER SET utf8))"""
	options["queueTimeUnit"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].WRTEZ')) AS CHAR CHARACTER SET utf8))"""
	
	options["controlKey"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].STEUS')) AS CHAR CHARACTER SET utf8))"""
	options["units"] = 		"""TRIM(BOTH '"' FROM CAST(jsonString->"$.BMEINS" AS CHAR CHARACTER SET utf8))"""
	options['basicFinishDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GLTRP' AS CHAR CHARACTER SET utf8))"""
	options['basicStartDate'] = """TRIM(BOTH '"' FROM CAST(jsonString->'$.GSTRP' AS CHAR CHARACTER SET utf8))"""
	options["dueDate"] = 	"""TRIM(BOTH '"' FROM CAST(jsonString->"$.GLTRS" AS CHAR CHARACTER SET utf8))"""
	options["quantity"] = 	"""CAST(jsonString->"$.BMENGE" AS DECIMAL)"""

	options['aionData'] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL_MES_DATA')) AS CHAR CHARACTER SET utf8))"""
	options['baseOperationQuantity'] = """TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].BMSCH')) AS CHAR CHARACTER SET utf8))"""
	
	
	
	# BUILD SELECT
	#===================

	# If no specific field is required, get all of them
	if fields==None:
		fields = options.keys()
	else:
		# Check that there is no mistake in field requests
		for field in fields:
			if field not in options.keys():
				raise ValueError("getWorkOrders: syntax error in the field name : " + field)

	# Build select clause
	myList = []
	for field in fields:
		# Add "clause" "space" "name of column"
		if field in options.keys():
			myList.append( options[field] + " " + field )
	selectClause = ",".join( myList )

	inClause = ""
	orderClause = ""
	limitClause = ""

	if workOrderName:
		inClause = inClause + """ AND jsonString->"$.AUFNR" = """ +"\""+ workOrderName + "\""
	if workCenter and operationNumber:
		inClause = inClause + """ AND (JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) = """ +"\""+ str(workCenter) + "\""
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) = """ +"\""+ operationNumber + "\")"
	elif workCenter:
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) = """ +"\""+ str(workCenter) + "\""
	elif operationNumber:
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) = """ +"\""+ operationNumber + "\""


	if order:
		orderClause = " ORDER BY "+order

	if limit:
		limitClause = " LIMIT "+str(limit)

	# Retrieve data

	sql_query = " SELECT "+ selectClause + """

					FROM pfce_MESJsonProperty
					JOIN (
						SELECT  0 AS idx UNION
						SELECT  1 AS idx UNION
						SELECT  2 AS idx UNION
						SELECT  3 AS idx UNION
						SELECT  4 AS idx UNION
						SELECT  5 AS idx UNION
						SELECT  6 AS idx UNION
						SELECT  7 AS idx UNION
						SELECT  8 AS idx UNION
						SELECT  9 AS idx UNION
						SELECT  10 AS idx UNION
						SELECT  11 AS idx UNION
						SELECT  12 AS idx UNION
						SELECT  13 AS idx UNION
						SELECT  14 AS idx UNION
						SELECT  15 AS idx UNION
						SELECT  16 AS idx UNION
						SELECT  17 AS idx UNION
						SELECT  18 AS idx UNION
						SELECT  19 AS idx UNION
						SELECT  20 AS idx UNION
						SELECT  21 AS idx UNION
						SELECT  22 AS idx UNION
						SELECT  23 AS idx UNION
						SELECT  24 AS idx UNION
						SELECT  25 AS idx UNION
						SELECT  26 AS idx UNION
						SELECT  27 AS idx UNION
						SELECT  28 AS idx UNION
						SELECT  29 AS idx UNION
						SELECT  30 AS idx UNION
						SELECT  31
					  ) AS indexes
						WHERE Enabled=1 AND
							JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, ']')) IS NOT NULL """ + inClause + orderClause + limitClause


	dataset = system.db.runPrepQuery(query=sql_query, database="mes_analysis")
	return dataset

#==========================================================
# Returns production step data that contains material consumption from MES PFCE table
# workOrderName mandatory - work order for wich we are checking the existance of the material
# other parameters not mandatory
#==========================================================
def getStepMaterials( workOrderName=None,workCenter=None, operationNumber=None, fields=None, order=None):

	# options
	options={}
	options["operationNumber"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) AS CHAR CHARACTER SET utf8))"""
	#options["stepDescription"]= 	"""IFNULL(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ZE1AFVOL_TX[*].TDLINE')), "[]") """
	options["workCenter"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) AS CHAR CHARACTER SET utf8))"""

	options["requiredQuantity"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].BDMNG')) AS CHAR CHARACTER SET utf8))"""
	## Changed by Patrik Pusnik 2021-02-15 SDSGA-3966
	#options["measureUnit"] =  	 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].MEINS')) AS CHAR CHARACTER SET utf8))"""
	options["measureUnit"] =  	 		"""CAST(JSON_UNQUOTE(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].MEINS'))) AS CHAR CHARACTER SET utf8)"""
	options["materialNumber"] =  	 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].MATNR')) AS CHAR CHARACTER SET utf8))"""
	## Changed by Patrik Pusnik 2021-02-15 SDSGA-3966
	#options["materialDescription"] =  	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].MATNR_EXTERNAL')) AS CHAR CHARACTER SET utf8))"""
	options["materialDescription"] =  	"""CAST(JSON_UNQUOTE(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].MATNR_EXTERNAL'))) AS CHAR CHARACTER SET utf8)"""
	options["consumMaterialStorage"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].LGORT')) AS CHAR CHARACTER SET utf8))"""
	options["itemCategory"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].POSTP')) AS CHAR CHARACTER SET utf8))"""
	options["mainMaterialStorage"] = 	"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFPOL.LGORT')) AS CHAR CHARACTER SET utf8))"""
	options["priceUnit"] = 				"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.PEINH')) AS CHAR CHARACTER SET utf8))"""
	options["movingPrice"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.VERPR')) AS CHAR CHARACTER SET utf8))"""
	options["currency"] = 				"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.WAERS')) AS CHAR CHARACTER SET utf8))"""
	options["standardPrice"] =			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.STPRS')) AS CHAR CHARACTER SET utf8))"""
	options["batchNumber"] =			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].CHARG')) AS CHAR CHARACTER SET utf8))"""

	options["backflushFlag"] = 			"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.RGEKZ')) AS CHAR CHARACTER SET utf8))"""
	options["confirmationUnits"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.I_ISOCODE_UM')) AS CHAR CHARACTER SET utf8))"""
	options["materialInstance"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZRESBLAD.C_INSTANCE')) AS CHAR CHARACTER SET utf8))"""
	options["managedByBatch"] = 		"""TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, '].ZE1RESBL.SPLKZ')) AS CHAR CHARACTER SET utf8))"""
	
	
	# BUILD SELECT
	#===================

	# If no specific field is required, get all of them
	if fields==None:
		fields = options.keys()
	else:
		# Check that there is no mistake in field requests
		for field in fields:
			if field not in options.keys():
				raise ValueError("getMaterialsUsedInProductionStep: syntax error in the field name : " + field)

	# Build select clause
	myList = []
	for field in fields:
		# Add "clause" "space" "name of column"
		if field in options.keys():
			myList.append( options[field] + " " + field )
	selectClause = ",".join( myList )

	inClause = ""
	orderClause = ""
	
	if order:
		orderClause = " ORDER BY " + order
	elif fields:
		orderClause = " ORDER BY " + fields[0]
	
		
	

	if workCenter and operationNumber:
		inClause = inClause + """ AND (JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) = """ +"\""+ str(workCenter) + "\""
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) = """ +"\""+ operationNumber + "\")"
	elif workCenter:
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].ARBPL')) = """ +"\""+ str(workCenter) + "\""
	elif operationNumber:
		inClause = inClause + """ AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].VORNR')) = """ +"\""+ operationNumber + "\""

	# Retrieve data

	# EDIT DOMI : changed pfce_mesjsonpropery to pfce_MESJsonProperty
	sql_query = " SELECT "+ selectClause + """
					FROM pfce_MESJsonProperty
					JOIN (
						SELECT  0 AS idx UNION
						SELECT  1 AS idx UNION
						SELECT  2 AS idx UNION
						SELECT  3 AS idx UNION
						SELECT  4 AS idx UNION
						SELECT  5 AS idx UNION
						SELECT  6 AS idx UNION
						SELECT  7 AS idx UNION
						SELECT  8 AS idx UNION
						SELECT  9 AS idx UNION
						SELECT  10 AS idx UNION
						SELECT  11 AS idx UNION
						SELECT  12 AS idx UNION
						SELECT  13 AS idx UNION
						SELECT  14 AS idx UNION
						SELECT  15 AS idx UNION
						SELECT  16 AS idx UNION
						SELECT  17 AS idx UNION
						SELECT  18 AS idx UNION
						SELECT  19 AS idx UNION
						SELECT  20 AS idx UNION
						SELECT  21 AS idx UNION
						SELECT  22 AS idx UNION
						SELECT  23 AS idx UNION
						SELECT  24 AS idx UNION
						SELECT  25 AS idx UNION
						SELECT  26 AS idx UNION
						SELECT  27 AS idx UNION
						SELECT  28 AS idx UNION
						SELECT  29 AS idx UNION
						SELECT  30 AS idx UNION
					  SELECT  31
					  ) AS steeps
					JOIN (
						SELECT  0 AS idy UNION
						SELECT  1 AS idy UNION
						SELECT  2 AS idy UNION
						SELECT  3 AS idy UNION
						SELECT  4 AS idy UNION
						SELECT  5 AS idy UNION
						SELECT  6 AS idy UNION
						SELECT  7 AS idy UNION
						SELECT  8 AS idy UNION
						SELECT  9 AS idy UNION
						SELECT  10 AS idy UNION
						SELECT  11 AS idy UNION
						SELECT  12 AS idy UNION
						SELECT  13 AS idy UNION
						SELECT  14 AS idy UNION
						SELECT  15 AS idy UNION
						SELECT  16 AS idy UNION
						SELECT  17 AS idy UNION
						SELECT  18 AS idy UNION
						SELECT  19 AS idy UNION
						SELECT  20 AS idy UNION
						SELECT  21 AS idy UNION
						SELECT  22 AS idy UNION
						SELECT  23 AS idy UNION
						SELECT  24 AS idy UNION
						SELECT  25 AS idy UNION
						SELECT  26 AS idy UNION
						SELECT  27 AS idy UNION
						SELECT  28 AS idy UNION
						SELECT  29 AS idy UNION
						SELECT  30 AS idy UNION
					  SELECT  31
					  ) AS materials
					WHERE jsonString->"$.AUFNR" = ?
						AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, ']')) IS NOT NULL
						AND JSON_EXTRACT(jsonString, CONCAT('$.E1AFFLL.E1AFVOL[', idx, '].E1RESBL[', idy, ']')) IS NOT NULL """ + inClause + orderClause

	dataset = system.db.runPrepQuery( query=sql_query, args=[workOrderName], database="mes_analysis")
	return dataset
	
	
#==========================================================
# Returns product details from SAP PO order
# workOrderName mandatory - work order for wich we are checking the existance of product details
# other parameters not mandatory
# details - product parameters that should be in result if None all will be prepared
#==========================================================
def getProductDetails2(workOrderName, details=None):
	sql_query = """SELECT 
				TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.ZMATCH')) AS CHAR CHARACTER SET utf8)) 
				FROM pfce_MESJsonProperty 
				WHERE jsonString->"$.AUFNR" = ?"""
				
	dataset = system.db.runPrepQuery( query=sql_query, args=[workOrderName], database="mes_analysis")


	if dataset:
		jsonDS = system.util.jsonDecode(dataset.getValueAt(0,0))
		
		headers = ["ATNAM", "ATWRT", "ATBEZ"]
		data = []
		
		for row in jsonDS:
			if details:
				if row["ATNAM"] in details:
					data.append([row["ATNAM"], row["ATWRT"], row["ATBEZ"]])
			else:
				data.append([row["ATNAM"], row["ATWRT"], row["ATBEZ"]])
		
		returnDS = system.dataset.toDataSet(headers, data)

		return returnDS
	else:
		return dataset


#==========================================================
# Returns product details from SAP PO order
# workOrderName mandatory - work order for wich we are checking the existance of product details
# other parameters not mandatory
# details - product parameters that should be in result if None all will be prepared
#==========================================================
def getProductDetails(workOrderName, details=None):
	sql_query = """SELECT 
				TRIM(BOTH '"' FROM CAST(JSON_EXTRACT(jsonString, CONCAT('$.ZMATCH')) AS CHAR CHARACTER SET utf8)) 
				FROM pfce_MESJsonProperty 
				WHERE jsonString->"$.AUFNR" = ?"""
				
	dataset = system.db.runPrepQuery( query=sql_query, args=[workOrderName], database="mes_analysis")


	if dataset:
		jsonDS = system.util.jsonDecode(dataset.getValueAt(0,0))
		
		headers = ["ATNAM", "ATWRT", "ATBEZ"]
		data = []
		
		if jsonDS is not None:
			for row in jsonDS:
				if details:
					if row["ATNAM"] in details:
						data.append([row["ATNAM"], row["ATWRT"], row["ATBEZ"]])
				else:
					data.append([row["ATNAM"], row["ATWRT"], row["ATBEZ"]])
			
			returnDS = system.dataset.toDataSet(headers, data)
	
			return returnDS
		else:
			return system.dataset.toDataSet(headers, [])
	else:
		return dataset
		
def getWorkOrderAionData(workOrderName=None, workCenter=None, operationNumber=None, aionFields=[]):
	# Helper function
	def addAionFieldToJson(json, operationNumber, fieldName, fieldValue):
		# Work via reference to update json
		if operationNumber not in json:
			json[operationNumber] = {fieldName : fieldValue }
		else:
			json[operationNumber][fieldName] = fieldValue
	#workorder = 
	
	if workOrderName is not None and not isinstance(workOrderName, basestring):
		workOrderName = str(workOrderName)
	
	if workCenter is not None and not isinstance(workCenter, basestring):
		workCenter = str(workCenter)
					
	if operationNumber is not None and not isinstance(operationNumber, basestring):
		operationNumber = str(operationNumber)
	
	
	ds = getSAPProductionStepDetails(workOrderName=workOrderName,workCenter=workCenter, operationNumber=operationNumber, fields=["aionData"])
	# There is something wrong. Go debug.
	if len(ds) == 0:
		sql = """
		SELECT JSON_CONTAINS(jsonString,?,"$.E1AFFLL.E1AFVOL"),JSON_CONTAINS(jsonString,?,"$.E1AFFLL.E1AFVOL") FROM pfce_MESJsonProperty 
		WHERE jsonString->"$.AUFNR" = ?"""
		
		workcenterJson = {'ARBPL': workCenter}
		operationNumberJson = {'VORNR': operationNumber}
		
		args = [system.util.jsonEncode(workcenterJson),
				system.util.jsonEncode(operationNumberJson),
				workOrderName]
		
		debugDs = system.db.runPrepQuery(query=sql, args=args, database="mes_analysis")
		
		debugPyDs = system.dataset.toPyDataSet(debugDs)
		
		if len(debugPyDs) == 0:
			return "Work order not found"
		
		row = list(debugPyDs[0])
		
		if workCenter and row[0] == 0:
			return "Workcenter not found"
		
		elif operationNumber and row[1] == 0:
			return "Step not found"
		else:
			return "Unknown reason"
	
	headers = ["ZAION_VALUE", "ZAION_DEC_POS", "ZOPERATION_NO", "ZAION_ATTRIBUTE"]
	# Check if parameter is string or array
	if isinstance(aionFields, basestring):
		missingFields = [aionFields]    
	else:
		# make a copy of list
		missingFields = list(aionFields)
	
	json = {}
	for row in ds:
		if row["aionData"]:
			aionData = system.util.jsonDecode(row["aionData"])
			#print aionFields
			if isinstance(aionData,list):           
				for aion in aionData:
				
					operationNumber = aion["ZOPERATION_NO"]
					attributeName = aion["ZAION_ATTRIBUTE"]
					attributeValue = aion["ZAION_VALUE"]
					
					# Get all aion fields
					if not aionFields:
						addAionFieldToJson(json, operationNumber,attributeName, attributeValue)
					# Search for specific ones
					else:           
						if attributeName in aionFields:
							addAionFieldToJson(json, operationNumber,attributeName, attributeValue)
							# Attribute with same name could be on different operation numbers
							if attributeName in missingFields:
								missingFields.remove(attributeName)
					
					
			else:
				# Get all aion fields           
				operationNumber = aionData["ZOPERATION_NO"]
				attributeName = aionData["ZAION_ATTRIBUTE"]
				attributeValue = aionData["ZAION_VALUE"]
				
				if not aionFields:
					addAionFieldToJson(json, operationNumber, attributeName, attributeValue)
				# Search for specific ones
				else:
					if attributeName in aionFields:
						addAionFieldToJson(json, operationNumber, attributeName, attributeValue)
						# Attribute with same name could be on different operation numbers
						if attributeName in missingFields:
							missingFields.remove(attributeName)
	for field in missingFields:
		json[field] = "Not found"
	#print system.util.jsonEncode(json, 4)       
	return json		