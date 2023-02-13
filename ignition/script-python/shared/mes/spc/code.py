#===============================================
#===============================================
#
# mes.spc.*
#
# Functions in relation with spc, especially sample retrieving function
#
#===============================================
#===============================================


# Retrieve sample
#sampleUUID = event.newValue
def getSampleData( sampleUUID):
	
	if not sampleUUID:
		return

	sample = system.quality.sample.data.getSample(sampleUUID)
	if not sample:
		return
		
	# Get sample definition
	sampleDef = sample.getSampleDefinition()
	if not sampleDef:
		return
				
	headers = []
	data = []
	
	# Get sample attributes
	sampleAtt = sampleDef.getEnabledAttributes()
	for attribute in sampleAtt:
		#print attribute.getName()
		headers.append( attribute.getName()  )
		 
	# Get measurements
	measurements = sample.getAllMeasurements()

	#print measurements
	data = list(measurements)

	# Get additional factors	
	additionalFactors = sample.getAllAddlFactors()
	for factor in additionalFactors:
		headers.append( factor.getName())
		data.append( factor.getValue())
	# headers [u'Weight', u'Thickness', u'Density', u'T2 mould', u'Cavity', u'T1 mould']
	# data [40.2045440674, 1.15999996662, 2.9165176884, u'3', u'3', u'4']

	return headers, data

#=============================================================
def getSamplesUUID(sampleDefName, sampleLoc, startDate,endDate):

	# Remove eventual [xxxx] at beginning
	if sampleLoc.find("]"):
		sampleLoc = sampleLoc.split(']', 1)[-1]
		
	if sampleLoc[0]=="\\":
		sampleLoc = sampleLoc[1:]
	
	Enterprise, Site, Area, Line, remaining = sampleLoc.split("\\",4)

	if not Enterprise or not Site or not Area or not Line:
		return

	Location = sampleLoc.rsplit("\\",1)[-1]
	if not Location:
		return

	dbSampleDef = system.quality.definition.getSampleDefinition( sampleDefName)
	if not dbSampleDef:
		return

	sampleDefUUID = dbSampleDef.getDefUUID()
	print type(sampleDefUUID)
	if not sampleDefUUID:
		return

	# Get data from QUAL_Sample
	results= system.db.runPrepQuery("SELECT SampleUUID FROM QUAL_Sample WHERE DefUUID=? AND Enterprise=? AND Site=? AND Area=? AND Line=? AND Location=? AND TimeStamp between ? AND ?", 
									[sampleDefUUID, Enterprise, Site, Area, Line, Location, startDate, endDate], 
									database="mes_analysis")
	if len(results)==0:
		return

	return results
	# SELECT SampleUUID FROM QUAL_Sample WHERE DefUUID="ce5c4d38-8755-4679-b0c1-a60bad793fd2" AND Enterprise="SGA" AND Site="AMB" AND Area="TW-40" AND Line="P55" AND Location="auto" LIMIT 10 

#=============================================================
def getSamples(sampleDefName, sampleLoc, startDate,endDate, attributes=[]):
	# SAMPLES SAMPLES SAMPLES
	
	# Remove eventual [xxxx] at beginning
	if sampleLoc.find("]"):
		sampleLoc = sampleLoc.split(']', 1)[-1]
		
	if sampleLoc[0]=="\\":
		sampleLoc = sampleLoc[1:]
	
	Enterprise, Site, Area, Line, remaining = sampleLoc.split("\\",4)
	if not Enterprise or not Site or not Area or not Line:
		return

	Location = sampleLoc.rsplit("\\",1)[-1]
	if not Location:
		return

	dbSampleDef = system.quality.definition.getSampleDefinition( sampleDefName)
	if not dbSampleDef:
		return

	sampleDefUUID = dbSampleDef.getDefUUID()
	if not sampleDefUUID:
		return

	# Get all UUIDs corresponding the parameters
	results= system.db.runPrepQuery("SELECT SampleUUID FROM QUAL_Sample WHERE DefUUID=? AND Enterprise=? AND Site=? AND Area=? AND Line=? AND Location=? AND TimeStamp between ? AND ?", 
									[sampleDefUUID, Enterprise, Site, Area, Line, Location, startDate, endDate], 
									database="mes_analysis")
	if len(results)==0:
		return
	
	# Transform as list
	list = []
	for row in results:
		list.append( "'"+ row[0]+ "'" )
	
	# Transform as comma separated string
	lookup = ",".join(list)
	
	# Get data from QUAL_SampleData
	if len(attributes)==0:
		clauseFields = ""
	else:
	
		clauseFields = " AND attrName IN ("+  ', '.join(['"%s"' % w for w in attributes])   + ") "

	#"SELECT SampleUUID, 		CONCAT( '{', GROUP_CONCAT( CONCAT_WS(':', QUOTE(FactorName), FactorValue) ), '}')  AS data FROM QUAL_AddlFactor WHERE SampleUUID IN ("+lookup+") " + clauseFields +" GROUP BY SampleUUID"	
	query = "SELECT SampleUUID, CONCAT( '{', GROUP_CONCAT( CONCAT_WS(':', QUOTE( CONCAT(attrName,MeasNo)  ), AttrValue  ) ), '}')  AS data FROM QUAL_SampleData WHERE SampleUUID IN ("+lookup+") " + clauseFields +" GROUP BY SampleUUID"
	results= system.db.runQuery( query , database="mes_analysis")
	#for x in results:
	#	print x[0], "=", x[1]
	if len(results)==0:
		return

	return results


#=============================================================
def getFactors(sampleDefName, sampleLoc, startDate,endDate, factors=[]):
	# FACTORS FACTORS FACTORS

	# Remove eventual [xxxx] at beginning
	if sampleLoc.find("]"):
		sampleLoc = sampleLoc.split(']', 1)[-1]
		
	if sampleLoc[0]=="\\":
		sampleLoc = sampleLoc[1:]
	
	Enterprise, Site, Area, Line, remaining = sampleLoc.split("\\",4)
	if not Enterprise or not Site or not Area or not Line:
		return

	Location = sampleLoc.rsplit("\\",1)[-1]
	if not Location:
		return

	dbSampleDef = system.quality.definition.getSampleDefinition( sampleDefName)
	if not dbSampleDef:
		return

	sampleDefUUID = dbSampleDef.getDefUUID()
	if not sampleDefUUID:
		return

	# Get all UUIDs corresponding the parameters
	results= system.db.runPrepQuery("SELECT SampleUUID FROM QUAL_Sample WHERE DefUUID=? AND Enterprise=? AND Site=? AND Area=? AND Line=? AND Location=? AND TimeStamp between ? AND ?", 
									[sampleDefUUID, Enterprise, Site, Area, Line, Location, startDate, endDate], 
									database="mes_analysis")
	if len(results)==0:
		return

	# Transform as list
	list = []
	for row in results:
		list.append( "'"+ row[0]+ "'" )
	
	# Transform as comma separated string
	lookup = ",".join(list)
	
	# Get data from QUAL_SampleData
	if len(factors)==0:
		clauseFields = ""
	else:
	
		clauseFields = " AND FactorName IN ("+  ', '.join(['"%s"' % w for w in factors])   + ") "
		
	
	# FUTURE MySQL 5.7.22 query = "SELECT SampleUUID, JSON_OBJECTAGG( attrName, AttrValue ) AS data FROM QUAL_SampleData WHERE SampleUUID IN ("+lookup+") " + clauseFields +" GROUP BY SampleUUID"
	query = "SELECT SampleUUID, CONCAT( '{', GROUP_CONCAT( CONCAT_WS(':', QUOTE(FactorName), FactorValue) ), '}')  AS data FROM QUAL_AddlFactor WHERE SampleUUID IN ("+lookup+") " + clauseFields +" GROUP BY SampleUUID"
	results= system.db.runQuery( query , database="mes_analysis")
	if len(results)==0:
		return

	return results


#=============================================================
def getAllSampleData(sampleDefName, sampleLoc, startDate,endDate, attributes=[], factors=[]):
	# FULL PROCESS - FULL PROCESS

	# Remove eventual [xxxx] at beginning
	if sampleLoc.find("]"):
		sampleLoc = sampleLoc.split(']', 1)[-1]
		
	if sampleLoc[0]=="\\":
		sampleLoc = sampleLoc[1:]
	#print sampleLoc
	Enterprise, Site, Area, Line, remaining = sampleLoc.split("\\",4)
	
	if not Enterprise or not Site or not Area or not Line:
		return

	Location = sampleLoc.rsplit("\\",1)[-1]
	
	if not Location:
		return

	dbSampleDef = system.quality.definition.getSampleDefinition( sampleDefName )
	if not dbSampleDef:
		return

	sampleDefUUID = dbSampleDef.getDefUUID()
	if not sampleDefUUID:
		return

	# Get all UUIDs corresponding the parameters
	UUIDs= system.db.runPrepQuery("SELECT SampleUUID, TimeStamp FROM QUAL_Sample WHERE DefUUID=? AND Enterprise=? AND Site=? AND Area=? AND Line=? AND Location=? AND TimeStamp between ? AND ?", 
									[sampleDefUUID, Enterprise, Site, Area, Line, Location, startDate, endDate], 
									database="mes_analysis")
	
	if len(UUIDs)==0:
		return
	
	# Transform as list
	list = []
	for row in UUIDs:
		list.append( "'"+ row[0]+ "'" )
	
	# Transform as comma separated string
	lookup = ",".join(list)

	# Get data from QUAL_AddlFactor
	if len(factors)==0:
		clauseFactorFields = ""
	else:	
		clauseFactorFields = " AND FactorName IN ("+  ', '.join(['"%s"' % w for w in factors])   + ") "
		
	# Get additional factor values	
	query = "SELECT SampleUUID, CONCAT( '{', GROUP_CONCAT( CONCAT_WS(':', QUOTE(FactorName), FactorValue) ), '}')  AS data FROM QUAL_AddlFactor WHERE SampleUUID IN ("+lookup+") " + clauseFactorFields +" GROUP BY SampleUUID"
	factorValues = system.db.runQuery( query , database="mes_analysis")
	
	if len(factorValues)==0:
		return

	# Get data from QUAL_SampleData
	if len(attributes)==0:
		clauseAttributesFields = ""
	else:
		clauseAttributesFields = " AND attrName IN ("+  ', '.join(['"%s"' % w for w in attributes])   + ") "

	# Get Sample data
	query = "SELECT SampleUUID, CONCAT( '{', GROUP_CONCAT( CONCAT_WS(':', QUOTE( CONCAT(attrName,MeasNo)  ), AttrValue  ) ), '}')  AS data FROM QUAL_SampleData WHERE SampleUUID IN ("+lookup+") " + clauseAttributesFields +" GROUP BY SampleUUID"
	attrValues = system.db.runQuery( query , database="mes_analysis")
	#for x in results:
	#	print x[0], "=", x[1]
	if len(attrValues)==0:
		return

	def merge_two_dicts(x, y):
	    """Given two dicts, merge them into a new dict as a shallow copy."""
	    z = x.copy()
	    z.update(y)
	    return z
	
	data = []
	for index in range(len(attrValues)):
		# Verify UUID is identical, we never know...
		if attrValues[index][0]==factorValues[index][0]:
			dict1 = system.util.jsonDecode( attrValues[index][1] )
			dict2 = system.util.jsonDecode( factorValues[index][1] )
			data.append(  merge_two_dicts( dict1, dict2 ) )
		else:
			print "NOK"

	return data
