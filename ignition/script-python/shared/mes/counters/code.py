# ==============================================
# shared.mes.production
# Last update: 2019-01-04
# Modified by: Rok Zupan
# ==============================================


#===============================================
# Add/Remove OUTFEED
#===============================================
def addOutfeed( callerTagPath, deltaQuantity ):
	tagEndPath = "/mes/prod_outfeed"
	
	# We want to know what is the Ignition folder for the equipment
	# We will look for the tagEndPath
	tagPath = callerTagPath + tagEndPath
	
	if not system.tag.exists(tagPath):
		return False
	
	# Get existing value
	qv = system.tag.read(tagPath)

	# Write only if tag does not exist or is not connected
	if qv.quality.isGood():
		system.tag.write( tagPath , qv.value + deltaQuantity)
		return True
	else:
		return False
				
#===============================================
# Add/Remove INFEED
#===============================================
def addInfeed( callerTagPath, deltaQuantity ):
	tagEndPath = "/mes/prod_infeed"
	
	# We want to know what is the Ignition folder for the equipment
	# We will look for the tagEndPath
	tagPath = callerTagPath + tagEndPath
	
	if not system.tag.exists(tagPath):
		return False
	
	# Get existing value
	qv = system.tag.read(tagPath)

	# Write only if tag does not exist or is not connected
	if qv.quality.isGood():
		system.tag.write( tagPath , qv.value + deltaQuantity)
		return True
	else:
		return False
				
#===============================================
# Add/Remove REJECTS
#===============================================
def addRejects( callerTagPath, deltaQuantity ):
	tagEndPath = "/mes/prod_rejects"
	
	#fullPath = callerTagPath + tagEndPath
	# We want to know what is the Ignition folder for the equipment
	# We will look for the tagEndPath
	#parent = callerTagPath.rsplit('/', 1)[0]
	#if not system.tag.exists( parent + tagEndPath):
	#	parent = parent.rsplit('/', 1)[0]
	#	if not system.tag.exists( parent+tagEndPath):
	#		return False		

	# Tag for mes outfeed for any machine - recorded as counter
	rejectsTagPath = callerTagPath + tagEndPath
	
	# Get existing value
	# If value is none, then set to 0
	qv = system.tag.read(rejectsTagPath)
	if qv.value is None:
		qv.value = 0
	
	# Write only if tag does not exist or is not connected
	if qv.quality.isGood():
		system.tag.write( rejectsTagPath , qv.value + deltaQuantity)
		return True
	else:
		return False


#===============================================
# Add/Remove using a specific tag
#===============================================
def addQuantityToTag( targetTagPath, deltaQuantity ):

	if not system.tag.exists( targetTagPath ):
		return False		

	# Get existing value
	qv = system.tag.read(targetTagPath)

	# Write only if tag does not exist or is not connected
	if qv.quality.isGood():
		system.tag.write( targetTagPath , qv.value + deltaQuantity)
		return True
	else:
		return False
