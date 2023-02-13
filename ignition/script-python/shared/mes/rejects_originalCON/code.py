def imageUpload(eventSource, factoryRejectCode, imagePath):
	"""
	Function for importing images.
	
	Parameters
	----------
	factoryRejectCode: str
		Reject code to relate to image
	imagePath: str
		Picture in bytes
		
	Returns
	-------
	None or str with error
	"""
	from java.io import ByteArrayInputStream
	from javax.imageio import ImageIO
	from java.net import URLConnection
	from java.awt.image import BufferedImage
	from javax.swing import ImageIcon
	
	picture = system.file.readFileAsBytes(imagePath)
	idReject = getRejectId(factoryRejectCode)
	
	image = ImageIcon(picture)
	imageWidth = image.getIconWidth()
	imageHeight = image.getIconHeight()
	
	if not idReject:
		return "Reject code does not exists"
	
	imageSize = len(picture)
	
	if imageSize >= 4 * 1024 * 1024: # 4Mb max size
		return "Image is too large. Max allowed image size is 4MB"
	
	bais = ByteArrayInputStream(picture)
	fileType = URLConnection.guessContentTypeFromStream(bais)
	allowedTypes = ["image/png", "image/jpg"]
	
	if fileType not in allowedTypes:
		return "Image is not in allowed format: "+ fileType + " Allowed types: "+str(allowedTypes)
	
	imageConversionWidth = 150
	imageConversionHeight = imageConversionWidth * imageHeight / imageWidth
	imageConversionHeight = int(imageConversionHeight)
	
	thumbnail = makeThumbnail(eventSource, picture, imageConversionWidth, imageConversionHeight)
	
	sqlInsert = """
		INSERT INTO 
			factory_reject_images 
			(image, size, timestamp, username, thumbnail, thumbnail_width, thumbnail_height) 
		VALUES 
			(?, ?, ?, ?, ?, ?, ?)
	"""	
	
	timestamp = system.tag.read("[System]Gateway/CurrentDateTime").value
	user = system.security.getUsername()
	
	
	#print timestamp, user, thumbnail, width, height
	
	#idImage = system.db.runUpdateQuery(query=sqlInsert, args=[timestamp], database="factory_parameters", getKey=1)
	idImage = system.db.runPrepUpdate(query=sqlInsert, args=[picture, imageSize, timestamp, user, thumbnail, imageConversionWidth, imageConversionHeight], database="factory_parameters", getKey=1)
	
	sqlUpdateReject = """
		UPDATE 
			factory_reject_codes 
		SET id_reject_image = ? 
		WHERE id = ?
	"""
	
	system.db.runPrepUpdate(query=sqlUpdateReject, args=[idImage, idReject], database="factory_parameters")
	
	
	
def makeThumbnail(eventSource, image, width, height):
	"""
	Helper function to make thumbnail from image
	
	Parameters
	----------
	image: byte[]
		Image from which to make thumbnail
	width: int
		width of thumbnail
	height: int
		height of thumbnail
		
	Returns
	-------
	byte[] 
		Thumbnail in png format
	"""
	
	from java.io import ByteArrayInputStream, ByteArrayOutputStream
	from javax.imageio import ImageIO
	from java.net import URLConnection
	from java.awt.image import BufferedImage
	from javax.swing import ImageIcon
	
	bais = ByteArrayInputStream(image)

	image = ImageIO.read(bais)

	thumbnail = image.getScaledInstance(width, height, BufferedImage.SCALE_SMOOTH)
	imageBuffered = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
	
	i = imageBuffered.getGraphics()
	o = i.drawImage(thumbnail, 0,0, eventSource)
	
	baos = ByteArrayOutputStream()

	ImageIO.write(imageBuffered, "png", baos)

	baos.flush()
	
	imageInBytes = baos.toByteArray()

	return imageInBytes
	
	

def getImage(rejectCode):
	sqlQuery = """
		SELECT 
			image 
		FROM 
			factory_reject_codes 
		JOIN factory_reject_images ON (factory_reject_images.id = factory_reject_codes.id_reject_image) 
		WHERE 
			factory_reject_codes.rejectCode = ?
	"""
	return system.db.runScalarPrepQuery(sqlQuery, args=[rejectCode], database="factory_parameters")
	
	
	
def getThumbnail(rejectCode):
	sqlQuery = """
		SELECT 
			thumbnail 
		FROM 
			factory_reject_images 
		JOIN factory_reject_codes ON (factory_reject_images.id = factory_reject_codes.id_reject_image) 
		WHERE 
			factory_reject_codes.rejectCode = ?
	"""
	return system.db.runScalarPrepQuery(sqlQuery, args=[rejectCode], database="factory_parameters")
	
	
	
def updateThumbnail(id):
	pass



def updateAllThumbnails():
	pass
	
	
		
def deleteImage(rejectCode):
	"""
	Clear and delete image data
	
	Parameters
	----------
	rejectCode: string
		rejectCode
		
	Returns
	-------
		Removal result
	"""

	# get imageId based on rejectCode
	sqlQuery = """
		SELECT 
			id_reject_image 
		FROM 
			factory_reject_codes 
		WHERE 
			rejectCode = ?"""
	imageId = system.db.runScalarPrepQuery(sqlQuery, [rejectCode], "factory_parameters")
	
	# Update reject codes table -> setting null to image id 
	sqlQuery = """
		UPDATE 
			factory_reject_codes 
		SET 
			id_reject_image = NULL
		WHERE 
			id_reject_image = ?
	"""
	sqlResult = system.db.runPrepUpdate(sqlQuery, [imageId], "factory_parameters")
	
	# Delete image from database
	sqlQuery = """
		DELETE FROM 
			factory_reject_images
		WHERE id = ?
	"""
	sqlResult = system.db.runPrepUpdate(sqlQuery, [imageId], "factory_parameters")
	
	return sqlResult
	
	
	
def deleteFamily(familyName):
	"""
	Clear and delete family data
	
	Parameters
	----------
	familyName: string
		
	Returns
	-------
		Removal result
	"""

	# get imageId based on rejectCode
	sqlQuery = """
		SELECT 
			id 
		FROM 
			factory_reject_families 
		WHERE 
			category = ?"""
	familyId = system.db.runScalarPrepQuery(sqlQuery, [familyName], "factory_parameters")
	
	# Update reject codes table -> setting null to family id 
	sqlQuery = """
		UPDATE 
			factory_reject_codes 
		SET 
			id_reject_family = NULL
		WHERE 
			id_reject_family = ?
	"""
	sqlResult = system.db.runPrepUpdate(sqlQuery, [familyId], "factory_parameters")
	
	# Delete image from database
	sqlQuery = """
		DELETE FROM 
			factory_reject_families
		WHERE id = ?
	"""
	sqlResult = system.db.runPrepUpdate(sqlQuery, [familyId], "factory_parameters")
	
	return sqlResult
	
	
	
def getRejectId(rejectCode):
	"""
	Helper function to check if reject code exists
	
	Parameters
	----------
	rejectCode: str
		ERP reject code
	
	Returns
	-------
	int
		Id if exists, None otherwise
	"""
	
	sql = "SELECT id FROM factory_reject_codes WHERE rejectCode=?"
	
	id = system.db.runScalarPrepQuery(sql, args=[rejectCode], database="factory_parameters")
	
	return id
	
	
def buildRejectStructure(machineName, ignitionTagPath, inputFamilyId):	
	"""
	Function that builds reject codes structure based on configuration made through factory configurator
	
	Parameters
	----------
	machineName: str
		name of the machine. coming from mes_udt param_stationName
	ignitionTagPath: str
		tag path to machine
		
	Returns
	-------
	dataset
		dataset that fits template repeater
	"""
	print machineName, ignitionTagPath, str(inputFamilyId)
	sqlQuery = """
		SELECT 
			factory_reject_codes.id as 'id',
			factory_reject_codes.timestamp as 'Last Update',
			factory_reject_codes.rejectCode as 'Reject Code',	
			factory_reject_codes.rejectDescription as 'Description',
			factory_reject_codes.rejectDescriptionLocal as 'Local Description',
			CAST(factory_reject_codes.factoryErpCode as CHAR) as 'Factory Code',
			factory_reject_codes.enabled as 'enabled',
			factory_reject_codes.id_reject_family as 'Family Id',
			factory_reject_families.category as 'Family Name',
			factory_reject_codes.id_reject_image as 'Image Id'
		FROM factory_reject_codes 
		LEFT JOIN factory_reject_families ON factory_reject_families.id = factory_reject_codes.id_reject_family  
		WHERE 
			JSON_EXTRACT(factory_reject_codes.mesMachines, '$.\"""" + machineName + """\"') = '[default]""" + ignitionTagPath + """'
	"""
	
	if inputFamilyId > 0:
		sqlQuery += """
			 AND 
			factory_reject_codes.id_reject_family = """ + str(inputFamilyId) + """
		"""

	sqlResult = system.db.runQuery(sqlQuery, "factory_parameters")
	
	headers = [
		"code",
		"ignitionTagPath",
		"leveled",
		"image",
		"wasteDescription",
		"wasteName",
		"familyId"
	]
	
	data = []
	family = []
	
	for row in system.dataset.toPyDataSet(sqlResult):
		familyIdValue = 0
		imageId = None
		appendData = 0
		
		if row["Family Id"] > 0 and inputFamilyId == 0:
			if row["Family Id"] not in family:
				leveled = 1
				wasteName = system.db.runScalarPrepQuery("SELECT category FROM factory_reject_families WHERE id = ?", [row["Family Id"]], "factory_parameters")
				family.append(row["Family Id"])
				displayText = wasteName
				familyIdValue = row["Family Id"]
				appendData = 1
		else:
			if row["Image Id"] > 0:
				imageId = str(row["Image Id"])
			else:
				imageId = None
				
			leveled = familyIdValue = 0
			wasteName = row["Description"]
			
			if row["Local Description"] is not None:
				if len(row["Local Description"]) > 0: 
					wasteName = row["Local Description"]

			displayText = "<html><font size='6'><b>" + row["Reject Code"] + "</b></font><br>" + wasteName + "</html>"
			appendData = 1
		
		if appendData == 1:
			data.append([
				row["Reject Code"],
				ignitionTagPath,
				leveled,
				imageId,
				row["Local Description"],
				displayText,
				familyIdValue
			])	
	
	
	dataDS = system.dataset.toDataSet(headers, data)
	
	
	return dataDS	