def retrieveMailAddress(userDataSet):
	# Take userDataSet and retrieve the user's email address from it
	#
	userPyDataSet = system.dataset.toPyDataSet(userDataSet)
	user = system.tag.read('[System]Client/User/Username').value
	
	for row in userPyDataSet:
		tempUser = row[0]
		if tempUser == user:
			email=row[3]
			break
	if len(email) == 0:
		email == ''
	
	return email
	
def takeScreenShot(event):
	from system.gui import getParentWindow
	from system.print import createImage
	from java.io import ByteArrayOutputStream
	from javax.imageio import ImageIO
	
	# Get an image of the entire screen as seen by the user.
	# By using getParentWindow(event).parent, you are grabbing the entire session.
	# This will include any docked windows.
	# Is returned as a bufferedImage. Need to convert it to a byteArray.
	# Instantiate an object of ByteArrayOutputStream.
	baos = ByteArrayOutputStream(15000)
	# Write the bufferedImage returned by the getParentWindow(event).parent as a ByteArrayOutputStream.
	ImageIO.write(createImage(getParentWindow(event).parent),'jpg', baos)
	# Flush the stream.
	baos.flush()
	# Convert the ByteArrayOutputStream to a byteArray.
	screenShot = baos.toByteArray()
	# Close the stream.
	baos.close()
	
	return screenShot
	
def setpointChangedNotification(tagName, oldValue, newValue, machineName, distributionList):
		distributionList = system.tag.read(distributionList).value
		recipients = []
		
		for row in range(distributionList.getRowCount()):
			recipients.append(distributionList.getValueAt(row,0))
		
		body = ( '____________________________________________________________\n'
			'This message is sent from Ignition.\n'
			'____________________________________________________________\n\n'
			'Setting ' + tagName + ' has been changed from ' + str(oldValue) + ' to ' + str(newValue) + '.\n\n' 
			'Please do not reply to this email.' )
			
		payload = {
			'to': recipients,
			'subject': ( machineName + ' setting ' + tagName + ' has been changed' ),
			'body' : body ,
			'fileName': [],
			'fileData': [],
			'priority': 1,
			'messageType': 1,
			'html': False
			}
			
		result = system.util.sendRequest('pfce_sga_interfaces', 'sendEmail', payload, timeoutSec='1200')
			
		return result
