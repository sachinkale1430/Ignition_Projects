def mailIncineratorReport(recipient, startDate, endDate):
	body = """
		<!DOCTYPE html>
		<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
			<head>
				<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
			</head>
			<body>
				Incinerator energy consumption report
			</body>
		</html> 
	"""
	
	fileName = 'IncineratorReport.pdf'
	fileData = system.report.executeReport(path='Incinerator/reportEnergy', project='sga_incinerator', parameters={'startDate':startDate, 'endDate':endDate},fileType='pdf')
	
	payload = {
		'to': [recipient],
		'subject': 'SGA - EIB Incinerator energy report',
		'body': body,
		'priority': 1,
		'messageType': 1,
		'html': True,
		'fileName': [fileName],
		'fileData': [fileData]	
	}

	result = system.util.sendRequest('pfce_sga_interfaces', 'sendEmail', payload)
	
	return result

def setpointChangedAlert(tagName, oldValue, newValue, distributionList):
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
		'subject': ( 'Incinerator setting ' + tagName + ' has been changed' ),
		'body' : body ,
		'fileName': [],
		'fileData': [],
		'priority': 1,
		'messageType': 1,
		'html': False
		}
		
	result = system.util.sendRequest('pfce_sga_interfaces', 'sendEmail', payload, timeoutSec='1200')
		
	return result
