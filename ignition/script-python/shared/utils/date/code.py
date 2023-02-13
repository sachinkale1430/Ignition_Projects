#======================================================================
# Convert SGA SAP datetime to UTC
# SAP SGA located in Boston USA, timezone of data = EST
# We need to get it to UTC and IGnition will manage displays
#======================================================================
def sga_sap_datetime_to_UTC( sap_date):
    #sap_time = sap_time.ljust(6, "0")       
    dt = system.date.parse( sap_date , 'yyyyMMdd')
    return dt


def sga_sap_datetime_user_friendly( sap_date):
	sap_date = sga_sap_datetime_to_UTC( sap_date)
	sap_date = system.date.format(sap_date, "dd.MM.yyyy")
	return sap_date
	
	
def date_time_iso_8061_parser():
	from java.text import SimpleDateFormat
	return SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX") 
	
	
def translate_iso_8061_time(input):
	import datetime
	result = datetime.datetime.strptime(input, "%Y-%m-%dT%H:%M:%S")
	return result
	
def local_date_time_to_SAP(dateTime):
	from java.util import TimeZone
	date = dateTime	
	SAPBostonTimeZone = TimeZone.getTimeZone("EST")	
	parser = shared.utils.date.date_time_iso_8061_parser()
	parser.setTimeZone(SAPBostonTimeZone)
	SAPBostonTime = parser.format(date)
	return SAPBostonTime
	
	
def getDateFormats():
	"""
	function that prepares dateFormats based on client
	
	Parameters
	----------
	None:
	
	Returns
		dict:
			dateFormat
			timeFormat
			dateDBFormat
			dateDB
	-------
	None
	"""	
	timezone = system.tag.read("[System]Gateway/Timezone").value
	
	if "Europe" in timezone:
		dateTimeFormat = "dd.MM.yyyy HH:mm:ss"
		dateTimeFormatDb = "%d.%m.%y %hh:%ii:%ss"
		timeFormat = "hh:mm"
		timeFormatDb = "%hh:%ii:%ss"
	else:
		dateTimeFormat = "MM/dd/yyyy h:m:s a"
		dateTimeFormatDb = "%m/%d/%y %h:%i %p"
		timeFormat = "hh:mm a"
		timeFormatDb = "%h:%i%p"
				
	return {
		"dateTimeFormat": dateTimeFormat,
		"dateTimeFormatDb": dateTimeFormatDb,
		"timeFormat": timeFormat,
		"timeFormatDb": timeFormatDb
	}		