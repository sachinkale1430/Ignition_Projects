##################
# GENERAL ERRORS #
##################

class GeneralError(Exception):
	def __init__(self, message="General error."):
		self.message = message
		Exception.__init__(self, message)

class WrongTypeError(GeneralError):
	def __init__(self, expectedType=None, givenType=None, message="Wrong data type."):
		self.expectedType = expectedType
		self.givenType = givenType
		GeneralError.__init__(self, message)

	def __str__(self):
		return "%s %s expected, %s given." % (self.message, self.expectedType, self.givenType)

class WrongElementsNumberError(GeneralError):
	def __init__(self, expectedNumber=None, givenNumber=None, message="Wrong number of elements."):
		self.expectedNumber = expectedNumber
		self.givenNumber = givenNumber
		GeneralError.__init__(self, message)
		
	def __str__(self):
		return "%s %s expected, %s given." % (self.message, self.expectedNumber, self.givenNumber)


#######################
# TAGS RELATED ERRORS #
#######################		

class TagError(GeneralError):
	def __init__(self, tagPath="not defined", message="Tag general error."):
		self.tagPath = tagPath
		GeneralError.__init__(self, self.message)
	
	def __str__(self):
		return "Tag path: %s. %s" % (self.tagPath, self.message)

class TagPathTypeError(WrongTypeError):
	def __init__(self, givenType=None, message="Wrong tag path type."):
		self.expectedType="str"
		WrongTypeError.__init__(self, self.expectedType, givenType, message)

class TagQualityError(TagError):
	def __init__(self, tagPath="not defined", message="Tag quality is bad."):
		TagError.__init__(self, tagPath, message)

class TagNotExistsError(TagError):
	def __init__(self, tagPath="not defined", message="Tag does not exists."):
		TagError.__init__(self, tagPath, message)

class TagWritingError(TagError):
	def __init__(self, tagPath="not defined", message="Writing to tag error."):
		TagError.__init__(self, tagPath, message)

class TagAnalysisError(TagError):
	def __init__(self, tagPath="not defined", message="Executing analysis error."):
		TagError.__init__(self, tagPath, message)

class TagAnalysisDatapointNumberError(TagError):
	def __init__(self, tagPath="not defined", numberOfDatapoints=0, message="Wrong number of datapoints given."):
		self.numberOfDatapoints = numberOfDatapoints
		TagError.__init__(self, tagPath, message)

	def __str__(self):
		return "Tag path: %s. %s 3 expected, %s given." % (self.tagPath, self.message, self.numberOfDatapoints)

class UdtPathTypeError(WrongTypeError):
	def __init__(self, givenType=None, message="Wrong udt path type."):
		self.expectedType="str"
		WrongTypeError.__init__(self, self.expectedType, givenType, message)

class UdtTagsTypeError(WrongTypeError, TagError):
	def __init__(self, udtPath="not defined", givenType=None, message="Wrong provided tags type."):
		self.udtPath = udtPath
		self.expectedType="list"
		WrongTypeError.__init__(self, self.expectedType, givenType)	
		TagError.__init__(self, udtPath)
	
	def __str__(self):
		return "Udt path: %s. %s %s expected, %s given." % (self.udtPath, self.message, self.expectedType, self.givenType)

class UdtTagsNumberError(WrongElementsNumberError, TagError):
	def __init__(self, udtPath="not defined", givenNumber=None, message="Wrong number of tags provided."):
		self.udtPath = udtPath
		self.expectedNumber=">0"
		WrongElementsNumberError.__init__(self, self.expectedNumber, givenNumber)	
		TagError.__init__(self, udtPath)
			
	def __str__(self):
		return "Udt path: %s. %s %s expected, %s given." % (self.udtPath, self.message, self.expectedNumber, self.givenNumber)


###########################
# ANALYSIS RELATED ERRORS #
##########################

class AnalysisParameterTypeError(WrongTypeError):
	def __init__(self, expectedType=None, givenType=None, message="Wrong analysis datapoints type."):
		WrongTypeError.__init__(self, expectedType, givenType)

class AnalysisDatapointsNumberError(WrongElementsNumberError):
	def __init__(self, givenType=None, message="Wrong number of elements."):
		self.expectedNumber=">0"
		WrongElementsNumberError.__init__(self, self.expectedNumber, givenNumber)

class AnalysisTimeRangeError(GeneralError):
	def __init__(self, message="No duration and start date specified."):
		GeneralError.__init__(self, message)
		
class AnalysisDurationTimeUnitError(GeneralError):
	def __init__(self, durationUnit=None, message="Duration unit error. Expected one of the strings: 'minutes', 'hours', 'days'."):
		self.durationUnit = durationUnit
		GeneralError.__init__(self, message)
	
	def __str__(self):
		return "%s '%s' given" % (self.message, self.durationUnit)
	
class AnalysisDurationTimeUnitNullError(GeneralError):
	def __init__(self, message="Duration unit NULL error."):
		GeneralError.__init__(self, message)

class AnalysisStartDateNullError(GeneralError):
	def __init__(self, message="Start date NULL error."):
		GeneralError.__init__(self, message)

class AnalysisResultsNullError(GeneralError):
	def __init__(self, message="Analysis results NULL error."):
		GeneralError.__init__(self, message)

class AnalysisNoResultsError(GeneralError):
	def __init__(self, message="Analysis no results error."):
		GeneralError.__init__(self, message)