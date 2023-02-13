from shared.mp.general.exceptions import *
from shared.mp.general.analysis import *

class MEStag:
	def __init__(self, path):
		if not isinstance(path, str):
			raise TagPathTypeError(type(path))
		self.__path = path
		self._readValue()
	
	def _readValue(self):
		qualifiedValue = system.tag.read(self.__path)
		if not qualifiedValue.quality.isGood():
			raise TagQualityError(self.__path)
		self._value = qualifiedValue.value
				
	def _writeValue(self):
		if not system.tag.write(self.__path, self._value):
			raise TagWritingError(self.__path)
	
	def getPath(self):
		return self.__path
	
	def getValue(self):
		return self._value
	
	def readAndGetValue(self):
		self._readValue()
		return self._value
	
	def updateValue(self):
		return self._readValue()
	
	def setValue(self, value):
		self._value = value
		return self._writeValue()


class MEScounter(MEStag):
	def incrementValue(self, delta):
		self._value += delta;
		if self._value < 0:
			self._value = 0
			self.setValue(self._value)
			return 0
		else:
			self.setValue(self._value)
			return 1
	
	def readAndIncrementValue(self, delta):
		self._readValue()
		return self.incrementValue(delta)


class MESanalysisTag(MEStag, MESanalysis):
	def __init__(self, path, analysisData):
		MEStag.__init__(self, path)
		MESanalysis.__init__(self, analysisData)
		if len(analysisData.getDatapoints()) != 3:
			raise TagAnalysisDatapointNumberError(self.getPath(), len(analysisData.getDatapoints()))
		
	def getDatapoint(self):
		return self.getAnalysisData().getDatapoints()[0]
		
	def executeAndGetValue(self, dynamicParams={}):
		try:
			results = self.execute(dynamicParams)
			value = results[0][self.getDatapoint()]
			self.setValue(value)
			return self.getValue()
		except (TagWritingError, TagQualityError, AnalysisResultsNullError, AnalysisNoResultsError):
			raise TagAnalysisError(self.getPath())
		

class MESudt:		
	def __init__(self, udtPath, MEStags = {}):
		if not isinstance(udtPath, str):
			raise UdtPathTypeError(type(udtPath))
		self.__udtPath = udtPath
		self.__correctPath()
		if not isinstance(MEStags, dict):
			raise UdtTagsTypeError(type(MEStags))
		if len(MEStags) <= 0:
			raise UdtTagsNumberError(len(MEStags))
		self.__MEStags = MEStags
				
	def __correctPath(self):
		if self.__udtPath[-1] != "/":
			self.__udtPath = self.__udtPath + "/"
			
	def getTag(self, keyName):
		return self.__MEStags[keyName]

	def getAllTags(self):
		return self.__MEStags
		
			