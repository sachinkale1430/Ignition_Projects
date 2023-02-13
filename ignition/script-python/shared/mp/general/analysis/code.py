from shared.mp.general.exceptions import *

class MESanalysisData():
	def __init__(self, analysisDatapoints, filterExpression, groupByExpression, parameters = {}, duration=None, durationTimeUnit=None, startDate=None):
		if not isinstance(analysisDatapoints, list):
			raise AnalysisParameterTypeError("list", type(analysisDatapoints), "Wrong analysis datapoints type.")
		if len(analysisDatapoints) <= 0:
			raise AnalysisDatapointsNumberError(len(analysisDatapoints))
		self._datapoints = analysisDatapoints
		if not isinstance(parameters, dict):
			raise AnalysisParameterTypeError("dict", type(parameters), "Wrong analysis parameters type.")
		self._parameters = parameters
		if not isinstance(filterExpression,str):
			raise AnalysisParameterTypeError("str", type(filterExpression), "Wrong analysis filter expression type.")
		self._filterExpression = filterExpression
		if not isinstance(groupByExpression,str):
			raise AnalysisParameterTypeError("str", type(groupByExpression), "Wrong analysis group by expression type.")
		self._groupByExpression = groupByExpression
		if not duration and not startDate:
			raise AnalysisTimeRangeError
		self._duration = duration
		if duration:
			if not isinstance(duration, int):
				raise AnalysisDurationTypeError(type(duration))
			if not durationTimeUnit in ("minutes","hours","days"):
				raise AnalysisDurationTimeUnitError(durationTimeUnit)
			self._durationTimeUnit = durationTimeUnit
		else:
			self._startDate = startDate
	
	def getDatapoints(self):
		return self._datapoints
	
	def getParameters(self):
		return self._parameters
	
	def getFilterExpression(self):
		return self._filterExpression
	
	def getGroupByExpression(self):
		return self._groupByExpression
	
	def getDuration(self):
		if not self._duration:
			raise AnalysisDurationNullError
		return self._duration
	
	def getDurationTimeUnit(self):
		if not self._durationTimeUnit:
			raise AnalysisDurationTimeUnitNullError
		return self._durationTimeUnit
	
	def getStartDate(self):
		if not self._startDate:
			raise AnalysisStartDateNullError
		return self._startDate
		

class MESanalysis():
	def __init__(self, analysisData):
		if not isinstance(analysisData, MESanalysisData):
			raise AnalysisDataTypeError(type(analysisData))
		self.__analysisData = analysisData
		self.__settings = system.mes.analysis.createMESAnalysisSettings("customAnalysis")
		self.__settings.setDataPoints(analysisData.getDatapoints())
		self.__addParameters(analysisData.getParameters())
		self.__settings.setFilterExpression(analysisData.getFilterExpression())
		self.__settings.setGroupBy(analysisData.getGroupByExpression())
		
		try:
			analysisData.getDuration()
			self.__setStartDayFunction(analysisData.getDurationTimeUnit())
			self.__getStartDate = self.__getStartDateByDuration
		except (AnalysisDurationNullError, AnalysisDurationTimeUnitNullError):
			self.__getStartDate = self.__getStartDateDirectly

		
	def __addParameters(self, parameters):
		for parameter in parameters:
			self.__settings.addParameter(parameter)
	
	def __setStartDayFunction(self, durationTimeUnit):
		if durationTimeUnit == "minutes":
			self.__getStartDateDeltaFcn = system.date.addMinutes
		elif durationTimeUnit == "hours":
			self.__getStartDateDeltaFcn = system.date.addHours
		elif durationTimeUnit == "days":
			self.__getStartDateDeltaFcn = system.date.addDays
	
	def __getStartDateByDuration(self):
		return self.__getStartDateDeltaFcn(system.date.now(), -self.getAnalysisData().getDuration())
	
	def __getStartDateDirectly(self):
		return self.getAnalysisData().getStartDate()
	
	def getAnalysisData(self):
		return self.__analysisData
		
	def execute(self, dynamicParams={}):
		allParameters = dict(self.getAnalysisData().getParameters().items() + dynamicParams.items())
		endDate = system.date.now()
		startDate = self.__getStartDate()
		data = system.mes.analysis.executeAnalysis(startDate, endDate, self.__settings, allParameters).getDataset()
		if not data:
			raise AnalysisResultsNullError
		if data.rowCount <= 0:
			raise AnalysisNoResultsError
		results = {}
		#sprawdzic wplyw w Bascharage
		for row in range(0,data.getRowCount()):
			for datapoint in self.getAnalysisData().getDatapoints():
				results[row][datapoint] = data.getValueAt(row, datapoint)
		return results