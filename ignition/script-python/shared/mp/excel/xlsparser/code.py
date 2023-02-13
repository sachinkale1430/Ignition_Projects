#import shared.mp.excel.xlsparser as Xlrd
#
#planNgTable = Xlrd.ExcelTable(
#	Xlrd.MEStag("ExcelParser/PlanNG/PlanNG_data"),
#	"Plan_NG",
#	[[2,7],[10,8]],
#	[[2,8],[10,-1]],
#	Xlrd.MEStag("ExcelParser/PlanNG/PlanNG_params"))
#planNgB1 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/B1"),
#	"Plan_NG",
#	[1,0]
#)
#planNgB2 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/B2"),
#	"Plan_NG",
#	[1,1]
#)
#planNgB4 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/B4"),
#	"Plan_NG",
#	[1,3]
#)
#planNgE2 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/E2"),
#	"Plan_NG",
#	[4,1]
#)
#planNgE3 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/E3"),
#	"Plan_NG",
#	[4,2]
#)
#planNgE4 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/E4"),
#	"Plan_NG",
#	[4,3]
#)
#planNgE5 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/E5"),
#	"Plan_NG",
#	[4,4]
#)
#planNgF2 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/F2"),
#	"Plan_NG",
#	[5,1]
#)
#planNgF3 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/F3"),
#	"Plan_NG",
#	[5,2]
#)
#planNgF4 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/F4"),
#	"Plan_NG",
#	[5,3]
#)
#planNgJ1 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/J1"),
#	"Plan_NG",
#	[9,0]
#)
#planNgJ3 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanNG/J3"),
#	"Plan_NG",
#	[9,2]
#)
#
#planPpTable = Xlrd.ExcelTable(
#	Xlrd.MEStag("ExcelParser/PlanPP/PlanPP_data"),
#	"Plan_PP",
#	[[0,4],[16,5]],
#	[[0,5],[16,-1]],
#	Xlrd.MEStag("ExcelParser/PlanPP/PlanPP_params"))
#planPpB1 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanPP/B1"),
#	"Plan_PP",
#	[1,0]
#)
#planPpB2 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanPP/B2"),
#	"Plan_PP",
#	[1,1]
#)
#planPpB3 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanPP/B3"),
#	"Plan_PP",
#	[1,2]
#)
#planPpP1 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanPP/P1"),
#	"Plan_PP",
#	[15,0]
#)
#planPpP2 = Xlrd.ExcelField(
#	Xlrd.MEStag("ExcelParser/PlanPP/P2"),
#	"Plan_PP",
#	[15,1]
#)
#
#parser = Xlrd.XLSparser(
#	"C:\Users\OPLM5401682\Desktop\Plan Norgrip & PostPrint 2021.xls",
#	[planNgTable,planPpTable],
#	[planNgB1,planNgB2,planNgB4,planNgE2,planNgE3,planNgE4,planNgE5,planNgF2,planNgF3,planNgF4,planNgJ1,planNgJ3,planPpB1,planPpB2,planPpB3,planPpP1,planPpP2])
#
#parser.parse()

import shared.mp.external.xlrd.__init__ as xlrd
from shared.mp.general.tags import *
from shared.mp.excel.exceptions import *
import datetime


class CellFormatter:
	def _getFormattedCellValue(self, file, cell):
		format = file.xf_list[cell.xf_index]
		formatKey = format.format_key
		f = file.format_map[formatKey]
		if f.format_str[-1] == "%":
			return str((cell.value*100)).decode("utf8")+"%"
		elif f.format_str == "m/d/yy":
			dateTuple = xlrd.xldate_as_tuple(cell.value, file.datemode)
			return str(datetime.datetime(dateTuple[0], dateTuple[1], dateTuple[2], dateTuple[3], dateTuple[4], dateTuple[5]))
		elif f.format_str.split(".")[0] == "0" and len(f.format_str) > 1:
			formatSplitted = f.format_str.split(".")
			if len(formatSplitted) > 1:
				floatingNumber = len(formatSplitted[1])
				return str(round(float(cell.value),floatingNumber))
		elif len(f.format_str.split(".")) <= 1 and isinstance(cell.value,float):
			return str(int(round(cell.value)))
		else:
			return str(cell.value).decode("utf8")	


class ExcelField(CellFormatter):
	def __init__(self, tag, sheetName, cellAddress):
		if not isinstance(tag, MEStag):
			raise WrongTypeError(MEStag, type(tag))
		self._tag = tag
		if not isinstance(sheetName, str):
			raise WrongTypeError(str, type(sheetName))
		self._sheetName = sheetName
		if not cellAddress[0] >= 0 or not cellAddress[1] >= 0:
			raise WrongCellAddressError
		self._cellAddress = cellAddress
	
	def getCellRow(self):
		return self._cellAddress[1]
	
	def getCellCol(self):
		return self._cellAddress[0]
	
	def getTag(self):
		return self._tag

	def getSheetName(self):
		return self._sheetName
		
	def parse(self, file):
		tag = self.getTag()
		sheet = file.sheet_by_name(self.getSheetName())
		cell = sheet.cell(rowx=self.getCellRow(), colx=self.getCellCol())
		try:
			cellValue = self._getFormattedCellValue(file, cell)
		except ValueError:
			cellValue = cell.value
		tag.setValue(str(cellValue))
		 

class ExcelTable(CellFormatter):
	def __init__(self, tag, sheetName, titleCellsRange=None, contentCellsRange=None, paramsTag=None, headersMaxOneLineChars=4, contentMaxOneLineChars=7):
		if not isinstance(tag, MEStag):
			raise WrongTypeError(MEStag, type(tag))
		self._tag = tag
		if not isinstance(sheetName, str):
			raise WrongTypeError(str, type(sheetName))
		self._sheetName = sheetName
		if paramsTag != None:
			if not isinstance(paramsTag, MEStag):
				raise WrongTypeError(MEStag, type(paramsTag))
		self._paramsTag = paramsTag
		if not isinstance(headersMaxOneLineChars, int):
			raise WrongTypeError(int, type(headersMaxOneLineChars))
		self._headersMaxOneLineChars = headersMaxOneLineChars
		if not isinstance(contentMaxOneLineChars, int):
			raise WrongTypeError(int, type(contentMaxOneLineChars))
		self._contentMaxOneLineChars = contentMaxOneLineChars
		if titleCellsRange[1][0] <= titleCellsRange[0][0] or contentCellsRange[1][0] <= contentCellsRange[0][0] or titleCellsRange[1][1] <= titleCellsRange[0][1] or (contentCellsRange[1][1] != -1 and contentCellsRange[1][1] <= contentCellsRange[0][1]):
			raise WrongCellsRange("Wrong cells ranges defined.")
		if (titleCellsRange[1][0] - titleCellsRange[0][0]) != (contentCellsRange[1][0] - contentCellsRange[0][0]):
			raise WrongCellsRange("Title columns number not equal to content columns number.")
		self._titleCellsRange = titleCellsRange
		self._contentCellsRange = contentCellsRange
		self._colorMap = {
			60 : "#C00000", #brown
			10 : "#FF0000", #red
			51 : "#FFC000", #orange
			13 : "#FFFF00", #yellow
			50 : "#92D050", #light green
			17 : "#00B050", #green
			40 : "#00B0F0", #cyan (light blue)
			30 : "#0070C0", #blue
			56 : "#002060", #dark blue
			36 : "#7030A0", #purple
			8 : "#000000", #black
			9 : "#FFFFFF" #white
		}
		self._transparentColorIndex = 64
		self._defaultBgColor = "#FFFF00"
		self._defaultFgColor = "#000000"
		self._cellsParams = {}
	
	def _createHeaders(self, sheet):
		startCell = self.getTitleCellsRange()[0]
		endCell = self.getTitleCellsRange()[1]
		headers = []
		for col in range(startCell[0],endCell[0]):
			for row in range(startCell[1],endCell[1]):
				title = self._makeMultiline(sheet.cell_value(rowx=row, colx=col), self._headersMaxOneLineChars)
				headers.append(str(title).decode("utf8"))
		return headers
	
	def _makeMultiline(self, text, charsLimit):
		textSplitted = text.split(" ")
		text = "<html>"
		lineLen = 0
		for word in textSplitted:
			lineLen += len(word)
			if lineLen >= charsLimit:
				text += word+"<br>"
				lineLen = 0
			else:
				text += word+" "
		text += "</html>"
		return text
	
	def _createContent(self, file, sheet):
		startCellCol = self.getContentCellsRange()[0][0]
		startCellRow = self.getContentCellsRange()[0][1]
		endCellCol = self.getContentCellsRange()[1][0]
		endCellRow = self.getContentCellsRange()[1][1]
		if endCellCol == -1:
			endCellCol = sheet.ncols
		if endCellRow == -1:
			endCellRow = sheet.nrows
		content = []
		for row in range(startCellRow, endCellRow):
			rowContent = []
			for col in range(startCellCol, endCellCol):
				cell = sheet.cell(rowx=row, colx=col)
				try:
					cellValue = self._makeMultiline(self._getFormattedCellValue(file, cell), self._contentMaxOneLineChars)
				except ValueError:
					cellValue = self._makeMultiline(cell.value, self._contentMaxOneLineChars)
				self._setCellParams(file, cell, row - startCellRow, col - startCellCol)
				rowContent.append(cellValue)
			content.append(rowContent)
		return content
	
	def _setCellParams(self, file, cell, row, col):
		if self.getParamsTag():
			format = file.xf_list[cell.xf_index]
			font = file.font_list[format.font_index]
			bg = format.background.pattern_colour_index
			fg = font.colour_index
			paramKeyName = "%s,%s"%(row, col)
			colors = self._getColorMap()
			params = self._getCellsParams()
			if not paramKeyName in params:
				params[paramKeyName] = {}
			if bg in colors:
				params[paramKeyName]["bg"] = colors[bg]
			elif bg != self._transparentColorIndex:
				params[paramKeyName]["bg"] = self._defaultBgColor
			if fg in colors:
				if not paramKeyName in params:
					params[paramKeyName] = {}
				params[paramKeyName]["fg"] = colors[fg]
			else:
				params[paramKeyName]["fg"] = self._defaultFgColor
	
	def _getColorMap(self):
		return self._colorMap
			
	def _getCellsParams(self):
		return self._cellsParams
	
	def _createDataset(self):
		headers = self.getHeaders()
		content = self.getContent()
		return system.dataset.toDataSet(headers, content)
	
	def _writeDataset(self):
		dataset = self.getDataset()
		tag = self.getTag()
		tag.setValue(dataset)
	
	def _writeParams(self):
		paramsTag = self.getParamsTag()
		if paramsTag != None:
			cellParams = self._getCellsParams()
			paramsTag.setValue(str(cellParams))
			
	def getPath(self):
		return self._path
	
	def getTag(self):
		return self._tag
	
	def getParamsTag(self):
		return self._paramsTag
	
	def getFile(self):
		return self._file
	
	def getSheetName(self):
		return self._sheetName
	
	def getTitleCellsRange(self):
		return self._titleCellsRange
	
	def getHeaders(self):
		return self._headers
	
	def getContentCellsRange(self):
		return self._contentCellsRange
		
	def getContent(self):
		return self._content
	
	def getDataset(self):
		return self._dataset
	
	def parse(self, file):
		sheet = file.sheet_by_name(self.getSheetName())
		self._headers = self._createHeaders(sheet)
		self._content = self._createContent(file, sheet)
		self._dataset = self._createDataset()
		self._writeDataset()
		self._writeParams()


class XLSparser:
	def __init__(self, filePath, excelTables=[], excelFields=[]):
		if not isinstance(filePath, str):
			raise WrongTypeError(str, type(filePath))
		self._path = filePath
		self._file = xlrd.open_workbook(filename=filePath, formatting_info=True)
		self._excelTables = excelTables
		self._excelFields = excelFields
	
	def getTables(self):
		return self._excelTables
	
	def getFields(self):
		return self._excelFields
	
	def getPath(self):
		return self._path
	
	def getFile(self):
		return self._file
	
	def parseTables(self):
		tables = self.getTables()
		file = self.getFile()
		for table in tables:
			table.parse(file)
	
	def parseFields(self):
		fields = self.getFields()
		file = self.getFile()
		for field in fields:
			field.parse(file)
		
	def parse(self):
		self.parseTables()
		self.parseFields()