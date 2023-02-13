def setTableDesign(table, tableType):
	from java.awt import Font, Dimension
	
	# setting up header
	# NEED TO OPTIMIZE CODE!!!
	
	if tableType == "Table":
		tableHeader = table.getTableHeader()
		
	elif tableType == "PowerTable":
		tableHeader = table.table.getTableHeader()
	
	# setting up header		
	table.headerFont = Font('Dialog', 0, 14)
	table.table.tableHeader.background = project._const.tableHeaderBackground	
	table.table.tableHeader.foreground = project._const.tableHeaderForeground

	dim = tableHeader.getPreferredSize()
	tableHeader.setPreferredSize(Dimension(int(dim.getWidth()), project._const.tableHeaderHeight))
	tableHeader.revalidate()

	# setting up table
	table.viewport.background = project._const.tableViewportBackground
	table.background = project._const.tableBackground
	table.foreground = project._const.tableFontColor
	table.selectionBackground = project._const.tableSelectionBackground
	
	# setting up grid
	table.gridColor = project._const.tableGridColor
		
	# disable table border
	table.border = None
	
	
def setChatTableDesign(table):
	from java.awt import Font, Dimension
	
	tableViewportBackground = system.gui.color("#172831")
	tableBackground = system.gui.color("#172831")
	tableSelectionBackground = system.gui.color("#CCCCCC")
	tableGridColor = system.gui.color("#BBBBBB")
	tableFontColor = system.gui.color("#2B2B2B")
	
	
	tableHeader = table.table.getTableHeader()
	
	dim = tableHeader.getPreferredSize()
	tableHeader.setPreferredSize(Dimension(int(dim.getWidth()), project._const.tableHeaderHeight))
	tableHeader.revalidate()

	# setting up table
	table.viewport.background = tableViewportBackground
	table.background = tableBackground
	table.foreground = tableFontColor
	table.selectionBackground = tableSelectionBackground
	
	# setting up grid
	table.gridColor = tableGridColor
		
	# disable table border
	table.border = None



def setDataTableDesign(table, bgColor):
	from java.awt import Font, Dimension
	from javax.swing import BorderFactory
	
	from javax.swing.BorderFactory import createEmptyBorder
	from javax.swing import UIManager
	from javax.swing.border import LineBorder
	from javax.swing import SwingConstants 
	
	
	tableViewportBackground = system.gui.color(bgColor)
	tableBackground = system.gui.color(bgColor)
	tableSelectionBackground = system.gui.color("#21353f")
	tableGridColor = system.gui.color("#4d565b")
	tableFontColor = system.gui.color("#ffffff")
	
	
	tableHeader = table.table.getTableHeader()
	
	tableHeader.setOpaque(False)
	tableHeader.setBackground(tableBackground)
	tableHeader.setForeground(tableFontColor)
		
	tableHeader.setBorder(LineBorder(tableBackground))
	UIManager.getDefaults().put("TableHeader.cellBorder" , BorderFactory.createEmptyBorder(0,0,0,0))
	#table.getTableHeader().setAlignmentX(JLabel.LEFT)
	tableHeader.setAlignmentX(SwingConstants.LEFT)

	tableHeader.setFont(Font("Open Sans Semibold", Font.PLAIN, 16))

	# setting up table
	table.viewport.background = tableViewportBackground
	table.background = tableBackground
	table.foreground = tableFontColor
	table.selectionBackground = tableSelectionBackground
	
	# setting up grid
	table.gridColor = tableGridColor
		
	# disable table border
	table.setBorder(BorderFactory.createEmptyBorder())


def tableScrollBarManagement(table):	
	tableObject = table.getTable()
	
	tableHeight = table.getHeight()

	tableRowHeights = sum(tableObject.getRowHeights().getRowHeights())
	
	if tableRowHeights > tableHeight:
		return True
	#	system.gui.transform(table, table.x, table.y, int(table.width))
	else:
		return False
	#	system.gui.transform(table, table.x, table.y, int(table.width) + 50)		