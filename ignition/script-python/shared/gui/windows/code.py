# ==============================================
# shared.gui.windows
# Last update: 2019-04-17
# Modified by: Rok Zupan
# ==============================================

def getParentWindowPath(event):
	
	window = system.gui.getParentWindow(event)
	windowPath = window.path
	windowPath = windowPath.rsplit('/', 1)[0]

	return windowPath
	