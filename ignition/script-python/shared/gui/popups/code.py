def closeAnyOpenedPopup():
	openedWindows = system.gui.getOpenedWindowNames()
	for window in openedWindows:
		try:
			windowName = window.split("/")[-1]
			if "popup" in windowName:
				system.nav.closeWindow(window)
		except:
			pass