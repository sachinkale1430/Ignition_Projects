# ==============================================
# shared.gui.menu
# Last update: 2018-12-06
# Modified by: Rok Zupan
# ==============================================

def menuProperties(event, menu, titles):
	#-----------------------------------------------
	# import java functions
	#-----------------------------------------------
	from javax.swing import JMenuItem, JSeparator, JPopupMenu
	from java.awt import Font, Color, Dimension
	from java.awt.font import FontRenderContext, GlyphVector
	#from com.inductiveautomation.ignition.client.images import PathIcon			
	
	
	#-----------------------------------------------
	# Main Menu properties
	#-----------------------------------------------
	mainMenuLevel1Font = Font("Open Sans", Font.PLAIN, 18)
	#frc = g2.getFontRenderContext()

	
	# Level 1 properties
	for count in range(len(titles)):
		menu.getComponent(count).setPreferredSize(Dimension(200,50))
		menu.getComponent(count).getPreferredSize()
		menu.getComponent(count).setBorder(None)
		menu.getComponent(count).setFont(mainMenuLevel1Font)
		menu.getComponent(count).setBackground(system.gui.color("21353f"))
		menu.getComponent(count).setForeground(system.gui.color("c9cece"))
		menu.getComponent(count).setOpaque(True)
		
	menu.setBorder(None)
	menu.setBackground(system.gui.color("21353f"))
	menu.insert(JPopupMenu.Separator(),1)
	
	
	return menu



def userMenu(event):
	#-----------------------------------------------
	# import java functions
	#-----------------------------------------------
	from javax.swing import JMenuItem
	from javax.swing import JSeparator
	from javax.swing import SwingConstants
	from java.awt import Graphics2D, RenderingHints



	#-----------------------------------------------
	# screen functions
	#-----------------------------------------------
	def logOut(event):
		system.security.logout()
			
	def lockScreen(event):
		system.security.lockScreen()

	def clientDiagnostic(event):
		system.gui.openDiagnostics()

	#g2 = event.graphics
	rh = RenderingHints(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

		
	
	#-----------------------------------------------------
	# building menu structure
	#-----------------------------------------------------
	# mainMenu - Level 1
	titlesMenuLevel1 = ["Client diagnostics", "Log Out", "Lock Screen"]
	
	functionsMainMenuLevel1 = [clientDiagnostic, logOut, lockScreen]
	
	
	#-----------------------------------------------------
	# create Main Menu
	#-----------------------------------------------------
	userMenu = system.gui.createPopupMenu(titlesMenuLevel1, functionsMainMenuLevel1)
	userMenuProperties = menuProperties(event, userMenu, titlesMenuLevel1)
	
	return userMenu
	