#======================================
# auto log out script 
#======================================
def autoLogOut():
	if system.util.getInactivitySeconds() > 1200:
		system.security.lockScreen()
		


