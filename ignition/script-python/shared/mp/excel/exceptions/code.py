from shared.mp.general.exceptions import *

class WrongCellsRange(GeneralError):
	def __init__(self, message="Wrong cells range."):
		GeneralError.__init__(self, message)

class WrongCellAddressError(GeneralError):
	def __init__(self, message="Wrong cell address."):
		GeneralError.__init__(self, message)