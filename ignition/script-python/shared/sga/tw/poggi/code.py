# POGGI BASED
# 2021-05-28
# Author: PFCE

def diagnosticsChanged(tagPath, value):
	def MSB(n):
		ndx = 0
		while ( 1 < n ):
			n = ( n >> 1 )
			ndx += 1
		return ndx
			
	tagPathParts = tagPath.split("/")
	
	# If value has changed to zero, basically nothing to do
	# (state is change automatically to run when a cycle is finished)
	if value == False:
		return
	
	# First we want to make sure that machine is running
	# If not, then we will not do anything
	ignitionTagPath =  tagPathParts[0] + "/" + tagPathParts[1] + "/" + tagPathParts[2] + "/" + tagPathParts[3]
	
	prod_tempStateCode = system.tag.read(ignitionTagPath + "/press/cycles/current/prod_tempStateCode").value
	
	# exit if it is not the 1st alarm
	if prod_tempStateCode is not None:
		return
	
	# calculation of the byte offset id
	tagName  = tagPathParts[-1]
	byteId = int(tagName.replace("diag", ""))
	
	# if nothing found, we exit
	if byteId < 1:
		return
		
	# calculation of the state code based on byteId
	state = 10000 + (byteId * 10)

	system.tag.write(ignitionTagPath + "/press/cycles/current/prod_tempStateCode", state)