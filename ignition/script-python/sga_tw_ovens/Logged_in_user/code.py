def retrieveMailAddress(userDataSet):
	# Take userDataSet and retrieve the user's email address from it
	#
	userPyDataSet = system.dataset.toPyDataSet(userDataSet)
	user = system.tag.read('[System]Client/User/Username').value
	
	for row in userPyDataSet:
		tempUser = row[0]
		if tempUser == user:
			email=row[3]
			break
	if len(email) == 0:
		email == ''
	
	return email