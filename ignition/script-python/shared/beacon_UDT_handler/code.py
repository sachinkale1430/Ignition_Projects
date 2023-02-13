def beacon_UDT_check(mac, json):
	beaconName = beacon_UDT_exists(mac)
	if not beaconName:
		beacon_UDT_create(mac, json)
	else:
		beacon_UDT_update(mac, json, beaconName)


	#if not system.tag.exists("[default]beacons/" + mac):
	#	beacon_UDT_create(mac, json)
	#else:
	#	beacon_UDT_update(mac, json)



def beacon_UDT_create(mac, json):
	system.tag.addTag(parentPath="beacons", name=mac, tagType="UDT_INST", attributes={"UDTParentType":"beacons/beaconV1"})
	beacon_UDT_update(mac, json, mac)



def beacon_UDT_exists(mac):
	beacons = system.tag.browseTags(parentPath="beacons", tagType="UDT_INST", udtParentType="beacons/beaconV1")
	for beacon in beacons:
		macAddress = system.tag.read(beacon.fullPath + "/macAddress").value
		if mac == macAddress:
			return beacon.name



def beacon_UDT_update(mac, json, beaconName):
	prefix = "[default]beacons/" + beaconName
	
	values = [
		json["rssi"], 
		json["acceleration"], 
		json["acceleration_x"],
		json["acceleration_z"], 
		json["acceleration_y"], 
		json["battery"], 
		json["humidity"], 
		json["pressure"], 
		json["temperature"], 
		json["hostname"],
		mac
		]
	
	tags = [
		prefix + "/rssi", 
		prefix + "/acc", 
		prefix + "/accX", 
		prefix + "/accZ", 
		prefix + "/accY", 
		prefix + "/battery", 
		prefix + "/humidity", 
		prefix + "/pressure", 
		prefix + "/temperature",
		prefix + "/factoryClientID",
		prefix + "/macAddress"
		]
	
	system.tag.writeAll(tags,values)
	