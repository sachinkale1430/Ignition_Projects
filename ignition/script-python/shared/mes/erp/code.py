#======================================================================
# Add a custom property, using MESObject, MESObjectType, "name", "description", and value
# Search for custom property and create if it does not exist
#======================================================================
def addUpdateCustomProperty(mes_object, cp_name, cp_description, cp_value):
	#This will only return the custom properties that have been added to the material definition.
	cps = mes_object.getCustomProperties()
	for cp in cps.keySet():
		if cp == cp_name:    #Find the first one that you want to override
			#print "Custom property - %s already exists. Setting it to %s" %(cp_name, cp_value)
			mes_object.setPropertyValue(cp_name, cp_value)
			mes_object.setCustomPropertyDescription(cp_name, cp_description)
			system.mes.saveMESObject(mes_object)    #Don't forget to save
			return mes_object
	# Can not find, add custom property
	mes_object.addCustomProperty(cp_name, 'String', cp_description, '', True, False, cp_value)
	system.mes.saveMESObject(mes_object)
	return mes_object

#======================================================================
# CLEAN ALL CUSTOM PROPERTIES of an object type with a certain name
#======================================================================
def clean_All_customProperties_byName(mes_name, mes_object_type):
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName( mes_object_type )
	filter.setMESObjectNamePattern( mes_name)
	search = system.mes.loadMESObjects(filter)
	if len(search) > 0:
		obj = search[0]
		properties = obj.getCustomProperties()
		for property in properties:
			obj.removeCustomProperty(property)
		system.mes.saveMESObject(obj)   


#=====================================================================
# FIND OBJECT BY NAME
#=====================================================================
def find_object_by_name(object_type, object_name):
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName(object_type)
	filter.setMESObjectNamePattern(object_name)
	return system.mes.loadMESObjects(filter)
	
#=====================================================================
# FIND OBJECT BY CUSTOM PROPERTY
#=====================================================================
def find_object_by_custom_property(object_type,cp_name,cp_value):
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName(object_type)
	list = system.mes.object.filter.parseCustomPropertyValueFilter(cp_name + " = " + cp_value)
	filter.setCustomPropertyValueFilter(list)
	return system.mes.searchMESObjects(filter)
	
#=====================================================================
# FIND EQUIPMENT THAT HAS THE CERTAIN WORK CENTER IN CUSTOM PROPERTY
#=====================================================================
def getEquipmentFromSAPWorkCenter(SAPWorkCenter):
	result = find_object_by_custom_property('Line,Equipment,LineCell,LineCellGroup','sap_work_center',SAPWorkCenter)
	if result:
		if len(result)>1:
			return "More than one equipment with SAP code" + str(SAPWorkCenter)
		else:
			return result[0]
	else:
		return "Equipment with this SAP code: "+ str(SAPWorkCenter) +" does not exists"

	
#======================================================================
# ADD CUSTOM PROPERTY TO MESObject "Site" by name
# DO NOT USE IT OLD FUNCTIONS
# 1st - Look by custom property
# 2nd - Look by name
# 3rd - Error if not found
#======================================================================
def deprecated_add_site_customProperty(sap_site):
	logger = project._const.get_mes_logger()
	# Try finding by custom property.
	search = find_object_by_custom_property("Site", "sap_plant", sap_site)
	if len(search) == 0:
		# Try finding by name.
		search = find_object_by_name("Site",sap_site)
		if len(search) > 0:
			site_object = addUpdate_customProperty(search[0], "Site", "sap_plant", "SAP Plant code (WERKS)", sap_site)
			logger.info("Created custom property 'sap_plant' on site: " + site_object.getName())
			return site_object
		else:
			logger.warn("ERROR : Site " + sap_site + " does not exist in production model")
			return None
	return search[0]

#======================================================================
# ADD CUSTOM PROPERTY TO MESObject "Area" by name
# DO NOT USE IT OLD FUNCTIONS
# 1st - Look by custom property
# 2nd - Look by name
# 3rd - Error if not found
#======================================================================
def deprecated_add_area_customProperty(sap_site, sap_prod_step):
	# TO DO : ADD filter on site as well !
	logger = project._const.get_mes_logger()
	search = find_object_by_custom_property("Area", "sap_prod_step", sap_prod_step)
	if len(search) == 0:
		search = find_object_by_name("Area", sap_prod_step)
		if len(search) > 0:
			area_object = addUpdate_customProperty(search[0], "Area", "sap_prod_step", "SAP Production step (VORNR)", sap_prod_step)
			logger.info("Created custom property 'sap_prod_step' on area: " + area_object.getName())
			return area_object
		else:
			logger.warm("ERROR : Area " + sap_prod_step + " does not exist in production model")
			return None
	return search[0]
	
#======================================================================
# ADD CUSTOM PROPERTY TO MESObject "Line" by name
# DO NOT USE IT OLD FUNCTIONS
# 1st - Look by custom property
# 2nd - Look by name
# 3rd - Error if not found
#======================================================================
def deprecated_add_line_customProperty(sap_site, sap_prod_step, sap_work_center):
	# TO DO : ADD filter on site and production step as well !
	logger = project._const.get_mes_logger()
	search = find_object_by_custom_property("Line","sap_work_center", sap_work_center)
	if len(search) == 0:
		search = find_object_by_name("Line", sap_work_center)
		if len(search) > 0:
			line_object = addUpdate_customProperty( search[0], "Line", "sap_work_center", "SAP Work-center (ARBPL)", sap_work_center)
			logger.info("Created custom property 'sap_prod_step' on line: " + line_object.getName())
			return line_object
		else:
			logger.warn("ERROR : Line " + sap_work_center + " does not exist in production model")
			return None
	return search[0]