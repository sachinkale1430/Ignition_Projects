def Retrieve_data():
	query= "Select prod_date, msnumber,run,grit,cap,op,prod_length,speed,ra_left,ra_right,rq_left,rq_right,height_right,height_left,nipgap_right, nipgap_left,minra, maxra, commentaire from data_roughness Order by (prod_date) DESC LIMIT 1"
	PyDataset= system.db.runQuery ( query , "ext_maker_rugosity")
	json= shared.json.export.datasetToJson(PyDataset)
	return (json)
	
