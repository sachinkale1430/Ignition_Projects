now = system.date.now()	
def ti(start,x):  
    return system.date.addHours(start,int(x))

def time_to_total(inData):
	sumTime = []						
	for x in range(len(inData)): sumTime.append(sum(inData[0:x+1]))						
	return sumTime
	
def list_to_Dist(list,start):						
	data = {'seg_'+str(x) : [ti(start, 0 if x==0 else list[x-1]),
	                         ti(start, list[x])
	                        ] for x in range(len(list))}					
	return data	

def whatSeg(dt,data):   						
	seg = []					
	for x in range(len(data)):					
	    dt1 = data['seg_'+str(x)][0]					
	    dt2 = data['seg_'+str(x)][1]									
	    if system.date.isBetween(dt,dt1,dt2) == 1 : seg.append(x)					
	try : return max(seg)					
	except : return ''

def dist_to_Dataset(dict,time):
                                                         
	Header = [head for head in dict.keys()]
	Header.insert(len(Header)+1, "Total")
	Header.insert(0, "t_stamp")
	rows = []
	for x in range(len(dict[list(dict.keys())[0]])):
		row = []
		for key in dict.keys():
		    row.append(dict[key][x])
		row.append(sum(row))
		row.insert(0,ti(time,x))  
		rows.append(row)
	return system.dataset.toDataSet(Header,rows) 
	  
def mainFunc(rec_id, selectedTime):

#		rec_id  = 2
		t_stamp = selectedTime #system.date.addHours(now,0)
		RunningOvens = system.db.runQuery('''SELECT concat('Oven ',oven_no), start_dt, fk_rec_ovprd_id, fk_ovms_ovprd_id FROM  ovprd_ovenproduction 
												inner join  ovms_ovenmaster on fk_ovms_ovprd_id = ovms_id where fk_sta_ovprd_running_id = 1 or start_dt > now()
											union
												SELECT 'Selected Oven' , DATE_ADD(now(), INTERVAL -2 MINUTE), '%s', -2  as myColumn
												order by fk_ovms_ovprd_id desc '''%(rec_id))
												
		cur_rec_total_time = int(system.db.runQuery("SELECT sum(duration) FROM  recms_recipemaster where fk_rec_recms_id = '%s'"%(rec_id)).getValueAt(0,0))													
		ovenConsumingPower = {}
#		print 'NO of Ovens in Run  '+ str(RunningOvens)
		for receipe in RunningOvens :
				if receipe[3] != -2 : masterData = system.db.runQuery("SELECT duration, power FROM sim_simulation where fk_sim_ovms_id = '%s' order by segment asc"%(receipe[3])) 
				else :                masterData = system.db.runQuery("SELECT duration, power FROM recms_recipemaster where fk_rec_recms_id  = '%s' order by segment asc"%(receipe[2])) 
				
				rawDuration = [x[0] for x in masterData]              #[2,1,1,1] # receipe duration	
				start  = receipe[1]                                  
				converted = time_to_total(rawDuration)                # convert 1,1,2 to 1,2,4
				dist = list_to_Dist(converted,start)                  # Dictonary Convertion
		
				segList = []
				for time in range(cur_rec_total_time):
					dt = ti(t_stamp,time)
					cur_Segment = whatSeg(dt,dist)                    # on which Segment it is Running
					segList.append(cur_Segment)
					
				power = []	
				for curSegment in segList :                           # Maping current power against Segment
					if curSegment != '' : power.append(int(masterData.getValueAt(curSegment,1)))
					else : power.append(0) 
										   
				ovenConsumingPower.update({receipe[0]: power})
		
		finalDataset = dist_to_Dataset(ovenConsumingPower,t_stamp)
		return finalDataset
	


		
def findSeg(oven_id):	

	RunningOvens = system.db.runQuery('''SELECT start_dt, fk_rec_ovprd_id FROM  ovprd_ovenproduction where fk_ovms_ovprd_id = '%s' and fk_sta_ovprd_running_id = 1 ''' %(oven_id))

	for receipe in RunningOvens :
		masterData = system.db.runQuery("SELECT duration FROM  sim_simulation where fk_sim_ovms_id = '%s' order by segment asc"%(receipe[1])) 
		rawDuration = [x[0] for x in masterData]               #[2,1,1,1] # from tags its time	
		start  = receipe[0]                                  
		converted = time_to_total(rawDuration)                 # convert 1,1,2 to 1,2,4
		dist = list_to_Dist(converted,start)                   # Dictonary Convertion
		cur_Segment = whatSeg(now,dist)                        # on which Segment it is Running
		return cur_Segment
		


def visualChart(start, oven_id):
#	masterData = system.db.runQuery("SELECT duration, temp FROM sim_simulation where fk_sim_ovms_id = '%s' order by segment asc " %(oven_id))
	masterData = system.db.runQuery("SELECT duration, temp FROM  recms_recipemaster where fk_rec_recms_id = '%s' order by segment asc " %(oven_id))
	rawDuration = [x[0] for x in masterData] 
	converted = time_to_total(rawDuration) 
	minutes = [hr*60 for hr in converted] 
	Header = ['t_stamp', 'temperature']
	rows = []
	for x in range(masterData.getRowCount()):
		time = system.date.addMinutes(start, int(minutes[x])) 
		rows.append([time, masterData.getValueAt(x, 1)])
	return system.dataset.toDataSet(Header,rows)   #system.dataset.toPyDataSet()



		
def chart(start, oven_id):

#	start  = now
	masterData = system.db.runQuery("SELECT duration, temp FROM  sim_simulation where fk_sim_ovms_id = '%s' order by segment asc " %(oven_id))
	rawDuration = [x[0] for x in masterData] 
	converted = time_to_total(rawDuration)           
	dist = list_to_Dist(converted,start)
      
	segList = []
#	for time in range(int(sum(rawDuration))):
	for time in range(int(sum(rawDuration)+1)):
		dt = ti(start, time)
		cur_Segment = whatSeg(dt,dist)                    # on which Segment it is Running
		segList.append(cur_Segment)
	
	temp = []	
	for curSegment in segList :                           # Maping current power against Segment
		if curSegment != '' : temp.append(int(masterData.getValueAt(curSegment,1)))
		else : temp.append(0) 
		
	Header = ['t_stamp', 'temperature']
	rows = []
	for setPt in range(len(temp)) :
		dt = ti(start, setPt)
		rows.append([dt,temp[setPt]])	
	return system.dataset.toDataSet(Header,rows)

			