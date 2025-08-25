#!/usr/bin/env python
# coding: utf-8
import psycopg2
import numpy
import datetime
import json
import calendar
import h5py
# from s100py import s104
import sys
import os
import re
import datetime
from s100py.s104.v2_0 import utils
from decimal import Decimal

def getAllDims(tbl,i):
    print('retrieving details from table ' , tbl, ' for id=',i)
    with conn.cursor() as curs:

        try:
            stmtx="select xmx from " + tbl + " where id = " + str(i)
            print(stmtx)
            curs.execute(stmtx)
            thisrow = curs.fetchall()[0][0]
            maxx=float(thisrow)
            print('maxx = ', maxx)

            stmtx="select xmn from " + tbl + " where id = " + str(i)
            print(stmtx)
            curs.execute(stmtx)
            thisrow = curs.fetchall()[0][0]
            minx=float(thisrow)
            print('minx = ', minx)

            stmtx="select ymin from " + tbl + " where id = " + str(i)
            print(stmtx)
            curs.execute(stmtx)
            thisrow = curs.fetchall()[0][0]
            ymin=float(thisrow)
            print('ymin = ', ymin)

            stmtx="select ymax from " + tbl + " where id = " + str(i)
            print(stmtx)
            curs.execute(stmtx)
            thisrow = curs.fetchall()[0][0]
            ymax=float(thisrow)
            print('ymax = ', ymax)

        except (Exception, psycopg2.DatabaseError) as error:
            print(x, " ERROR")
            print(error)
            return (-1,-1)

    #print(t)
    return(maxx,minx,ymin,ymax)

def getDims(conn,tbl):
    print('retrieving details for table ' , tbl)
    with conn.cursor() as curs:

        try:
            stmtx="select max(r) from " + tbl + " where c = 0"
            print(stmtx)
            curs.execute(stmtx)
            thisrow = curs.fetchall()[0][0]
            maxy=int(thisrow)
            maxy=maxy+1
            print('maxy = ', maxy)

            stmty="select max(c) from " + tbl + " where r = 0"
            curs.execute(stmty)
            thisrow = curs.fetchall()[0][0]
            maxx=int(thisrow)
            maxx=maxx+1
            print('maxx = ', maxx)
            
        except (Exception, psycopg2.DatabaseError) as error:
            print(x, " ERROR")
            print(error)
            return (-1,-1)

    #print(t)
    return(maxx,maxy)

def j(conn,xlimit,ylimit,rowoffset,coloffset,tbl):
    print('retrieving ' , xlimit , ' values across' , ylimit , ' rows')
    t = [ [0]*xlimit for i in range(ylimit)]
    with conn.cursor() as curs:

        try:
            for x in range(ylimit):
                row=x+rowoffset
                stmt="select array_agg(d) from (select id,r,c,d from " + tbl + " where r = " + str(row) + " and c >= " + str(coloffset) + " order by c limit " + str(xlimit) + ") e"
                #print(stmt)
                curs.execute(stmt)
                thisrow = curs.fetchall()[0][0]

                if x==0:
                    print('->',stmt)
                    print(len(thisrow))
                    
                #t[ylimit-x-1]=thisrow
                #t[x-1]=thisrow
                t[row]=thisrow
        
            
        except (Exception, psycopg2.DatabaseError) as error:
            print(x, " ERROR")
            print(error)
            return -1

    #print(t)
    data = numpy.asarray(t)
    print('shp=',numpy.shape(data))
    return(data)

def matchvalsfromfile(dir,fn,match):

    def create_date_regex(match_str):
        match = re.match(r"(\d{4})-\((\d{4})\|(\d{4})\)", match_str)
        if not match:
            raise ValueError("Invalid match string format")
        year = match.group(1)
        start = match.group(2)
        end = match.group(3)

        month = start[:2]
        start_day = int(start[2:])
        end_day = int(end[2:])

        days = "|".join(f"{d:02d}" for d in range(start_day, end_day + 1))
        return fr"{year}-{month}-({days})", int(year), int(month), start_day, end_day

    regex,year,month, start_day, end_day = create_date_regex(match)

    with open(f"{dir}/{fn}", 'r') as file1:
        Lines = file1.readlines()

    i = 0
    j = 0
    interval=0
    fnddt=0
    ret=[]    
    allvals = []
    alldts = []
    retdt = None
    # Strips the newline character
    for line in Lines:

        i += 1        
        # Surely match goes in here....
        #if i<20000:
        if i>0:
            #x = re.search('(\b(0[1-9]|1[0-2])[-/](0[1-9]|[12]\d|3[01])[-/](\d\d)\b)', line)            
            y = re.search(r'(.*) Minute', line)
            if y and interval == 0:
                # found time interval
                interval=60 * int(y.group(1))
                print("Interval->",y.group(1), " minutes")

            x = re.search(r'([0-9][0-9])/([0-9][0-9])/([0-9][0-9][0-9][0-9]) ([0-9]{2}):([0-9]{2}) ([0-9]{2}\.[0-9]{2})', line)
            if x:
                j+=1
                yr=int(x.group(3))
                mt=int(x.group(2))
                dy=int(x.group(1))
                hh=int(x.group(4))
                mm=int(x.group(5))
                vl=x.group(6)                
                #print("->" ,x.group(1)," ",x.group(2)," ",x.group(3))

                # this is what the s100py demo does, creating date time...
                dt = datetime.datetime(yr, mt, dy, hh, mm)   
                allvals.append(float(vl))
                alldts.append(dt)

                # if j==1:
                
                #     retdt = dt
                #     print('First datetime=',retdt)                
                # #datetime_interval = datetime.timedelta(seconds=3600)                
                # z = re.search(regex, str(dt))                                           
                # if z:                    
                #     fnddt=fnddt+1
                #     print(fnddt, ": Matched : ", dt ," = ",vl)
                #     ret.append(float(vl))
                #     if fnddt==1:
                #         print("Found regex for date -> ",dt," start with this date")
                #         retdt=dt

            else:
                print("WARNING: No luck parsing date from line ",line)


     # Identify the first index matching the regex
    match_start_idx = None
    for idx, dt in enumerate(alldts):
        if re.search(regex, str(dt)):
            match_start_idx = idx
            break
    if match_start_idx is not None:
        # Include previous 3 values if available
        start_idx = max(0, match_start_idx - 3)
        fnddt = 0
        for idx in range(start_idx, len(alldts)):
            dt = alldts[idx]
            if idx >= match_start_idx and not re.search(regex, str(dt)):
                break
            if fnddt == 0:
                retdt = dt
                print('First datetime =', retdt)
            fnddt += 1
            print(fnddt, ": Matched or buffer:", dt, "=", allvals[idx])
            ret.append(allvals[idx])
    else:
        print("No match found for regex in file.")

    return (retdt,ret)


try:
    conn = psycopg2.connect("dbname='NC_S102' user='postgres' host='devfarm' port='5999' password='postgres123#'")
except Exception as e:
    print(e)
    print("I am unable to connect to the database")

print("DB connected")


input_datafile = sys.argv[1]

with open(input_datafile) as config_file:
    data = json.load(config_file)
    comt = data['comt']
    print(comt)

print('done')

tbl = data['dbtable']
print(conn)
(nx,ny) = getDims(conn,str(tbl))
xlimit = nx
ylimit = ny

dimstbl = data['dimensionsTable']
dimid = data['PolygonID']
(xmax,xmin,ymin,ymax) = getAllDims(dimstbl,dimid)
print(xmax,xmin,ymin,ymax)

#cellx = (xmax-xmin)/2
#celly = (ymax-ymin)/2
#print('cellx=',cellx,' celly=',celly)
print('nx=',xlimit,' ny=',ylimit)

filename=data['filename']
rowoffset=0
coloffset=0

print("Sourcing grid from ",tbl)

# think we only have to do this once.
dataframe1 = j(conn,xlimit,ylimit,rowoffset,coloffset,tbl)


print('got dataframes')

tnd1   = [ [0]*xlimit for i in range(ylimit)]
trend1 = numpy.asarray(tnd1)

dir='txt'
testfile1=data['geographicIdentifier']
name = ''
name = filename
testfile=data['predictionfile']
#match_name= re.search(r'\d+_(.+?)_\d+', testfile)
# name = ''
# if match_name:
#     name = match_name.group(1)
#     name = name.replace('_','')

# mtch=data['DateMatchExpression']
mtch=sys.argv[4]
yr = mtch.split('-')[0]
match = match = re.match(r"(\d{4})-\((\d{4})\|(\d{4})\)", mtch)

#new date format 
issue_date_str = ''
month_abbr = ''
file_year = ''
if match:
    year = int(match.group(1))
    start_mmdd = match.group(2)
    end_mmdd = match.group(3)
    file_year = f'{year}{start_mmdd}_{year}{end_mmdd}'
    start_month = int(start_mmdd[:2])
    start_day = int(start_mmdd[2:])
    issue_date_str = f"{year:04d}{start_month:02d}{start_day:02d}"  
    month_abbr = calendar.month_abbr[start_month].upper()
print("Formatted issue date:", issue_date_str)

issue_date = datetime.datetime(2024, 5, 7, 14, 0, 0)
print('issue date',issue_date)

editionNumber = 0
purpose = ''
if yr == '2025':
    editionNumber = 1
    purpose = 'newDataset'
else:
    editionNumber = 2
    purpose = 'newEdition'

mtchs=str(mtch)
print('predictions ->' + testfile)

(dt1,allv) = matchvalsfromfile(dir,testfile,mtchs)
print('Date of first entry = ',dt1," Matched ", len(allv) , " values from ",testfile)

# Format the returned date of first record.
print('date of first record=',dt1)
sdt1="{:%Y%m%dT%H%M%SZ}".format(dt1)
data_series_time_001 = dt1
timeoffset=0
print('Adding time offset -> ' + str(timeoffset))
timeoff = datetime.timedelta(hours=timeoffset)
datetime_interval = datetime.timedelta(seconds=data['timeRecordInterval'])
data_series_time_001 = data_series_time_001 + 3*datetime_interval
sdt1 = data_series_time_001.strftime("%Y%m%dT%H:%M:%SZ")
#sdt1 = "{:%Y%m%dT%H%M%SZ}".format(data_series_time_001)

dd=sys.argv[3]
os.makedirs(dd,exist_ok=True)
fn = os.path.join(dd, f"{name}_{file_year}.h5")
file_path = os.path.join(dd,f"{name}_{file_year}.txt")
# fn=dd + '/' + f"{filename}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%SZ')}.h5"
print('Creating->',fn)
data_file = utils.create_s104(fn, 2)

grid_properties = {
        'maxx': xmax,
        'minx': xmin,
        'miny': ymin,
        'maxy': ymax,
        'cellsize_x': data['cellsize_x'],
        'cellsize_y': data['cellsize_y'],
        #'cellsize_x': cellx,
        #'cellsize_y': celly,
        'nx': xlimit,
        'ny': ylimit
}

issue_date = datetime.datetime(2024, 6, 13, 7, 0, 0)

datetime_forecast_issuance = dt1

# Example metadata
metadata = {
    'horizontalCRS': data['horizontalCRS'], #EPSG code
    'geographicIdentifier': data['geographicIdentifier'],
    'waterLevelHeightUncertainty': -1.0, # Default or Unknown values
    'verticalUncertainty': -1.0, # Default or Unknown values
    'horizontalPositionUncertainty': -1.0, # Default or Unknown values
    'waterLevelTrendThreshold': 0.2,
    'verticalCS': 6499, # EPSG code
    'verticalCoordinateBase': 2,
    'verticalDatumReference': data['verticalDatumReference'], # previous value 1
    'verticalDatum': data['verticalDatum'], # previous value 23
    'commonPointRule': 4, # 4:all
    'interpolationType': 10, # 10:discrete
    'dataDynamicity': 2, # 2:Astronomical Predictions(F)
    'datetimeOfFirstRecord': sdt1,
    'trendInterval': 60, # minutes
    # 'datasetDeliveryInterval': 'PT6H',
    'epoch': '2005',
    'verticalDatumEpoch': "1992 - 2011 LAT epoch", # previous value NOAA_NTDE_1983-2001
    "verticalDatum": data["verticalDatum"],  # earlier value 23
    'issueDate': issue_date_str,
    'dataOffsetCode' : 5
}

data_coding_format = 2

print(numpy.shape(dataframe1))
print(numpy.shape(trend1))

const=allv[3]
const0 = allv[0]
print('Using value for first dataframe ->',const, ' dt=',issue_date_str)
# Custom trend functions
def absval(x):
    result = (x**2)**0.5    
    return result  

# -1 -> | -1 | -> 1

def trend(x1, x2):    
    diff = x2 - x1    
    if absval(diff) >= 0.1:        
        if diff > 0:            
            return 2  # increasing
        else:       
            return 1 # decreasing
    else:   
        return 3 #steady
    
def mkmask(x):
    if x == 0:        
        #result = 1.0*-99.99
        #return result
        return numpy.nan              
    else:
        return 1.0*const
    
def mkmask0(x):
    if x == 0:        
        #result = 1.0*-99.99
        #return result
        return numpy.nan              
    else:
        return 1.0*const0

output_lines = []
output_lines.append( "date\t\tTime\tHeight(m)\tDifference(m)\tTrend")
# Vectorize the function
datamaskfn    = numpy.vectorize(mkmask)
datamaskfn0 = numpy.vectorize(mkmask0)
global_min = numpy.inf
global_max = -numpy.inf
maskddata0 = datamaskfn0(dataframe1)
maskeddata1 = datamaskfn(dataframe1)
valid_values1 = maskeddata1[~numpy.isnan(maskeddata1)]
if valid_values1.size > 0:
    global_min = min(global_min, valid_values1.min())
    global_max = max(global_max, valid_values1.max())
utils.add_metadata(metadata, data_file)
series_time1 = datetime_forecast_issuance + 3 * datetime_interval
datat3 = maskeddata1        
datat0 = maskddata0
trend0 = numpy.zeros_like(datat3,dtype=int)
for i in range(maskeddata1.shape[0]):
    for j in range(maskeddata1.shape[1]):
        val_n = datat0[i][j]
        val_n3 = datat3[i][j]
        if not (numpy.isnan(val_n) or numpy.isnan(val_n3)):
            trend0[i][j] = trend(val_n,val_n3)
        else:
            trend0[i][j] = 0                       
utils.add_data_from_arrays(datat3,trend0,data_file,grid_properties,series_time1,data_coding_format)
# for first value
date_part0 = series_time1.strftime("%d/%m/%Y")
time_part0 = series_time1.strftime("%H:%M") 
height0 = allv[3]
difference0 = allv[3] - allv[0]
diff_str0 = f"{difference0:.2f}"
trend_val0 = trend(allv[0], allv[3])   

output_lines.append(f"{date_part0}\t{time_part0}\t{height0:.2f}\t\t{diff_str0}\t\t{trend_val0}")
procgroup=1
maxgroups = int(sys.argv[2])
if maxgroups==0:
    print("No limit from command line, printing all values retrieved")
    maxgroups=len(allv)

print('retrieving ->',maxgroups,' groups')
last_date=sdt1
series_time2 = series_time1
avg_trend = 0
masked_groups = []
time_group = []

water_level_trend_threshold = 0
#for gp in range(maxgroups-1): 
for gp in range(maxgroups):      
    const=allv[gp]    
    maskeddata2 = datamaskfn(dataframe1)
    valid_values2 = maskeddata2[~numpy.isnan(maskeddata2)]
    if valid_values2.size > 0:
        global_min = numpy.float32(min(global_min, valid_values2.min()))
        global_max = numpy.float32(max(global_max, valid_values2.max()))
        prev_global_min = global_min
        prev_global_max = global_max
    else:
        global_min = prev_global_min
        global_max = prev_global_max
    
    #append all the maskedata and series time in masked group and time group
    masked_groups.append(maskeddata2)
    time_group.append(series_time2)
    
    if gp > 3:
        series_time2 = series_time2 + datetime_interval
        last_date=series_time2.strftime("%Y%m%dT%H:%M:%SZ")    
   
    if gp > 5:    
        data_n = masked_groups[gp-3]        
        data_n3 = masked_groups[gp]
        trend2 = numpy.zeros_like(data_n,dtype=int)

        for i in range(maskeddata2.shape[0]):
            for j in range(maskeddata2.shape[1]):                
                #trend2[i][j] = trend(data_n[i][j], data_n3[i][j])
                val_n = data_n[i][j]
                val_n3 = data_n3[i][j]
                if not (numpy.isnan(val_n) or numpy.isnan(val_n3)):
                    trend2[i][j] = trend(val_n,val_n3)
                else:
                    trend2[i][j] = 0  
        #for value afer condition 5 
        date_part = series_time2.strftime("%d/%m/%Y")
        time_part = series_time2.strftime("%H:%M") 
        height = allv[gp]           
        difference = allv[gp] - allv[gp -3]
        diff_str = f"{difference:.2f}"
        trend_val = trend(allv[gp -3], allv[gp])
        output_lines.append(f"{date_part}\t{time_part}\t{height:.2f}\t\t{diff_str}\t\t{trend_val}")        
        utils.add_data_from_arrays(data_n3,trend2,data_file,grid_properties,series_time2,data_coding_format)
        procgroup +=1        
    #elif gp == 3 or gp == 4:
    elif gp == 4 or gp == 5:
        dat_1 = masked_groups[gp-3]
        dat_2 = masked_groups[gp]
        trend2 = numpy.zeros_like(maskeddata2,dtype=int)
        for i in range(maskeddata2.shape[0]):
            for j in range(maskeddata2.shape[1]):
                #trend2[i][j] = trend(maskeddata2[i][j], maskeddata1[i][j])
                val_n = dat_1[i][j]
                val_n3 = dat_2[i][j]
                if not (numpy.isnan(val_n) or numpy.isnan(val_n3)):
                    trend2[i][j] = trend(val_n,val_n3)
                else:
                    trend2[i][j] = 0    
        #for value 3 or 4
        date_part = series_time2.strftime("%d/%m/%Y")
        time_part = series_time2.strftime("%H:%M") 
        height = allv[gp]           
        difference = allv[gp] - allv[gp -3]
        diff_str = f"{difference:.2f}"
        trend_val = trend(allv[gp -3], allv[gp])
        output_lines.append(f"{date_part}\t{time_part}\t{height:.2f}\t\t{diff_str}\t\t{trend_val}")                   
        utils.add_data_from_arrays(dat_2,trend2,data_file,grid_properties,series_time2,data_coding_format)
        procgroup +=1
#last_date = "{:%Y%m%dT%H%M%SZ}".format(series_time2)
last_date = series_time2.strftime("%Y%m%dT%H:%M:%SZ")   
update_meta = {
        'dateTimeOfLastRecord': last_date,
        'numberOfGroups': procgroup,
        'numberOfTimes': maxgroups,
        'timeRecordInterval': data['timeRecordInterval'],
        'num_instances': 1,
    }
utils.update_metadata(data_file, grid_properties, update_meta)
utils.write_data_file(data_file)

with open(file_path, "w") as f:
    f.write("\n".join(output_lines))

current_time = datetime.datetime.utcnow().strftime("%H%M%SZ")
h5_file = h5py.File(f"{fn}", "r+")

h5_file.attrs['editionNumber'] = editionNumber
h5_file.attrs['purpose'] = purpose
h5_file.attrs['issueTime'] = current_time

group = h5_file["/WaterLevel"]
group.attrs['methodWaterLevelProduct']= 'Astronomical_Prediction'
# group.attrs['dataOffsetCode'] = 5
group.attrs['minDatasetHeight'] = global_min
group.attrs['maxDatasetHeight'] = global_max

# In[27]:

bs = os.path.getsize(fn)
print('done. wrote ',str(bs),' bytes. gpx=', maxgroups , ' nX=' , xlimit , ' nY=', ylimit, ' to ' , fn,'and txt file as ', file_path)
