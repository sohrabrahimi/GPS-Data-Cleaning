
# coding: utf-8

# # Cleaning GPS data collected from Catoctin park hikers 
# 
# In this porject I intend to clean a dataset collected between July 31st and August 20th from 121 GPS divices that were distributed among the hikers in Catoktin Mountain Park, MD. During this period, every hiker was given a GPS divice in the beginning of her hike at the visiting center and was asked to return the divice at the end of her hike, at the same location (i.e. visitors' center). The data collected from these GPS is not consistent as many GPS holders have occasionally turned off the divice or have drived long distances while holding these divices. Since we are looking into their hiking behaviors we are only interested in hiking trajectories and therefor will need to exclude undesired trajectories. Since the hikers have paused at the visitors' center for relatively long time, we also would like to exclude the data collected during that time since these data points significantly affect the average speed and time spent. Therefore we are looking at the following steps:
# 
# <br>
# <li> Remove data points located at the visiting center </li>
# <li> Remove data points collected while driving by filtering high speed path segments </li>
# <li> Identify spots where the hiker turns of his/her diviceRemove path segments where the time interval or distance between two consecutive points is larger than one minute and 180 meters, respectively. </li>
# <li> Remove tracks with less than 50 points (shorter than roughly 10 minutes hikes) </li>
# <li> Remove extraneous points. These points include those points where more than one point has been collected from a certain location at a certain time </li>
# <li> Remove points that are not in the Catoctin Mountain Park Area. </li>
# 
# 

# # Accessing the data stored in shapefiles

# In[1]:

# open folders
import os
path = "C:/Users/sur216/Desktop/GPS Tracks_ALL"
folders = os.listdir (path)[1:]
dbf_path = []
for n,f in enumerate(folders):
        if not "GPS " in f: continue
        n_path = path + "/"+f
        print  f
        for j in os.listdir(n_path): 
            if "dbf" in j:dbf_path.append(str(n_path + "/" +j))


# We will next read through dbf files and record everything in python lists. As shown below, this data includes 22 columns. 

# In[2]:

# open shapefiles and save the data into python lists
from dbfread import DBF
import pandas as pd
records = []
for n, i in enumerate(dbf_path): 
        idr = i.split("/")[-1][:-4]
        if not "pt" in idr:continue
        for record in DBF(i):
            record.update({"id":idr})
            records.append(record)
columns = []            
for n,r in enumerate(records):
    if n==1: 
        for j in r: columns.append(j)
print columns


# In[3]:

# convert the lists to Pandas DataFrame. We have 22 columns and 128930 rows
recs = []
for n,r in enumerate(records): 
        lst = []
        for c in columns : 
            lst.append(r[c])
        recs.append(lst)
df = pd.DataFrame(recs)
df.columns = columns
print "table size :",df.shape


# In[4]:

print len(df["id"].unique())


# # Creating new ids
# 
# As we can see this data includes 42 unique GPS ids, however, recall that these GPS divices have been assigned to different people at different dates. Therefore, we will need to create a new ID that considers both the date and the GPS divice. We will call this "new_id". We can see that we have 146 unique IDs now. 

# In[5]:

# create new id for each GPS track based on data and file name
new_id = ["/".join(["-".join(list(i.split(" ")[0].split("/")[1:]))]+[j]) for i,j in zip(df['time'].tolist(),df['id'].tolist())]


# In[6]:

df['new_id'] = new_id


# In[7]:

print len(df["new_id"].unique())


# # Excluding the points nearby the visitor center
# 
# As discussed earlier, we wanted to exclude the points collected from the visitor center. To this end, I difined a 46X63 meters rectangle around the visitor center and excluded every point that was inside this square. 

# In[8]:

# identifying those points that fall near the visitors' center
df2 = df[(df['Latitude']<39.63435507) & (df['Latitude']>39.63378507) & (df['Longitude']<-77.44983899) & (df['Longitude']>-77.45038231)]


# In[9]:

#df3 is the sets of points without those located at the visiting center
df3 = pd.merge(df, df2, how='outer', indicator=True)
df3 = df3[df3["_merge"]=="left_only"]
df3 = df3.iloc[:,0:23]


# # Calculating speed and distance
# 
# In the next step we will calculate the speed between every two consecutive points. This step is important as this will provide the basis for filtering the points. The distance between every two points has been calculated by latitude, longitude and altitude. Also, we will calculate and store the change in elevation, planar distance and time difference in separate columns. 

# In[10]:

#convert to timestamp format
df3["time"] = pd.to_datetime(df3["time"])


# In[11]:

# calculate distances, time steps and speeddf4
from geopy.distance import vincenty
import math
import datetime as dt
import time
tracks = df3['new_id'].unique()
df4 = pd.DataFrame()
for n,t in enumerate(tracks): 
    q = df3[df3["new_id"]==t]
    q = q.sort(['time'], ascending=True) 
    time = q["time"].tolist()
    lons = q["Longitude"].tolist()
    lats = q["Latitude"].tolist()
    alts = q["altitude"].tolist()
    lat_lng =zip(lats,lons)
    dist_horiz = ["NA"]
    dist_vert = ["NA"]
    tot_dist = ["NA"]
    time_dif = ["NA"]
    speed = ["NA"]
    for n,p in enumerate(lat_lng): 
        if not n==0:  
            y = vincenty(lat_lng[n], lat_lng[n-1]).meters
            z = alts[n]-alts[n-1]
            t = time[n]-time[n-1]
            t = t.total_seconds()
            d = math.sqrt(y**2+z**2)
            dist_horiz.append(y)
            dist_vert.append(z)
            tot_dist.append(d)
            time_dif.append(t)
            if not t == 0:
                speed.append(d/t)
            else: speed.append("NA")
    q['dist_horiz'] = dist_horiz  
    q['dist_vert'] = dist_vert
    q['time_dif'] = time_dif
    q['tot_dist'] = tot_dist
    q['speed'] = speed
    df4 = df4.append(q)


# In[222]:

df4.to_csv("GPS_compiled.csv", index = False)


# As we can see below, the time interval between concecutive points is not steady. Although the majority of points have been collected within 10 seceonds intervals (104699 points). We can also see that some data points has been collected with 0 seconds intervals. If we print these points out we can see that the distance between thes epoints are also 0. Therefore we will simply remove these from our dataframe. 

# In[12]:

ls = [int(i) for i in df4[df4["tot_dist"].isin(["NA"])==False]['time_dif'].unique()]
ls2 = [int(i) for i in df4[df4["tot_dist"].isin(["NA"])==False]['time_dif']]
ls.sort()
from collections import defaultdict
fq= defaultdict( int )
for t in ls2:
    fq[t] += 1
print fq


# In[13]:

ls = [int(i) for i in df4[df4["tot_dist"].isin(["NA"])==False]['tot_dist'].unique()]
ls2 = [int(i) for i in df4[df4["tot_dist"].isin(["NA"])==False]['tot_dist']]
ls.sort()
from collections import defaultdict
fq= defaultdict( int )
for t in ls2:
    fq[t] += 1
print fq


# In[14]:

#remove points with 0 time difference
df4 = df4[df4['time_dif'] !=0]


# In[15]:

df4.shape


# # filtering according to speed, time and total distance
# 
# We will now remove those tracks with less than 50 data points (less than 10 minutes hiking). After this step we will end up with 113 GPS tracks. We will also delet those points that show speeds higher than 3 m/s, the time difference between every two points is higher than 60 seconds and the distnce taken between tow points is higher than 180 meters. 

# In[16]:

# walked for at least 10 mins
lll = []
df5 = pd.DataFrame()
for n, t in enumerate(tracks):
    q = df4[df4["new_id"]==t]
    if not q.shape[0]<50: lll.append(q)
for d in lll: 
    if not d.empty: df5 = pd.concat([df5,d], ignore_index=True)


# In[17]:

df5 = df5[df5['speed']<3]
df5 = df5[df5['time_dif']<60]
df5 = df5[df5['tot_dist']<180]
df5.shape


# In[18]:

len(df5['new_id'].unique())


# In[24]:

df5.to_csv("GPS_quasicleaned.csv", index = False)


# # Using Arcmap for filtering
# 
# In the next step, we will use the arcmap software to reove the data points that are not close to the Catoctin park area. Occasionally, the GPS holders have drived to a nearby town or other areas without turning their GPS divices off. We are not interested in these data points and our cleaning process so far has not filtered these. To this end, we will use the Arcmap 10.4 software and manually remove these points. After this step, we will convert the DBF data to Pandas Dataframe:

# In[19]:

from dbfread import DBF
import pandas as pd
records = []
dbf_path = "C:/Users/sur216/Desktop/GPS Tracks_ALL/all_compiled_projected.dbf"
for record in DBF(dbf_path):
            records.append(record)
print records[1]


# In[107]:

# save to csv after arcmap edits
columns = []            
for n,r in enumerate(records):
    if n==1: 
        for j in r: columns.append(j)
print columns
recs = []
for n,r in enumerate(records): 
        lst = []
        for c in columns : 
            lst.append(r[c])
        recs.append(lst)
df = pd.DataFrame(recs)
df.columns = columns
print "table size :",df.shape


# # Aggregating the data
# 
# In the next step we will aggregate the values that we are interested in. We would like to have the average speed, total distance and total time spent. We will also add the amount of vertical distance climbed up and down separately.

# In[108]:

sm = df.groupby(['new_id']).sum()
mn = df.groupby(['new_id']).mean()
mn_spd = mn.loc[:,['speed']]
mn_spd['new_id']= mn_spd.index
mn_vert = df.loc[:,['new_id','dist_vert']]
clm_up = mn_vert[mn_vert['dist_vert']>0]
clm_up = clm_up.groupby(['new_id']).sum()
clm_dwn = mn_vert[mn_vert['dist_vert']<0]
clm_dwn = clm_dwn.groupby(['new_id']).sum()
clm_dwn['climb_down'] = clm_dwn['dist_vert']
clm_up['climb_up'] = clm_up['dist_vert']
del clm_up['dist_vert'] 
del clm_dwn['dist_vert']
clm_up['new_id'] = clm_up.index
clm_dwn['new_id'] = clm_dwn.index
mn_vert['dist_vert'] = mn_vert['dist_vert'].abs()
mn_vert = mn_vert.groupby(['new_id']).mean()
mn_vert['mean_elevation'] = mn_vert['dist_vert']
mn_vert =  mn_vert.loc[:,['mean_elevation']]


# In[109]:

up_dwn = clm_up.merge(clm_dwn)
sm = sm.loc[:,['dist_horiz','time_dif','tot_dist']]
sm['new_id']= sm.index
df1 = up_dwn.merge(sm, on = 'new_id')
df2 = mn_spd.merge(df1, on= 'new_id' )
df2.columns = ['average_speed','new_id','climb_up','climb_down','Total_planar_distance','Total_time','Total_distance']


# In[111]:

df2 = df2.reindex_axis(['new_id','average_speed','climb_up','climb_down','Total_planar_distance','Total_time','Total_distance'], axis=1)
print df2.head(10)


# In[106]:

df2.to_csv("GPS_aggregated.csv", index = False)

