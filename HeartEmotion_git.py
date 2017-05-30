
# coding: utf-8

# In[1]:

sqlContext=SQLContext(sc)


# In[4]:


# @hidden_cell
credentials_1 = {
  'username':'yourcloudant user name OR any other data source username',
  'password':"""<<password>>""",
  'host':'2a61fe0c-4934-97b1-165baf9bf9ec-bluemix.cloudant.com',
  'port':'443',
  'url':'https://2a61fe0c-4934-97b1-165baf9bf9ec-bluemix:b3b3e06b7e7e5d74d3f647bd05b8@4934-97b1-165baf9bf9ec-bluemix.cloudant.com'
}


# In[19]:
#cloudant username, password and document name
cloudantdata=sqlContext.read.format("com.cloudant.spark").option("cloudant.host","2a61fe0c-4934-97b1-165baf9bf9ec-bluemix.cloudant.com").option("cloudant.username", "2a61fe0165baf9bf9ec-bluemix").option("cloudant.password", "5d74d3f647bd05b8").load("churcheve");


# In[20]:

cloudantdata.printSchema()


# In[126]:

df = cloudantdata.selectExpr("payload.d.BPM","payload.d.PULSE_RATE","payload.d.Time","payload.d.IBI","payload.d.PTT","payload.d.RAMP","payload.d.HRV","payload.d.SPECTRAL_MAX_A","payload.d.SPECTRAL_MAX_F","payload.d.SPECTRAL_MEAN_A")


# In[127]:

df.take(10)


# In[128]:

import pprint
import pandas as pd
pandaDF = df.toPandas()
#Fill NA/NaN values to 0
pandaDF.fillna(0, inplace=True)
pandaDF.columns



# In[37]:

len(pandaDF)


# In[107]:

pandaDF["IBI"]


# In[39]:

pandaDF[["Time","BPM"]]


# In[48]:

#import the datatime library
from datetime import datetime
# convert the time from string to panda's datetime
pandaDF.timestamp = pandaDF.Time
pandaDF.index = pandaDF.timestamp
pandaDF.sort_index(inplace=True)


# In[52]:

date="Sun May 07 19:14:05 GMT+05:30 2017"
pandaDF.ix[date]


# In[108]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.IBI.hist() 


# In[86]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np

plotDF = pandaDF[['BPM']]

import matplotlib.dates as dates

fig, ax = plt.subplots()
plotDF.head(540).plot(figsize=[20,10], ax=ax, grid=True)
#plotDF.plot(figsize=[20,10], ax=ax, grid=True)

ax.set_xlabel("Timestamp",fontsize=20)
ax.set_ylabel("BPM",fontsize=20)
ax.set_title("Overall BPM", fontsize=20)
plt.tight_layout()
plt.show()



# In[78]:

# find the maximum BPM
maximum = pandaDF.BPM.max()
maximum


# In[115]:

threshold_crossed_days = pandaDF[pandaDF.PTT > 140]
print "The days are --> " + str(threshold_crossed_days) 


# In[116]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.PTT.hist() 


# In[117]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.RAMP.hist() 


# In[118]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.SPECTRAL_MAX_A.hist() 


# In[119]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.SPECTRAL_MAX_F.hist() 


# In[120]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.SPECTRAL_MEAN_A.hist() 


# In[122]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt

pandaDF.IBI.hist() 


# In[124]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np

plotDF = pandaDF[['RAMP']]

import matplotlib.dates as dates

fig, ax = plt.subplots()
plotDF.head(540).plot(figsize=[20,10], ax=ax, grid=True)
#plotDF.plot(figsize=[20,10], ax=ax, grid=True)

ax.set_xlabel("Timestamp",fontsize=20)
ax.set_ylabel("RAMP",fontsize=20)
ax.set_title("Overall RAMP", fontsize=20)
plt.tight_layout()
plt.show()


# In[125]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np

plotDF = pandaDF[['IBI']]

import matplotlib.dates as dates

fig, ax = plt.subplots()
plotDF.head(540).plot(figsize=[20,10], ax=ax, grid=True)
#plotDF.plot(figsize=[20,10], ax=ax, grid=True)

ax.set_xlabel("Timestamp",fontsize=20)
ax.set_ylabel("IBI",fontsize=20)
ax.set_title("Overall IBI", fontsize=20)
plt.tight_layout()
plt.show()


# In[129]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import numpy as np

plotDF = pandaDF[['HRV']]

import matplotlib.dates as dates

fig, ax = plt.subplots()
plotDF.head(540).plot(figsize=[20,10], ax=ax, grid=True)
#plotDF.plot(figsize=[20,10], ax=ax, grid=True)

ax.set_xlabel("Timestamp",fontsize=20)
ax.set_ylabel("HRV",fontsize=20)
ax.set_title("Overall HRV", fontsize=20)
plt.tight_layout()
plt.show()


# In[ ]:



