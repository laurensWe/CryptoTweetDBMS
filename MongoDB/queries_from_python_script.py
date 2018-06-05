import pymongo
import pandas as pd
client = pymongo.MongoClient("localhost", 27017)
db = client.test_alex
collection=db['coins']

#load data
data = pd.read_csv('/Users/alexandrosstavroulakis/Downloads/CMC Crypto Data 1_8_18 - Sheet1.csv')
data=data.fillna('0.0%')
['Calendar Date']=pd.to_datetime(data['Calendar Date'])

def p2f(x):
    return float(x.strip('%'))/100

data['Daily % Change USD']=data.apply(lambda row: p2f(row['Daily % Change USD']),axis=1)
data['Daily % Change BTC']=data.apply(lambda  row: p2f(row['Daily % Change BTC']),axis=1)
data['Daily % Change ETH']=data.apply(lambda  row: p2f(row['Daily % Change ETH']),axis=1)
data['% Change to BTC']=data.apply(lambda  row: p2f(row['% Change to BTC']),axis=1)
data['% Change to ETH']=data.apply(lambda  row: p2f(row['% Change to ETH']),axis=1)
data['Percent Max Price']=data.apply(lambda  row: p2f(row['Percent Max Price']),axis=1)

#store data into MongoDB
columns=data.columns
for index,row in data.iterrows():
	query={}
	for i in columns:
		query[i]=row[i]
	collection.insert(query)


#############Queries###################

################.   1.     ############
import time
b=int(round(time.time() * 1000))
df = pd.DataFrame(list(collection.find({'Daily % Change USD':{'$gt':4}})))
num=len(df)
lis=[{}]
for i in range(0,num):
	a=df['Day of Month'][i]-1
	df2=pd.DataFrame(list(collection.find({'Coin Name':df['Coin Name'][i],'Month':df['Month'][i],'Day of Month':int(a)})))
	a={'difference' :df['Circulating Coins'][i] -df2['Circulating Coins'][0], 'coin':df['Coin Name'][i]}
	lis.append(a)
c=int(round(time.time() * 1000))
print(float(c)-float(b))



##############     2.   ################

b=int(round(time.time() * 1000))
df = pd.DataFrame(list(collection.aggregate([{'$group':{'_id':'$Coin Name','average':{'$avg':'$Close Price'}}}])))
df2=pd.DataFrame(list(collection.aggregate([{'$match':{'Calendar Date':{'$regex' : ".*2017.*"}}},{'$group':{'_id':'$Coin Name','average':{'$avg':'$Close Price'}}}])))
maxAvg=df.max()
dffinal=df2.loc[df2.average>maxAvg[1]]
c=int(round(time.time() * 1000))
print(float(c)-float(b))


##############     3.   ################


b=int(round(time.time() * 1000))
collection.aggregate([
{ '$match':{'Day of Month':{'$gt':0,'$lt':15}}}, 
{'$group':{'_id':'$Coin Name','average':{'$avg':'$Close Price'}}},
{ '$sort' : { 'average' : -1}}
])
c=int(round(time.time() * 1000))

