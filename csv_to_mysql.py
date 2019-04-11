# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 21:27:46 2019

@author: smald
"""

import pandas as pd
import numpy as np

df = pd.read_csv('contact_list .csv')

#this is done to avoid nan issues
df = df.fillna('')

#this is done to convert the mobile and other numbers into string.
df['Mobile Number'] = df['Mobile Number'].apply(lambda x:str(x).rstrip('0').rstrip('.'))
df['Other Number'] = df['Other Number'].apply(lambda x:str(x).rstrip('0').rstrip('.'))

#This is done to sanitize the data from harmful sql injections.list can be expanded. 
def Sanitize_Notes(x):
    sql_injection_list = ['DROP TABLE','INSERT INTO','TRUNCATE TABLE','CREATE USER','DROP USER','GRANT ALL PRIVILEGES']
    for item in sql_injection_list:
        if item in x:
            return ''
        else:
            return x;
df['Notes'] = df['Notes'].apply(lambda x:Sanitize_Notes(str(x)))

#this is done to resolve inconsistent DOB records. 
df['Date Of Birth'] = pd.to_datetime(df['Date Of Birth'],format='%m%d%Y', infer_datetime_format=True)

#this is done to remove trailing full stops and capitalise the title. 
df['Title'] = df['Title'].str.strip('.').str.capitalize()
#this is done to also done to upper case the first letter of the first name. No apostrophe's found here so capitalise can work fine. 
df['First Name'] = df['First Name'].str.capitalize()
#this is done to upper case the first letter even after an apostrophe e.g. O'Malley
df['Last Name'] = df['Last Name'].str.title()

#this is the reference table which can be expanded based on more known acronyms. 
acronyms = {'A.B.C':['ABC','abc','a.b.c'],
            'A.N.Z.A.C':['ANZAC','anzac','a.n.z.a.c'],
            'P.P.P':['PPP','ppp','p.p.p']}
#this method uppercases the acronyms based on the reference table above.     
def Uppercase_Acronyms(businessName):   
    for key in acronyms:
        for value in acronyms[key]:
            if value in businessName:
                businessName = businessName.replace(value,key)
    return businessName      
df['Business']=df['Business'].apply(lambda businessName: Uppercase_Acronyms(businessName))

#this is done to ensure all mobile numbers are prefixed with 64 and landline numbers prefixed with 09
def Prefix_Phone_Number(prefix_num,x):
    if x[:2] != prefix_num and str(x) != '' and x[:4]!='('+ prefix_num + ')':
        return prefix_num + str(x).rstrip('0').rstrip('.')
    else :
        return x
df['Mobile Number'] = df['Mobile Number'].apply(lambda x : Prefix_Phone_Number('64',x))
df['Work Number'] = df['Work Number'].apply(lambda x : Prefix_Phone_Number('09',x))
df['Fax Number'] = df['Fax Number'].apply(lambda x : Prefix_Phone_Number('09',x))
df['Home Number'] = df['Home Number'].apply(lambda x : Prefix_Phone_Number('09',x))

#creating the dataframe used to insert into the contact table
contact_dataframe = pd.concat([df["Title"],df["First Name"],df["Last Name"],df["Business"],df["Date Of Birth"],df["Notes"]],axis=1, keys=['title','first_name','last_name','company_name','date_of_birth','notes'])

#this is done to create the contact_id in address and phone tables. This assumes contact table is empty
df["contact_id"] = df.index
df["contact_id"] =df["contact_id"].apply(lambda x: x + 1)

#creating the dataframe used to insert into the address table
address_dataframe = pd.concat([df["contact_id"],df["Address Line 1"],df["Address Line 2"],df["Suburb"],df["City"],df["Post Code"]],axis=1, keys=['contact_id','street1','street2','suburb','city','post_code'])  

#creating the dataframe used to insert into the phone table
phone_dataframe = pd.DataFrame(columns=['contact_id','name','content','type'])

#populating the phone dataframe with home phone numbers. 
for rownumber in range(df.shape[0]):
    if df["Home Number"].iloc[rownumber] != "":
        phone_dataframe = phone_dataframe.append({'contact_id':df["contact_id"].iloc[rownumber],'name':df["First Name"].iloc[rownumber] + " " + df["Last Name"].iloc[rownumber],'content':df["Home Number"].iloc[rownumber],'type':'Home'}, ignore_index=True)

#populating the phone dataframe with fax phone numbers but hardcoded to other type.  
for rownumber in range(df.shape[0]):
    if df["Fax Number"].iloc[rownumber] != "":
        phone_dataframe = phone_dataframe.append({'contact_id':df["contact_id"].iloc[rownumber],'name':df["First Name"].iloc[rownumber] + " " + df["Last Name"].iloc[rownumber],'content':df["Home Number"].iloc[rownumber],'type':'Other'}, ignore_index=True)

#populating the phone dataframe with work phone numbers. 
for rownumber in range(df.shape[0]):
    if df["Work Number"].iloc[rownumber] != "":
        phone_dataframe = phone_dataframe.append({'contact_id':df["contact_id"].iloc[rownumber],'name':df["First Name"].iloc[rownumber] + " " + df["Last Name"].iloc[rownumber],'content':df["Home Number"].iloc[rownumber],'type':'Work'}, ignore_index=True)

#populating the phone dataframe with home mobile numbers. 
for rownumber in range(df.shape[0]):
    if df["Mobile Number"].iloc[rownumber] != "":
        phone_dataframe = phone_dataframe.append({'contact_id':df["contact_id"].iloc[rownumber],'name':df["First Name"].iloc[rownumber] + " " + df["Last Name"].iloc[rownumber],'content':df["Home Number"].iloc[rownumber],'type':'Mobile'}, ignore_index=True)

#populating the phone dataframe with other phone numbers. 
for rownumber in range(df.shape[0]):
    if df["Other Number"].iloc[rownumber] != "":
        phone_dataframe = phone_dataframe.append({'contact_id':df["contact_id"].iloc[rownumber],'name':df["First Name"].iloc[rownumber] + " " + df["Last Name"].iloc[rownumber],'content':df["Home Number"].iloc[rownumber],'type':'Other'}, ignore_index=True)

#this is done to replace all empty strings with null values. prevents from writing empty strings in target tables. 
contact_dataframe = contact_dataframe.replace('', np.nan, regex=True)
address_dataframe = address_dataframe.replace('', np.nan, regex=True)
phone_dataframe = phone_dataframe.replace('', np.nan, regex=True)

#creating the connection to mysql database
import sqlalchemy as db
engine = db.create_engine('mysql://root:root@localhost/test')
connection = engine.connect()
metadata = db.MetaData()

#writing the dataframes to the target mysql tables. 
contact_dataframe.to_sql(con=engine, name='contact', if_exists='append',chunksize=100, index=False)
address_dataframe.to_sql(con=engine, name='address', if_exists='append',chunksize=100, index=False)
phone_dataframe.to_sql(con=engine, name='phone', if_exists='append',chunksize=100, index=False)

