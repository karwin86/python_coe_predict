from cmath import nan
import csv
import pandas as pd
import numpy as np
import sklearn.preprocessing as onehotencode
import matplotlib.pyplot as plt
from decimal import Decimal

##Load data

##read COE bidding exercise
file = "D:\Personal\DS\Carro\Results of COE Bidding Exercise - Results.csv"
coe_new_df = pd.read_csv(file)
##coe_new_df = coe_df
coe_new_df = coe_new_df.reindex(columns = coe_new_df.columns.tolist() 
                                  + ['Data Series','Bid no'])
##print(coe_new_df)
##filter out only cat A and B and change month and year values to fit other datasets
for ind in coe_new_df.index:
    year_month = coe_new_df['Bidding Exercise'][ind]
    year_month = year_month.split(' ')
    year_month = year_month[1] + " " + year_month[0][0:3]
    ##print (year_month) 
    ##coe_new_df["Data Series"][ind] = year_month
    coe_new_df.at[ind,'Data Series'] = year_month
    bid_no = coe_new_df['Bidding Exercise'][ind]
    bid_no = bid_no.split(' ')
    if bid_no[2] == "Second":
        bid_no = 2
    elif bid_no[2] == "First":
        bid_no = 1
    else:
        bid_no = 0
    ##coe_new_df["Bid no"][ind] = bid_no
    coe_new_df.at[ind, 'Bid no'] = bid_no
    cat = coe_new_df['Category'][ind]
    cat = cat.split(' ')
    cat = cat[0] + " " + cat[1]
    ##coe_new_df['Category'][ind] = cat
    coe_new_df.at[ind, 'Category'] = cat
options = ['Cat A', 'Cat B']
coe_new_df = coe_new_df[coe_new_df['Category'].isin(options)]
##print(coe_new_df['Data Series'])

##read CPI index
file = "D:\Personal\DS\Carro\M212881.csv"
cpi_df = pd.read_csv(file, skiprows=10)
cpi_df['Data Series'] = cpi_df['Data Series'].str.strip()
##print(cpi_df.head())
cpi_df = cpi_df[cpi_df['Data Series'] == "Cars"]
cpi_new_df = cpi_df.set_index('Data Series').T
cpi_new_df = cpi_new_df.reset_index()
cpi_new_df = cpi_new_df.rename(columns = {'index': 'Data Series', 'Cars': 'CPI Index'})
##print(cpi_df)

##read new registration vehicles monthly data
file = "D:\Personal\DS\Carro\M650281.csv"
n_veh_df = pd.read_csv(file, skiprows=10)
n_veh_df['Data Series'] = n_veh_df['Data Series'].str.strip()
options = ['Category A: Cars', 'Category B: Cars']
n_veh_df = n_veh_df[n_veh_df['Data Series'].isin(options)]
for ind in n_veh_df.index:
    car_type_str = n_veh_df['Data Series'][ind].split(' ')
    finstr = car_type_str[0][0:3]  + " " + car_type_str[1][0:1]
    ##print(finstr)
    n_veh_df['Data Series'][ind] = finstr
n_veh_df = n_veh_df.set_index('Data Series').T
n_veh_df = n_veh_df.reset_index()
n_veh_df = n_veh_df.rename(columns = {'index': 'Data Series', 'Cat A': 'Cat A Veh Amount', 'Cat B': 'Cat B Veh Amount'})
##print (n_veh_df)

##read motor vehicles deregistered monthly data
file = "D:\Personal\DS\Carro\M650291.csv"
dereg_veh_df = pd.read_csv(file, skiprows=10)
dereg_veh_df['Data Series'] = dereg_veh_df['Data Series'].str.strip()
options = ['Category A: Cars', 'Category B: Cars']
dereg_veh_df = dereg_veh_df[dereg_veh_df['Data Series'].isin(options)]
for ind in dereg_veh_df.index:
    car_type_str = dereg_veh_df['Data Series'][ind].split(' ')
    finstr = car_type_str[0][0:3]  + " " + car_type_str[1][0:1]
    ##print(finstr)
    dereg_veh_df['Data Series'][ind] = finstr
dereg_veh_df = dereg_veh_df.set_index('Data Series').T
dereg_veh_df = dereg_veh_df.reset_index()
dereg_veh_df = dereg_veh_df.rename(columns = {'index': 'Data Series', 'Cat A': 'Cat A Veh Amount', 'Cat B': 'Cat B Veh Amount'})
##print (dereg_veh_df)

##read motor vehicles under vehicle quota system monthly data
file = "D:\Personal\DS\Carro\M650341.csv"
pop_veh_df = pd.read_csv(file, skiprows=10)
pop_veh_df['Data Series'] = pop_veh_df['Data Series'].str.strip()
options = ['Category A: Cars', 'Category B: Cars']
pop_veh_df = pop_veh_df[pop_veh_df['Data Series'].isin(options)]
for ind in pop_veh_df.index:
    car_type_str = pop_veh_df['Data Series'][ind].split(' ')
    finstr = car_type_str[0][0:3]  + " " + car_type_str[1][0:1]
    ##print(finstr)
    pop_veh_df['Data Series'][ind] = finstr
pop_veh_df = pop_veh_df.set_index('Data Series').T
pop_veh_df = pop_veh_df.reset_index()
pop_veh_df = pop_veh_df.rename(columns = {'index': 'Data Series', 'Cat A': 'Cat A Veh Amount', 'Cat B': 'Cat B Veh Amount'})
##print (pop_veh_df)

## combine datasets
final_df = pd.merge(coe_new_df, cpi_new_df, left_on="Data Series", right_on="Data Series")
final_df = final_df.reindex(columns = final_df.columns.tolist() 
                                  + ['New Veh Amt','Dereg Veh Amt', 'Vehicle Quota'])
##for col in final_df.columns:
##    print (col)
for ind in final_df.index:
    for tind in n_veh_df.index:
        finym = final_df['Data Series'][ind].strip()
        ##print(finym)
        tinym = n_veh_df['Data Series'][tind].strip()
        ##print(tinym)
        cater = final_df['Category'][ind].strip()
        if (finym == tinym and cater == "Cat A"):
            final_df.at[ind, 'New Veh Amt'] = n_veh_df['Cat A Veh Amount'][tind]
            break
        elif (finym == tinym and cater == "Cat B"):
            final_df.at[ind, 'New Veh Amt'] = n_veh_df['Cat B Veh Amount'][tind]
            break
for ind in final_df.index:
    for tind in dereg_veh_df.index:
        finym = final_df['Data Series'][ind].strip()
        ##print(finym)
        tinym = dereg_veh_df['Data Series'][tind].strip()
        ##print(tinym)
        cater = final_df['Category'][ind].strip()
        if (finym == tinym and cater == "Cat A"):
            final_df.at[ind, 'Dereg Veh Amt'] = dereg_veh_df['Cat A Veh Amount'][tind]
            break
        elif (finym == tinym and cater == "Cat B"):
            final_df.at[ind, 'Dereg Veh Amt'] = dereg_veh_df['Cat B Veh Amount'][tind]
            break
for ind in final_df.index:
    for tind in pop_veh_df.index:
        finym = final_df['Data Series'][ind].strip()
        ##print(finym)
        tinym = pop_veh_df['Data Series'][tind].strip()
        ##print(tinym)
        cater = final_df['Category'][ind].strip()
        if (finym == tinym and cater == "Cat A"):
            final_df.at[ind, 'Vehicle Quota'] = pop_veh_df['Cat A Veh Amount'][tind]
            break
        elif (finym == tinym and cater == "Cat B"):
            final_df.at[ind, 'Vehicle Quota'] = pop_veh_df['Cat B Veh Amount'][tind]
            break
##print(coe_new_df['Data Series'].dtypes)
##print(list(cpi_df.columns.values))
##print(list(cpi_new_df.columns.values))
##print(final_df)

## Data Cleaning and wrangling
## check for missing values
print (sum(final_df.isnull().any(axis=1)))

## break date into day month
final_df = final_df.reindex(columns = final_df.columns.tolist() 
                                  + ['Day','Month'])
for ind in final_df.index:
    dmy = final_df['Announcement Date'][ind].split('/')
    day = dmy[0]
    month = dmy[1]
    final_df.at[ind, 'Day'] = day
    final_df.at[ind, 'Month'] = month
##print(final_df)

## convert Category, Data Series to numbers through onehotencoding
labelenc = onehotencode.LabelEncoder()
new_data = labelenc.fit_transform(final_df['Data Series'])
final_df = final_df.reindex(columns = final_df.columns.tolist() 
                                  + ['Data Series Encoded','Category Encoded'])
final_df['Data Series Encoded'] = new_data
new_data = labelenc.fit_transform(final_df['Category'])
final_df['Category Encoded'] = new_data

## convert Quota Premium, total bids received and number of successful bids to a decimal
for ind in final_df.index:
    prem = final_df['Quota Premium'][ind].strip()
    prem = prem[1:len(prem)]
    prem = prem.replace(',','')
    prem = Decimal(prem)
    final_df.at[ind, 'Quota Premium'] = prem
    prem = final_df['Total Bids Received'][ind].strip()
    prem = prem.replace(',','')
    prem = Decimal(prem)
    final_df.at[ind, 'Total Bids Received'] = prem
    prem = final_df['Number of Successful Bids'][ind].strip()
    prem = prem.replace(',','')
    prem = Decimal(prem)
    final_df.at[ind, 'Number of Successful Bids'] = prem

    ##print(prem)

##print(final_df)

## picking features to be used for fitting the model

## check relationship of various columns vs Quota Premium
plt.plot(final_df['Category Encoded'], final_df['Quota Premium'])
plt.show