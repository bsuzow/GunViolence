# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 13:48:58 2018

@author: bsuzow
"""


def get_incident_count(df,year,cnt_field="incident_id"):
    """ 
    Runs count groupby on state 
    arguments:
        year:  The year for which incident counts by state to be calc'd
        df: the gun violence dataframe
        cnt_field: the field on which groupby runs on
    """
    yr_df = gv.loc[gv.year==year]

    return yr_df.groupby('state')[cnt_field].count()
    
    
import pandas as pd

# read the data
gv_df = pd.read_csv("gun-violence-data_01-2013_03-2018.csv")

# select columns of interests for summary
gv = gv_df[["incident_id","date","state",
           "n_killed","n_injured","latitude",
           "longitude","n_guns_involved",
           "state_house_district",
           "state_senate_district"]]

gv2=gv_df[["incident_id",
           "participant_age",
           "participant_gender",
           "participant_type",]]

# check which columns with NaNs
gv.columns[gv.isna().any()].tolist()

# percentages of NaNs
na_counts = gv.isna().sum()
na_counts / len(gv)

# convert the date column to a datetime object
# from datetime import datetime as dt
gv.loc[:,("date")]=pd.to_datetime(gv["date"],format="%Y-%m-%d")

# group the set by month
gv["yymm"] = gv["date"].map(lambda x: str(x.year) + "-"+str(x.month))
gv["year"] = gv["date"].map(lambda x: str(x.year))


# save the data as a csv
gv.to_csv("gv_summary1.csv", index=False)
gv2.to_csv("gv_participant.csv",index=False)

# add the state populate (2013-2017)
# the csv is extracted from https://www.census.gov/data/datasets/time-series/demo/popest/2010s-state-total.html
# an excerpt of the popu data for 2013-17
gv_popu_df = pd.read_csv("state_popu.csv")

# int(a.replace(',' , ''))
# remove the commas from the values and rename columns

gv_popu_df["popu2013"] = [int(x.replace(',','')) for x in gv_popu_df["2013"]]
gv_popu_df["popu2014"] = [int(x.replace(',','')) for x in gv_popu_df["2014"]]
gv_popu_df["popu2015"] = [int(x.replace(',','')) for x in gv_popu_df["2015"]]
gv_popu_df["popu2016"] = [int(x.replace(',','')) for x in gv_popu_df["2016"]]
gv_popu_df["popu2017"] = [int(x.replace(',','')) for x in gv_popu_df["2017"]]

gv_popu_df.drop(["2013","2014","2015","2016","2017"], axis='columns',inplace=True)      

# reindex gv_popu_df

gv_popu_df.set_index('state', inplace=True)

# zeroing in on 2013  Data is scanty ---------------------

cnt2013= get_incident_count(gv,"2013")
gv_popu_df["ic_2013"] = cnt2013

# zeroing in on 2014 -------------------------------------

cnt2014= get_incident_count(gv,"2014")
gv_popu_df["ic_2014"] = cnt2014

# zeroing in on 2015 -------------------------------------

cnt2015= get_incident_count(gv,"2015")
gv_popu_df["ic_2015"] = cnt2015


# zeroing in on 2016 -------------------------------------

cnt2016= get_incident_count(gv,"2016")
gv_popu_df["ic_2016"] = cnt2016

# zeroing in on 2017 -------------------------------------

cnt2017= get_incident_count(gv,"2017")
gv_popu_df["ic_2017"] = cnt2017

# write the augmented state popu dataset

gv_popu_df.to_csv("state_popu2.csv", index=True)
