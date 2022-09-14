import pandas as pd
import numpy as np
import re
import sqlite3



def load_data(database):
    return pd.read_csv(database)

def content_database_1(df):
    df["Gender"] = df["Gender"].replace({"0":"Male","1":"Female","M":"Male","F":"Female"})
    df['FirstName'] = df['FirstName'].str.title().replace({r"\\":"",r'""':''}, regex = True)
    df['LastName'] = df['LastName'].str.title().replace({r"\\":"",r'""':''}, regex = True)
    del df['UserName']
    df["Email"] = df['Email'].str.lower()
    df["City"] = df["City"].str.title().replace({r"\\":"",r'""':'',r"_":' ',r"-":" "}, regex = True)
    df["Country"] = "USA"
    return df

def content_database_2(df2):
    lst = ["Age","City", "Gender","Fullname","Email"]
    df2 = pd.read_csv("only_wood_customer_us_2.csv",delimiter=";",names=lst)
    lst = ["Age","City", "Gender","Fullname","Email"]
    df2[["FirstName","LastName"]]=df2['Fullname'].str.split(" " , expand=True)
    del df2['Fullname']
    df2["Age"] = df2["Age"].replace({"\D":""},regex = True)
    df2["Gender"] = df2["Gender"].replace({"0":"Male","1":"Female","M":"Male","F":"Female"})
    df2["City"] = df2["City"].str.title()
    df2["Email"] = df2['Email'].str.lower()
    df2['FirstName'] = df2['FirstName'].str.title().replace({"\W":""}, regex = True)
    df2['LastName'] = df2['LastName'].str.title().replace({"\W":""}, regex = True)
    df2["Country"]="USA"
    return df2

def content_database_3(df3):
    df3 = pd.read_csv("only_wood_customer_us_3.csv",sep = "\t|,",engine='python')
    df3 = df3.replace({"string_":'',"integer_":'',"boolean_":'',"character_":''}, regex=True)
    df3["Gender"] = df3["Gender"].replace({"0":"Male","1":"Female","M":"Male","F":"Female"})
    df3[["FirstName","LastName"]]=df3['Name'].str.split(" " , expand=True)
    df3['FirstName'] = df3['FirstName'].str.title().replace({"\W":""}, regex = True)
    df3['LastName'] = df3['LastName'].str.title().replace({"\W":""}, regex = True)
    df3["Country"] = "USA"
    df3["Age"] = df3["Age"].replace({"\D":""},regex = True)
    df3["City"] = df3["City"].str.title()
    df3["Email"] = df3['Email'].str.lower()
    del df3["Name"]
    return df3

def all(df,df2,df3):
    global all
    all = df.append(df2, ignore_index=True).append(df3, ignore_index=True) 
    return all


def string_to_csv(all):
  return all.to_csv (r'merged_csv', index=None)


def csv_to_sql(csv_content, database, table_name):
    df = pd.read_csv(csv_content)
    connection = sqlite3.connect(database)
    df.to_sql(table_name, con=connection, if_exists='replace', index=False)



def _main():
    global all
    df=load_data("only_wood_customer_us_1.csv")
    df2=load_data("only_wood_customer_us_2.csv")
    df3=load_data("only_wood_customer_us_3.csv")
    df=content_database_1(df)
    df2 = content_database_2(df2)
    df3 = content_database_3(df3)
    all=all(df,df2,df3)
    string_to_csv(all)
    csv_to_sql("merged_csv","plastic_free_boutique.db","customers")

_main()