# -*- coding: utf-8 -*-
"""
Created on Mon Nov 22 20:30:11 2021

@author: Aref Manghutay

A code Snippet for reading an specific telegram channel posts and sending message details through RESTful methods to server




"""
import sqlite3
from sqlite3 import Error
from telethon import TelegramClient, sync, events
import re
import glob
import asyncio
import csv
import os
import requests
import pandas as pd
import nest_asyncio
nest_asyncio.apply()
import requests
import uuid
#importing required Libraries

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn
# above function creates connection to the database
    
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
# above function creates desired table in the database
        
def sending_signals(symbolID,action,entry,target1,target2,target3,stop):
    
    TradePredictUniqueCode = uuid.uuid4()
    TradePredictShortOneTargetUniqueCode = uuid.uuid4()
    TradePredictShortTwoTargetUniqueCode = uuid.uuid4()
    TradePredictMediumOneTargetUniqueCode = uuid.uuid4()
    TradePredictMediumTwoTargetUniqueCode = uuid.uuid4()
    TradePredictLongOneTargetUniqueCode = uuid.uuid4()
    TradePredictLongTwoTargetUniqueCode = uuid.uuid4()

    payload = '''
        { "TradePredictId": null,
         "TradePredictUniqueCode": "arefJavad1",
         "AssetSymbolId": SymbolianID,
         "PredictTypeId": "1",
         "TradeTypeId": "buyOrSell",
         "TradeClassId": "2",
         "TradeCategoryId": "2",
         "TradePredictEntryPointLowPrice": "Entryanii",
         "TradePredictEntryPointHighPrice": "Entryanii",
         "TradePredictShortTarget": [
                 {
                         "TradePredictTargetUniqueCode": "arefJavad2",
                         "TradePredictTargetValue": ShortTargetanii
                         },
                         {
                                 "TradePredictTargetUniqueCode": "arefJavad3",
                                 "TradePredictTargetValue": ShortTargetanii2
                                 }
                         ],
         "TradePredictMidTarget": [ 
                 {
                         "TradePredictTargetUniqueCode": "arefJavad4",
                         "TradePredictTargetValue": MidTargetanii 
                         },
                         {
                                 "TradePredictTargetUniqueCode": "arefJavad5",
                                 "TradePredictTargetValue": MidTargetanii2
                                 }
                         ],
         "TradePredictLongTarget": [
                 {
                         "TradePredictTargetUniqueCode": "arefJavad6",
                         "TradePredictTargetValue": LongTargetanii
                         },
                         {
                                 "TradePredictTargetUniqueCode": "arefJavad7",
                                 "TradePredictTargetValue": LongTargetanii2
                                 }],
                 "TradeStopLoss": "Stopinannii",
                 "TradeTotalVolume": "1",
                 "TradePredictRisk": "10"
                 }'''
    url = "--your url here --"
    payload = payload.replace('arefJavad1',str(TradePredictUniqueCode))
    payload = payload.replace('arefJavad2',str(TradePredictShortOneTargetUniqueCode))
    payload = payload.replace('arefJavad3',str(TradePredictShortTwoTargetUniqueCode))
    payload = payload.replace('arefJavad4',str(TradePredictMediumOneTargetUniqueCode))
    payload = payload.replace('arefJavad5',str(TradePredictMediumTwoTargetUniqueCode))
    payload = payload.replace('arefJavad6',str(TradePredictLongOneTargetUniqueCode))
    payload = payload.replace('arefJavad7',str(TradePredictLongTwoTargetUniqueCode))
    
    payload = payload.replace('SymbolianID',str(symbolID))
    payload = payload.replace('buyOrSell',str(action))
    payload = payload.replace('Entryanii',str(entry))
    payload = payload.replace('ShortTargetanii',str(target1))
    payload = payload.replace('MidTargetanii',str(target2))
    payload = payload.replace('LongTargetanii',str(target3))
    
    payload = payload.replace('ShortTargetanii2',str(float(target1)+0.03))
    payload = payload.replace('MidTargetanii2',str(float(target2)+0.03))
    payload = payload.replace('LongTargetanii2',str(float(target3)+0.03))
    
    payload = payload.replace('Stopinannii',str(stop))


    headers = {
            'authority': '--your authority code here --',
            'Content-Type': 'text/plain'
            }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    #print(string)
      
        
# above function sends signals to server-specific format.this code created with the help of POSTMAN python-request code tool      
        
        
        
url = "--your url here --"
payload={}
headers = {}
response = requests.request("GET", url, headers=headers, data=payload)
#print(response.text)
header = "AssetSymbolIId,AssetSymbolId,AssetSymbolNName,AssetSymbolName,AssetSymbolSShort,AssetSymbolShort"
dataText = response.text[23:-189]
dataText = dataText.replace(':', ',')
dataText = dataText.replace('},{', '\n')
with open('coinLibrary.txt', 'w') as f:
    f.write(header + "\n" + dataText)
    f.close
data = pd.read_csv ('coinLibrary.txt', delimiter = ',')
coinData = data[['AssetSymbolId','AssetSymbolName','AssetSymbolShort']]

        

api_id = #-- your telegram api id here--
api_hash = #'--your telegram api hash here --'
client = TelegramClient('session_name', api_id, api_hash)
client.start()
@client.on(events.NewMessage)
async def my_event_handler(event):
    sender = await event.get_sender()
    if 'bye' in event.raw_text:
            if sender.username=='--Commad sender User ID here --' :
                await event.reply('Bye Bye Baby!')
                await client.disconnect()
    if sender.username=='--channel Id here --' :
        message_date = event.date
        message_id = event.id
        string = event.raw_text
        string = string.replace('#', '')
        string = string.replace(':', '')
        #print(string)
        pattern = '[\r\n]+'
        result = re.split(pattern, string)
        pattern_second = r'[-+]?\d*\.\d+|\d+'
        result_second = re.findall(pattern_second, string) 
        print(result)
        print(result_second)
        result_second = [float(i) for i in result_second]
        print(result_second)
        targets = result_second[1:]
        del targets[0]
        del targets[-1]
        print(targets)
        print(result_second)
        #recieved_signal =  {'id':'','time':'','signalType':result[0],
        #'signalFrom':result[1],'leverage':result_second[0],'entry':result_second[1],'target1':targets[0],
        #'target2':targets[1],'target3':targets[2],'stop':result_second[-1] }
        #print(recieved_signal)
        database = r"C:\sqlite\db\pythonsqlite.db"
        conn = sqlite3.connect(database)
        c = conn.cursor()
        print("Successfully Connected to SQLite")
        c.execute('''INSERT INTO signals (message_id,message_time,signal_type,signal_from,leverage
                          ,entry,target_1,target_2,target_3,stop) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',(message_id, message_date, result[0],result[1],
                                   result_second[0], result_second[1], targets[0], targets[1], targets[2],
                                   result_second[-1]))
    
        conn.commit()
        print("Record inserted successfully into signals table ", c.rowcount)
        c.close()
        symbolName = result[0].replace(" FUTURE","")
        symbolName = symbolName + 'USDT'
        targetData = coinData.loc[(coinData['AssetSymbolName'] == symbolName)]
        targetID = int(targetData.iloc[0]['AssetSymbolId'])
        print(targetID)
        action = ''
        if (targets[0]>=result_second[1]):
            action = '1'
        else:
                action = '2'
    
        print (action)
        sending_signals(targetID,action,result_second[1],targets[0],targets[1],targets[2],result_second[-1])    

# using telethon library to access telegram messages and decode signals by regex, then saving signals into SQlite database and sending them through POST method.    
    
async def main():
    
    database = r"C:\sqlite\db\pythonsqlite.db"
    sql_create_signals_table = """ CREATE TABLE IF NOT EXISTS signals (
                                        message_id integer,
                                        message_time text,
                                        signal_type text,
                                        signal_from text,
                                        leverage real,
                                        entry real,
                                        target_1 real,
                                        target_2 real,
                                        target_3 real,
                                        stop real
                                    ); """
    # create a database connection
    conn = create_connection(database)
    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_signals_table)

    else:
        print("Error! cannot create the database connection.")
    
    
    async with client:
        await client.run_until_disconnected()

    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    

    
    
