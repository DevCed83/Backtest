#ddata processor clean up
import json
import collections
import sys
import time
import numpy as np
import database as dtb
import pandas as pd
import indicators as idc


def get_candle_id(transaction, timeframe):
	#candle_id = int (transaction["timestamp"]) // timeframe
	candle_id = int (transaction["timestamp"]) // timeframe
	return candle_id


def get_ohlc(price_list):
	ohlc = {}
	ohlc["open"]=price_list[0]
	ohlc["high"]=max(price_list)
	ohlc["low"]=min(price_list)
	ohlc["close"]=price_list[-1]
	return (ohlc)

def is_new_candle(rtd):

	last_rtd = dtb.get_rtd(rtd['id'] - 1)
	#print ("dpr.is_new_candle last rtd", last_rtd)
	last_cid = last_rtd["candle_id"]
	#print ("dpr.new candle last cid ", last_cid)

	current_cid = rtd["candle_id"]

	#print ("dpr.new candle current cid ", current_cid)
	if (current_cid > last_cid) & (last_cid != -1): 
		dtb.log_candle(last_rtd)
		#print ("logging candle")

def tx_processor(transaction, timeframe):
	#print ("received :", transaction, timeframe)
	current_data = {}
	current_data["candle_id"] = get_candle_id(transaction, timeframe)

	current_price_list = dtb.get_current_price_list(transaction['id'], current_data["candle_id"], timeframe)
	current_price_list.append(transaction['price'])
	current_ohlc = get_ohlc(current_price_list)
	#print ("dpr.proc price list:", current_price_list)
	current_volume = dtb.get_current_volume(transaction['id'], current_data["candle_id"], timeframe)
	
	current_data["id"] = transaction["id"]
	current_data["open"] = current_ohlc["open"]
	current_data["high"] = current_ohlc["high"]
	current_data["low"] = current_ohlc["low"]
	current_data["close"] = current_ohlc["close"]
	current_data["volume"] = current_volume
	#indicators = idc.bollinger_bands(current_data)
	#print (indicators)
	#print ('dpr.txproc : current_data', current_data)

	#idc.bollinger_bands(current_data)
	
	#dtb.log_rtd(current_data)
	return current_data

def update_bankroll():
	a = 1 

def find_gap():
	book = dtb.get_all()
	for i in range (0, len(book)-1):
		if book[i][0] != book[i+1][0] - 1:
			print (book[i])
			print(book[i+1])
		#if book[i][-3] == None:
		#	print (book[i][0])
	print (len(book))	

def update_line():
	book = dtb.get_old_lines()
	print (book[0])
	for i in range (0, len(book)-1):
		if book[i][-3] == None:
			#delete line (id)
			#replace line
			a = 1

if __name__ == '__main__':
	find_gap()
