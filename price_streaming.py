import datetime
import json 
import time
import sqlite3
import database as dtb
import data_processor as dpr
import indicators as idc 

#First tryout indicates 640s for 10k entries. 
strategy_1 = {"name": "Bollinger bands",
			"window" : 24,
			"standard deviation" : 3}
scale_list = [300]
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print ('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed

def concat(row,rtd, indicator_values):
	rtd['timestamp'] = int(row[1])
	rtd['price'] = round(float(row[2]))
	rtd['amount'] = round(float(row[3]))
	rtd["sma_24_period"] = round (float(indicator_values["sma_24_period"]), 2)
	rtd["stdev_24_period"] = round (float(indicator_values["stdev_24_period"]), 2)
	rtd["upper_band"] = round (float(indicator_values["upper_band"]),2)
	rtd["lower_band"] = round (float(indicator_values["lower_band"]), 2)
	rtd["buy_trigger"] = bool(indicator_values["buy_trigger"])
	rtd["sell_trigger"] = bool(indicator_values["sell_trigger"])
	#print ("try type", type (final_form["id"]))
	return rtd

#@timeit
def loop(row, timeframe):
		info = {"id" : row[0], "timestamp" : row[1], "price" : row[2], "volume" : row[3]}
		rtd = dpr.tx_processor(info,timeframe)
		indicator_values = idc.bollinger_bands(rtd, strategy_1["window"], strategy_1["standard deviation"])
		dpr.is_new_candle(rtd)
		rtd = concat(row, rtd, indicator_values)
		dtb.log_rtd(rtd)
		#print ("str rtd", rtd)
		#print ("new candle tested")
		#print ("str indicators", indicator_values)
		#decision = dpr.validate_order(indicator_values)


			#print (" indicator values logged")
def start(txid):
	#, 180, 300, 600, 1800, 3600, 7200, 14400]	
	for timeframe in scale_list:
		print ('go', time.time())
	#exisitng_book = len (dtb.get_iloc())
	cursor = dtb.test_book(timeframe, strategy_1["window"], txid)
	for row in cursor:
		loop(row, timeframe)

if __name__ == "__main__":
	#use this to resume calculation
	#last_tx_id = dtb.get_book_length()
	#print (last_tx_id)
	
	#targetting missing lines
	last_tx_id = 199992
	start(last_tx_id)