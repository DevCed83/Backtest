#taken from https://medium.com/python-data/setting-up-a-bollinger-band-with-python-28941e2fa300
# import needed libraries

#indicators clean up 

import database as dtb 
import data_processor as dpr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

def bollinger_bands(rtd, window, stddev):
    data = dtb.get_window_list(rtd, window + 1)
    #print ("idc.boll bands data", data)
    #print (type(data["close"]))
    data["candle_id"].append(rtd["candle_id"])
    data["close"].append(rtd["close"])
    #print ("bool bands close list\n",data)
    btceur = pd.DataFrame(data)
    btceur["id"] = rtd["id"]
    #print ("idc boll bands df :",btceur['close'])
    btceur["sma_24_period"] = btceur['close'].rolling(window = window).mean()
    btceur["stdev_24_period"] = btceur['close'].rolling(window = window).std()
    btceur["upper_band"] = btceur["sma_24_period"] + (btceur["stdev_24_period"] * stddev)
    btceur["lower_band"] = btceur["sma_24_period"] - (btceur["stdev_24_period"] * stddev)
    #btceur['StochRSI'] = rsi_func(btceur['close'])
    mask1 = btceur["lower_band"] - btceur["close"] > 0 
    mask2 = btceur["close"]  - btceur["upper_band"] > 0 
    btceur['buy_trigger'] = mask1
    btceur['sell_trigger'] = mask2
    #print (type (btceur["upper_band"]))

    #print (btceur)
    try:
        line_to_feed = {}
        line_to_feed = btceur.iloc[-1]
        final_form = line_to_feed.to_dict()
        final_form["id"] = int(final_form["id"])
        final_form["candle_id"] = int(final_form["candle_id"])
        final_form["close"] = round (float(final_form["close"]), 2)
        final_form["sma_24_period"] = round (float(final_form["sma_24_period"]), 2)
        final_form["stdev_24_period"] = round (float(final_form["stdev_24_period"]), 2)
        final_form["upper_band"] = round (float(final_form["upper_band"]),2)
        final_form["lower_band"] = round (float(final_form["lower_band"]), 2)
        final_form["buy_trigger"] = str(final_form["buy_trigger"])
        final_form["sell_trigger"] = str(final_form["sell_trigger"])
        #print ("try type", type (final_form["id"]))
        return final_form

    except Exception as e:
        print ("iloc 0  exception", e)