#taken from https://medium.com/python-data/setting-up-a-bollinger-band-with-python-28941e2fa300
# import needed libraries

import database as dtb 
import data_processor as dpr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



def get_adj_close():
    info = {}
    #print (current_close[0][0])
    current_close = dtb.get_current_close()

    processed = []
    processed.append(current_close[0][0])
    raw = dtb.get_closes()
    for elt in raw:
        processed.append(elt[0])
    info["close"] = processed
    return ( pd.DataFrame(info))
def prepare_data(buy_serie, sell_serie, price_serie):
    buy = np.asarray(btceur['buy_trigger'])
    return np.where (buy == True, update_balance(), )

    sell = np.asarray(btceur['sell_trigger'])
    price = np.asarray(btceur['close'])
    return (buy, sell, price)

def update_balance(buy_trigger, sell_trigger, price):

    if buy_trigger:
        #bankroll['euros'] -= (0.01 * btceur['close'].iloc[i])
        bankroll['euros'] -= (0.01 * price *1.0025)
        bankroll['btc'] += 0.01
        return(bankroll)

    elif sell_trigger:
        bankroll["sell_counter"]+=1
        #bankroll['euros'] += (0.01 * btceur['close'].iloc[i])
        bankroll['euros'] += (0.01 * price/1.0025)
        bankroll['btc'] -= 0.01
        return (bankroll)


    return (bankroll)
if __name__ == '__main__':
    btceur = dtb.get_close()

    # Calculate 24 Period Moving Average, Std Deviation, Upper Band and Lower Band
    btceur['24 Day MA'] = btceur['close'].rolling(window=24).mean()
    btceur['24 Day STD'] = btceur['close'].rolling(window=24).std()
    btceur['Upper Band'] = btceur['24 Day MA'] + (btceur['24 Day STD'] * 3)
    btceur['Lower Band'] = btceur['24 Day MA'] - (btceur['24 Day STD'] * 3)
    mask1 = btceur['Lower Band'] - btceur["close"] > 0 
    mask2 = btceur["close"]  - btceur['Upper Band'] > 0 
    btceur['buy_trigger'] = mask1
    btceur['sell_trigger'] = mask2
    
    buy_counter = 0 
    sell_counter = 0
    bankroll = {'euros' : 10000, 'btc' : 10, 'buy_counter' : 0, "sell_counter" : 0}
    vectfunc = np.vectorize(update_balance, otypes=[np.float],cache=False)
    #bankroll = list(vectfunc(btceur["buy_trigger"], btceur["sell_trigger"], btceur["close"]))
    print (bankroll)
    #for i in range (len(btceur['close'])):
    """
    cost = 5000 - bankroll['euros']
    product = round(bankroll['btc'] - 1, 8)
    current_price = btceur['close'].iloc[-1]
    print (buy_counter, sell_counter)
    print (bankroll)
    print ((5000 - bankroll['euros'])/(bankroll['btc'] - 1))
    print (5000 - bankroll['euros'])
    print(round(bankroll['btc'] - 1, 8))
    print ("profit")
    print (current_price * product - cost)
    """

    #btceur[['close', '24 Day MA', 'Upper Band', 'Lower Band']].plot(figsize=(12,6))
    btceur[['close']].plot(figsize=(12,6))
    plt.plot(btceur['close'][mask1], "X", color = 'yellow')
    plt.plot(btceur['close'][mask2], "o", color = 'red') 
    plt.title('5m BB 24, 3')
    plt.ylabel('Price (EUR)')
    plt.show()
