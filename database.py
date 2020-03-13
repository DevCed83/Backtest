#Database clean up
import os 
import sqlite3
import data_processor as dpr
import indicators as idc
import pandas as pd
import time

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

database_name = "Swing_Trader_5m.db"
def database_init (database_name):
	try :
		database = sqlite3.connect(str(database_name))
		cursor = database.cursor()
		cursor.execute('''CREATE TABLE Transactions (
			id integer primary key,
			timestamp integer,
			price float,
			amount float)
			''')
		cursor.execute('''CREATE TABLE Candles (
			id integer primary key,
			candle_id float,
			open float, 
			high float, 
			low float, 
			close float,
			volume float)
			''')
		cursor.execute('''CREATE TABLE Real_Time_Data (
			id integer primary key,
			timestamp integer,
			price float,
			amount float,
			candle_id integer,
			open float, 
			high float, 
			low float, 
			close float,
			volume float,
			sma_24_period float, 
			stdev_24_period float,
			upper_band float,
			lower_band float, 
			buy_trigger float,
			sell_trigger float)
			''')
		cursor.execute('''
			CREATE TABLE Indicators(
			id integer primary key,
			candle_id, integer,
			close, float
			)
			''')
		cursor.execute('''
			CREATE TABLE Balance(
			timestamp INTEGER,
			eur_available float,
			btc_available float,
			eur_reserved float,
			btc_reserved float,
			total_value_eur float,
			total_value_btc float,
			)
			''')
		database.commit()

		database.close()
		print ("Database " + database_name + " created.")
	except Exception as e:
		print ("initial setup error\n" + str (e))
		exit (1)

def get_all():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT * FROM Real_Time_Data''')
	book = [row for row in cursor]
	cursor.close()
	database.close()
	return book

def remove_sma_holes():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''DELETE FROM Real_Time_Data
		WHERE lower_band IS NULL''')
	cursor.close()
	database.close()

def get_old_lines():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT * FROM Real_Time_Data
					WHERE lower_band IS NULL''')
	book = [row for row in cursor]
	cursor.close()
	database.close()
	return book
"""
def update_records():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''UPDATE Real_Time_Data
					SET
					WHERE lower_band IS NULL''')
	book = [row for row in cursor]
	cursor.close()
	database.close()
	return book
"""

def test_book(timeframe, window, start_id):
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT * FROM Transactions 
					WHERE id > :id
					''',{"id" : start_id})
	#ORDER BY id ASC
	book = cursor.fetchall()
	#book = [row for row in cursor]
	cursor.close()
	database.close()
	return book


def find_missing_part():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT * FROM Transactions 
					WHERE id > :id
					''',{"id" : start_id})
	#ORDER BY id ASC
	book = cursor.fetchall()
	#book = [row for row in cursor]
	cursor.close()
	database.close()
	return book

	
def get_book_length():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT id
					FROM Real_Time_Data
					order by id desc
					limit 1''',)
	data = cursor.fetchone()
	return data[0]

def get_rtd(transaction_id):
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT *
					FROM Real_Time_Data 
					WHERE id = :transaction_id
					order by id desc
					limit 1''',{"transaction_id" : transaction_id})
	data = cursor.fetchone()
	cursor.close()
	database.close()
	if data == None:
		return {"candle_id" : -1}
	else:
		rtd = {"id": data[0],
				"timestamp" : data[1],
				"price" : data[2],
				"amount" : data[3],
				"candle_id": data[4],
				"open" : data[5],
				"high":data[6],
				"low": data[7],
				"close": data[8],
				"volume" : data[9]
				}
		return rtd

		
def get_close():
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT close
					FROM Real_Time_Data 
					''')

	closes = [int(line[0]) for line in cursor]
	data_frame = pd.DataFrame(closes, columns = ["close"])
	return data_frame

def log_candle(candle):
	database = sqlite3.connect(str(database_name))
	cursor  = database.cursor()
	cursor.execute('''INSERT into Candles 
					VALUES (:candle_id,
							:id, 
							:open,
							:high,
							:low,
							:close,
							:volume)
			''', candle)
	database.commit()
	cursor.close()
	database.close()

def get_current_price_list(transaction_id, candle_id, timeframe):
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	current_price_list = []
	cursor.execute('''SELECT price 
		FROM Transactions 
		WHERE (id <= :transaction_id) AND (timestamp /:timeframe>= :candle_id )
		''', {"transaction_id": transaction_id, "candle_id": candle_id, "timeframe" : timeframe})
	#print (cursor)
	current_price_list = [row[0] for row in cursor]
	cursor.close()
	database.close()
	return current_price_list

def get_current_volume(transaction_id, candle_id, timeframe):
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT amount 
		FROM Transactions
		WHERE (id <= :transaction_id) AND (timestamp / :timeframe >= :candle_id)
		order by id desc 
		''', {"candle_id": candle_id, "transaction_id" : transaction_id, "timeframe": timeframe})
	
	volume_list = [row[0] for row in cursor]
	cursor.close()
	database.close()
	return sum(volume_list)
#@timeit
def log_rtd(rtd):
	database = sqlite3.connect(str(database_name))
	cursor  = database.cursor()
	cursor.execute('''INSERT into Real_Time_Data 
					VALUES (:id,
							:timestamp,
							:price,
							:amount,
							:candle_id,
							:open,
							:high,
							:low,
							:close,
							:volume,
							:sma_24_period,
							:stdev_24_period,
							:upper_band,
							:lower_band,
							:buy_trigger,
							:sell_trigger)
							''', rtd)
	database.commit()
	cursor.close()
	database.close()

def get_window_list(rtd, window):
	database = sqlite3.connect(str(database_name))
	cursor = database.cursor()
	cursor.execute('''SELECT candle_id, close
					FROM Candles
					WHERE (candle_id < :candle_id)
					order by id asc
					limit :window''',{"candle_id" : rtd["candle_id"], "window": window})
	raw_list = [row for row in cursor]
	#print ("dtb iwndow list raw list", raw_list)
	cid_list = [row[0] for row in raw_list]
	#print ("dtb window list cid list", cid_list)
	close_list = [row[1] for row in raw_list]
	#print ("dtb window list close list", close_list)

	cursor.close()
	database.close()
	if cid_list == None:
		return {"candle_id":[], "close":[]}
	else :
		return {"candle_id":cid_list, "close":close_list}

def log_indicators(data):
	sqlite3.register_adapter(np.float64, lambda val: float(val))

	database = sqlite3.connect(str(database_name))
	cursor  = database.cursor()
	cursor.execute('''INSERT into Indicators 
					VALUES (:id,
							:candle_id,
							:close,
							:sma_24_period,
							:stdev_24_period,
							:upper_band,
							:lower_band,
							:buy_trigger,
							:sell_trigger)
			''', data)
	database.commit()
	cursor.close()
	database.close()

if __name__ == '__main__':
	database_init(database_name)