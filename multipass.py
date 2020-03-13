# non multiprocessed code is 5000 rows in 296.4s 59.28s / 1k
# first try outs at 5100 ( 3 times 1700 rows ) in 220.4s 43.2 / 1k
# 5* 1000 rows = 202.9 s 40.58s /1k
# 10 * 500 rows = 207s  41.4s / 1k
# 8 * 700 rows = 236.9s  42.1s / 1k
import datetime
import json 
import time
import sqlite3
import database as dtb
import data_processor as dpr
import indicators as idc 
import price_streaming as pst
from threading import Thread, RLock
import multiprocessing

#First tryout indicates 640s for 10k entries. 
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


if __name__ == "__main__":
	slices = 100000
	procs = 5
	"""
	run(0, 5000)
	"""
	jobs = []
	for i in range (0, procs):
		process = multiprocessing.Process(target = pst.start, args = (i*slices, slices))
		jobs.append(process)

	for j in jobs :
		j.start()

	for j in jobs:
		j.join()

	print ("list processing complete")

