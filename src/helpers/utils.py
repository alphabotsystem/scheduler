from time import time
from datetime import datetime

def seconds_until_cycle():
	return (time() + 60) // 60 * 60 - time()

def get_frequency_time(t):
	if t == "1D": return 86400
	elif t == "12H": return 43200
	elif t == "8H": return 28800
	elif t == "6H": return 21600
	elif t == "4H": return 14400
	elif t == "3H": return 10800
	elif t == "2H": return 7200
	elif t == "1H": return 3600
	elif t == "30m": return 1800
	elif t == "20m": return 1200
	elif t == "15m": return 900
	elif t == "10m": return 600
	elif t == "5m": return 300
	elif t == "3m": return 180
	elif t == "2m": return 120
	elif t == "1m": return 60

def get_accepted_timeframes(t):
	acceptedTimeframes = []
	for timeframe in ["1m", "2m", "3m", "5m", "10m", "15m", "20m", "30m", "1H", "2H", "3H", "4H", "6H", "8H", "12H", "1D"]:
		if t.second % 60 == 0 and (t.hour * 60 + t.minute) * 60 % get_frequency_time(timeframe) == 0:
			acceptedTimeframes.append(timeframe)
	return acceptedTimeframes