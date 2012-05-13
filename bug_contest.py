#!/usr/bin/python2.6
# -*- coding: utf-8 -*-

import gdata.calendar.data
import gdata.calendar.client
import atom.data
import urllib
import HTMLParser
import string
import sqlite3
import json
import time
import sys

_datsrc = r'http://contests.acmicpc.info/contests.json'
_dbname = r'contests.db'
_tblname = r'tbl_item'
_curi = r'http://www.google.com/calendar/feeds/mh8mjnllutqs9m5eb2dbi921j4@group.calendar.google.com/private/full'

_conn = sqlite3.connect(_dbname)
_curs = _conn.cursor()

_curs.execute('''
CREATE TABLE IF NOT EXISTS ''' + _tblname + '''(
	id	TEXT	PIRMARY,
	name	TEXT,
	week	TEXT,
	start_time	TEXT,
	link	TEXT,
	access	TEXT,
	oj	TEXT,
	status	TEXT
)
''')

class Dao():

	def __init__(self):
		"init dao"
	
	def __del__(self):
		"destory after commit"
		_conn.commit()
	
	def add(self, data):
		"insert new record"
		n = len(data)
		if n <= 0:
			return False
		dk = data.keys()
		s = 'INSERT INTO ' + _tblname + '(' + ','.join(dk) + ') VALUES(' + ','.join(['?']*n) + ')'
		v = tuple([data[k] for k in dk])
	
		_curs.execute(s, v)
		_conn.commit()

		return True

	def modify(self, data, cond = {'1':'1'}):
		"modify by condition"
		n = len(data)
		if n <= 0:
			return False
		dk = data.keys()
		ck = cond.keys()
		s = 'UPDATE ' + _tblname + ' SET ' + ','.join([k + '=?' for k in dk]) \
			+ ' WHERE ' + ','.join([k + '=?' for k in ck])
		v = tuple([data[k] for k in dk] + [cond[k] for k in ck])
		
		_curs.execute(s, v)
		_conn.commit()

		return True

class Calendar:
	def __init__(self, email, password):
		self.cal_client = gdata.calendar.client.CalendarClient(source='Google-Calendar_Python_Autopost_wolf5x-0.1')
		self.cal_client.ClientLogin(email, password, self.cal_client.source)

	def format_time(self, tm):
		"convert struct time to calendar time string"
		return time.strftime('%Y-%m-%dT%H:%M:%S.000Z', tm)

	def insert_single_event(self, title, content, where,
			start_time = None, end_time = None,
			tz = 0, length = 7200):
		event = gdata.calendar.data.CalendarEventEntry()
		event.title = atom.data.Title(text = title)
		event.content = atom.data.Content(text = content)
		event.where.append(gdata.data.Where(value = where))

		tz = -tz * 3600
		# convert time_zone time to localtime
		if start_time is None:
			start_time = time.localtime()
		else:
			start_time = time.localtime(time.mktime(start_time) + time.timezone - tz)
		if end_time is None:
			end_time = time.localtime(time.mktime(start_time) + length)
		else:
			end_time = time.localtime(time.mktime(end_time) + time.timezone - tz)
		
		# convert localtime to gmtime
		start_time = time.gmtime(time.mktime(start_time))
		end_time = time.gmtime(time.mktime(end_time))

		start_time_str = self.format_time(start_time)
		end_time_str = self.format_time(end_time)
		event.when.append(gdata.calendar.data.When(start=start_time_str, end=end_time_str))

		new_event = self.cal_client.InsertEvent(event, _curi)

		return new_event

def logger(flag='DEBUG', msg=''):
	stime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
	print '%s [%s] %s' %(stime.encode('utf-8'), flag.encode('utf-8'), msg.encode('utf-8'))

def grab_data():
	"grab contest list from nkoj"
	lst = None
	tries = 3
	while tries > 0:
		tries = tries - 1
		try:
			sock = urllib.urlopen(_datsrc)
			src = sock.read()
			sock.close()	
			lst = json.loads(src)
		except IOError as (errno, strerr):
			logger('WARN', '[%d][%s]' %(errno, strerr))
			logger('WARN', 'Retry in 10 seconds.')
			time.sleep(10)
		except:
			logger('ERROR', 'Unexpected error[%s]' %(sys.exc_info()[0]))
		else:
			break
	return lst

def proceed(itemlst):
	"proceed items and update database"
	if itemlst is None:
		return

	cal = Calendar(r'wolf5xzh', r'gmail5705!#&')
	dbc = Dao()

	for item in itemlst:
		_curs.execute('select status from ' + _tblname + ' where id=?', (item['id'],))
		res = _curs.fetchall()

		flag = 0
		if len(res) == 0:
			flag = 1
		elif res[0][0] != '1':
			flag = 2


		if flag != 0:
			new_event = cal.insert_single_event(
					title = item['name'],
					content = 'access:[%s]' %(item['access']),
					where = HTMLParser.HTMLParser().unescape(item['link']),
					start_time = time.strptime(item['start_time'], '%Y-%m-%d %H:%M:%S'),
					tz = 8)
			item.setdefault('status',1)
			if flag == 1:
				dbc.add(item)
			elif flag == 2:
				dbc.modify(item, {'id':item['id']})

			logger('INFO', 'Succeeded adding contest [%s][%s][%s].' %(item['id'], item['start_time'], item['name']))

if __name__=='__main__':
	logger('INFO', 'Schedule job started.')
	itemlst = grab_data()
	proceed(itemlst)
	logger('INFO', 'Schedule job finished.')

