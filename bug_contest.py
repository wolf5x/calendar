#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-

import gdata.calendar.data
import gdata.calendar.client
import atom.data
import urllib
import HTMLParser
import string
import sqlite3
import json
from datetime import datetime, timedelta
import time
import sys, getopt

_datsrc = r'http://contests.acmicpc.info/contests.json'
_dbname = r'contests.db'
_tblname = r'tbl_item'
_curi = r'http://www.google.com/calendar/feeds/mh8mjnllutqs9m5eb2dbi921j4@group.calendar.google.com/private/full'
_username = None
_password = None

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
		return tm.strftime('%Y-%m-%dT%H:%M:%S.000Z')

	def to_utctime(self, tm, tz):
		"convert to utc+0000 time as if tm is in timezone tz"
		return tm - timedelta(hours=tz)

	def insert_single_event(self, title, content, where,
			start_time = None, end_time = None,
			tz = 0, length = 7200):
		"tz: timezone offset in hour. length: in second."
		event = gdata.calendar.data.CalendarEventEntry()
		event.title = atom.data.Title(text = title)
		event.content = atom.data.Content(text = content)
		event.where.append(gdata.data.Where(value = where))

		# convert timezone time to utctime
		if start_time is None:
			start_time = datetime.utcnow()
		else:
			start_time = self.to_utctime(start_time, tz)
		if end_time is None:
			end_time = start_time + timedelta(seconds=length)
		else:
			end_time = self.to_utctime(end_time, tz)
		
		start_time_str = self.format_time(start_time)
		end_time_str = self.format_time(end_time)
		event.when.append(gdata.calendar.data.When(start=start_time_str, end=end_time_str))

		new_event = self.cal_client.InsertEvent(event, _curi)

		return new_event

def logger(flag='DEBUG', msg=''):
	stime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

	cal = Calendar(_username, _password)
	dbc = Dao()

	for item in itemlst:
		_curs.execute('select status from ' + _tblname + ' where id=?', (item['id'],))
		res = _curs.fetchall()

		"flag: 0:do nothin; 1:add new; 2:modify"
		flag = 0
		if len(res) == 0:
			flag = 1
		elif res[0][0] != '1':
			flag = 2

		if flag != 0:
			new_event = cal.insert_single_event(
					title = '[%s] %s' %(item['oj'], item['name']),
					content = 'access:[%s]' %(item['access']),
					where = HTMLParser.HTMLParser().unescape(item['link']),
					start_time = datetime.strptime(item['start_time'], '%Y-%m-%d %H:%M:%S'),
					tz = 8)
			item.setdefault('status',1)
			if flag == 1:
				dbc.add(item)
			elif flag == 2:
				dbc.modify(item, {'id':item['id']})

			logger('INFO', 'Succeeded adding contest [%s][%s][%s].' %(item['id'], item['start_time'], item['name']))

def help_and_exit():
		print 'bug_contest.py -u <username> -p <password>'
		sys.exit(2)

def main(argv):
	global _username, _password
	"parse args"
	try:
		opts, args = getopt.getopt(argv, 'u:p:', ["username=","password="])
	except getopt.GetoptError:
		help_and_exit()
	for opt, arg in opts:
		if opt in ('-u', '--username'):
			_username = arg
		elif opt in ('-p', '--password'):
			_password = arg
	if _username == None or _password == None:
		help_and_exit()
	
	"do job"
	logger('INFO', 'Schedule job started.')
	itemlst = grab_data()
	proceed(itemlst)
	logger('INFO', 'Schedule job finished.')


if __name__=='__main__':
	main(sys.argv[1:])

