from vosk import Model, KaldiRecognizer, SetLogLevel
import json
import pymssql
import pymysql as mysql
import datetime
import os
import wave
import contextlib
import re
import pandas as pd
import sys
import time
import requests
from shutil import copyfile

class stt_server:

	def __init__(self, cpu_id):

		# settings ++
		self.cpu_id = cpu_id
		with open('cores.count', 'r') as file:
			cores_count = int(file.read().replace('\n', ''))
			file.close()
		self.cpu_cores = [i for i in range(0, cores_count)]

		# telegram
		# self.telegram_chat = '106129214' # alex
		self.telegram_chat = '-1001443983697'  # log 1c
		with open('telegram_bot.token', 'r') as file:
			self.telegram_bot_token = file.read().replace('\n', '')
			file.close()
		
		# ms sql
		self.sql_name = 'voice_ai'
		self.sql_server = '10.2.4.124'
		self.sql_login = 'ICECORP\\1c_sql'

		# mysql
		self.mysql_name = {
			1: 'MICO_96',
			2: 'asterisk',
		}
		self.mysql_server = '10.2.4.146'
		self.mysql_login = 'asterisk'

		self.script_path = '/home/alex/projects/call_centre_stt_server/'
		# self.model_path = '/home/alex/projects/vosk-api/python/example/model'
		self.model_path = '/mnt/share/audio_call/model_v0/model' # important to naming last directory 'model'
		self.source_id = 0
		self.sources = {
			'call': 1,
			'master': 2,
		}
		self.original_storage_path = {
			# 1: '/mnt/share/audio_call/MSK_SRVCALL/RX_TX/',
			1: '/mnt/share/audio/MSK_SRVCALL/RX_TX/',
			# 2: '/mnt/share/audio/MSK_SRVCALL/REC_IN_OUT/'
			2: '/mnt/share/audio_master/MSK_MRM/REC_IN_OUT/'
		}
		self.saved_for_analysis_path = '/mnt/share/audio_call/saved_for_analysis/'
		self.confidence_of_file = 0
		# settings --

		self.temp_file_path = ''
		self.temp_file_name = ''

		# store pass in file, to prevent pass publication on git
		with open(self.script_path+'sql.pass','r') as file:
			self.sql_pass = file.read().replace('\n', '')
			file.close()

		with open(self.script_path+'mysql.pass','r') as file:
			self.mysql_pass = file.read().replace('\n', '')
			file.close()
			
		self.conn = self.connect_sql()
		self.mysql_conn = {
			1: self.connect_mysql(1),
			2: self.connect_mysql(2),
		}

		self.model = Model(self.model_path)

	def send_to_telegram(self, message):
		headers = {
			"Origin": "https://api.telegram.org",
			"Referer": 'https://api.telegram.org/bot' + self.telegram_bot_token,
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'}
		url = 'https://api.telegram.org/bot' + self.telegram_bot_token
		url += '/sendMessage?chat_id=' + str(self.telegram_chat)
		url += '&text=' + message + '\nStt cpu: ' + str(self.cpu_id)
		try:
			requests.get(url, headers=headers)
		except Exception as e:
			print('Telegram send message error:', str(e))
			
	def connect_sql(self):

		return pymssql.connect(
			server=self.sql_server,
			user=self.sql_login,
			password=self.sql_pass,
			database=self.sql_name,
			#autocommit=True
		)

	def connect_mysql(self, source_id):

		return mysql.connect(
			host=self.mysql_server,
			user=self.mysql_login,
			passwd=self.mysql_pass,
			db=self.mysql_name[source_id],
			# autocommit = True
			# cursorclass=mysql.cursors.DictCursor,
		)

	def perf_log(self, step, time_start, time_end, duration, linkedid):
		print('perf_log', step)
		spent_time = (time_end - time_start)
		current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		cursor = self.conn.cursor()
		sql_query = "insert into perf_log("
		sql_query += "cores, event_date, step, time, cpu, file_name, duration, linkedid, source_id"
		sql_query += ") "
		sql_query += "values ("
		sql_query += str(len(self.cpu_cores)) + ", "
		sql_query += "'" + current_date + "', "
		sql_query += str(step) + ", "
		#sql_query += str(spent_time.seconds + spent_time.microseconds / 1000000) + ", "
		sql_query += str(spent_time) + ", "
		sql_query += str(self.cpu_id) + ", "
		sql_query += "'" + self.temp_file_name + "', "
		sql_query += "'" + str(duration) + "', "
		sql_query += "'" + str(linkedid) + "', "
		sql_query += "'" + str(self.source_id) + "');"
		try:
			cursor.execute(sql_query)
			self.conn.commit()
		except Exception as e:
			print('perf_log query error:', str(e), '\n', sql_query)
	
	def linkedid_by_filename(self, filename, date_y, date_m, date_d):

		filename = filename.replace('rxtx.wav', '')
		
		date_from = datetime.datetime(int(date_y), int(date_m), int(date_d))
		date_toto = date_from+datetime.timedelta(days=1)
		date_from = datetime.datetime.strptime(str(date_from), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
		date_toto = datetime.datetime.strptime(str(date_toto), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')

		mysql_conn = self.connect_mysql(self.source_id)

		with mysql_conn:
			query = """
			select				
				linkedid,
				SUBSTRING(dstchannel, 5, 4),
				src
				from PT1C_cdr_MICO as PT1C_cdr_MICO
				where 
					calldate>'"""+date_from+"""' and 
					calldate<'"""+date_toto+"""' and 
					PT1C_cdr_MICO.recordingfile LIKE '%"""+filename+"""%' 
					limit 1;"""

			cursor = mysql_conn.cursor()
			cursor.execute(query)
			for row in cursor.fetchall():
				linkedid, dstchannel, src = row[0], row[1], row[2]
				return linkedid, dstchannel, src
		return '', '', ''
	
	def make_file_splitted(self, side, original_file_path, original_file_name, linkedid, duration):

		#split_start = datetime.datetime.now()
		split_start = time.time()

		"""if self.source_id == self.sources['master']:
			self.temp_file_path = original_file_path
			if side == 0:
				# original_file_name = linkedid + '-in.wav'
				self.temp_file_name = linkedid + '-in.wav'
			else:
				# original_file_name = linkedid + '-out.wav'
				self.temp_file_name = linkedid + '-out.wav'
			print(side, 'master', self.temp_file_path + self.temp_file_name)"""

		#elif self.source_id == self.sources['call']:
		# crop '.wav' & append postfix
		self.temp_file_path = self.script_path+'files/'
		self.temp_file_name = original_file_name[:-4]+('_R' if side else '_L')+'.wav'

		os_cmd 	= 'ffmpeg -y -i '
		os_cmd += original_file_path
		os_cmd += original_file_name
		os_cmd += ' -ar 16000 -af "pan=mono|c0=F'
		os_cmd += 'R' if side else 'L'
		os_cmd += '" '
		os_cmd += self.temp_file_path
		os_cmd += self.temp_file_name

		try:
			os.system(os_cmd)
		except Exception as e:
			print('make_file_splitted error:',str(e))

		isfile = os.path.isfile(self.temp_file_path + self.temp_file_name)

		#split_end = datetime.datetime.now()
		split_end = time.time()
		self.perf_log(1, split_start, split_end, duration, linkedid)

		return isfile

	def delete_current_queue(self, original_file_name, linkedid):

		cursor = self.conn.cursor()
		"""if self.source_id == self.sources['master']:
			sql_query = "delete from queue where linkedid = '" + linkedid + "';"
		else:"""
		sql_query = "delete from queue where filename = '"+original_file_name+"';"
		cursor.execute(sql_query)
		self.conn.commit() # autocommit

	"""def send_to_telegram(self, message):
		headers = {
			"Origin": "http://scriptlab.net",
			"Referer": "http://scriptlab.net/telegram/bots/relaybot/",
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'}

		url = "http://scriptlab.net/telegram/bots/relaybot/relaylocked.php?chat=" + self.telegram_chat
		url += "&text=" + 'cpu: ' + self.cpu_id + ' # ' + message
		requests.get(url, headers=headers)"""

	def delete_source_file(self, original_file_path, original_file_name, linkedid):

		#if self.source_id == self.sources['call']:
		myfile = original_file_path + original_file_name
		try:
			os.remove(myfile)
			print('succesfully removed', myfile)
			# debug ++
			# self.send_to_telegram('delete_source_file removed: ' + str(myfile))
			# debug --
		except OSError as e:  ## if failed, report it back to the user ##
			print("Error: %s - %s." % (e.filename, e.strerror))
			self.send_to_telegram('delete_source_file error:\n' + str(e))


		#elif self.source_id == self.sources['master']:
		"""myfile = original_file_path + linkedid + '-in.wav'
		try:
			os.remove(myfile)
			print('succesfully removed', myfile)
		except OSError as e:  ## if failed, report it back to the user ##
			print("Error: %s - %s." % (e.filename, e.strerror))
		myfile = original_file_path + linkedid + '-out.wav'
		try:
			os.remove(myfile)
			print('succesfully removed', myfile)
		except OSError as e:  ## if failed, report it back to the user ##
			print("Error: %s - %s." % (e.filename, e.strerror))"""

	def transcribe_to_sql(
		self, 
		duration, 
		side, 
		original_file_name, 
		rec_date, 
		src, 
		dst, 
		linkedid, 
		file_size, 
		queue_date
		):

		trans_start = time.time() # datetime.datetime.now()

		# if self.source_id == self.sources['master']:
		# 	original_file_name = linkedid + ('-in.wav' if side == 0 else '-out.wav')

		transcribation_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		print('side:', side, 'file_size:', file_size, '### transcribing:', self.temp_file_path + self.temp_file_name)
		# read file
		wf = wave.open(self.temp_file_path + self.temp_file_name, "rb")

		# read model
		# self.model = Model(self.model_path)
		rec = KaldiRecognizer(self.model, wf.getframerate())

		# recognizing
		phrases_count = 0

		confidences = []

		while True:

			conf_score = []
			
			data = wf.readframes(4000)
			if len(data) == 0:
				break

			if rec.AcceptWaveform(data):
				accept = json.loads(rec.Result())
				if accept['text'] != '':

					accept_start = str(accept['result'][0]['start'])
					accept_end = accept['result'][-1:][0]['end']
					accept_text = str(accept['text'])
					
					for result_rec in accept['result']:
						conf_score.append(float(result_rec['conf']))
					conf_mid = str(sum(conf_score)/len(conf_score))
					confidences.append(sum(conf_score)/len(conf_score))
					# conf_score = []
					
					self.save_result(
						duration,
						accept_text,
						accept_start,
						accept_end,
						side,
						transcribation_date,
						conf_mid,
						original_file_name,
						rec_date,
						src,
						dst,
						linkedid,
						file_size,
						queue_date
					)
					
					phrases_count += 1

		if len(confidences):
			self.confidence_of_file = sum(confidences)/len(confidences)
		else:
			self.confidence_of_file = 0
		
		trans_end = time.time() # datetime.datetime.now()
		self.perf_log(2, trans_start, trans_end, duration, linkedid)
		
		# quality control
		self.save_file_for_analysis(self.temp_file_path, self.temp_file_name, duration)

		if phrases_count == 0:
			self.save_result(
				duration,
				'',
				'0',
				'0',
				side,
				transcribation_date,
				0,
				original_file_name,
				rec_date,
				src,
				dst,
				linkedid,
				file_size,
				queue_date
			)

	def save_result(
			self,
			duration,
			accept_text,
			accept_start,
			accept_end,
			side,
			transcribation_date,
			conf_mid,
			original_file_name,
			rec_date,
			src,
			dst,
			linkedid,
			file_size,
			queue_date
		):

		if not str(rec_date) == 'Null' and \
				len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(rec_date))) == 0:
			print(str(linkedid), 'save_result - wrong rec_date:', str(rec_date), 'converting to Null..')
			rec_date = 'Null'


		cursor = self.conn.cursor()
		sql_query = "insert into transcribations("
		sql_query += " cpu_id,"
		sql_query += " duration,"
		sql_query += " audio_file_name,"
		sql_query += " transcribation_date,"
		sql_query += " text,"
		sql_query += " start,"
		sql_query += " end_time,"
		sql_query += " side,"
		sql_query += " conf,"
		sql_query += " linkedid,"
		sql_query += " src,"
		sql_query += " dst,"
		sql_query += " record_date,"
		sql_query += " source_id,"
		sql_query += " file_size,"
		sql_query += " queue_date)"
		sql_query += " values ("
		sql_query += " " + str(self.cpu_id) + ","
		sql_query += " " + str(duration) + ","
		sql_query += " '" + original_file_name + "',"
		sql_query += " '" + transcribation_date + "',"
		sql_query += " '" + accept_text + "',"
		sql_query += " '" + str(accept_start) + "',"
		sql_query += " '" + str(accept_end) + "',"
		sql_query += " '" + str(side) + "',"
		sql_query += " '" + str(conf_mid) + "',"
		sql_query += " '" + str(linkedid) + "',"
		sql_query += " '" + str(src) + "',"
		sql_query += " '" + str(dst) + "',"
		sql_query += " " + str(rec_date) if str(rec_date) == 'Null' else "'" + str(rec_date) + "'"
		sql_query += " ,'" + str(self.source_id)+"'"
		sql_query += " ,'" + str(0 if file_size is None else file_size) + "',"
		sql_query += " '" + str(queue_date) + "');"

		try:
			cursor.execute(sql_query)
			self.conn.commit() # autocommit
		except Exception as e:
			print('query error:',sql_query) # DEBUG
			print(str(e))
			sys.exit('save_result')

		#save_end = time.time() # datetime.datetime.now()
		#self.perf_log(3, save_start, save_end, duration, linkedid)

	def remove_temporary_file(self):
		if self.source_id == self.sources['call']:
			print('removing',self.temp_file_path + self.temp_file_name)
			try:
				os.remove(self.temp_file_path + self.temp_file_name)
				# debug ++
				# self.send_to_telegram('remove_temporary_file removed: ' + str(self.temp_file_name))
				# debug --
			except Exception as e:
				msg = 'remove_temporary_file error:\n' + str(e)
				print(msg)
				self.send_to_telegram(msg)

	def get_sql_complete_files(self):

		cursor = self.conn.cursor()
		sql_query = "select distinct filename from queue where"
		sql_query += " source_id='" + str(self.source_id) + "'"
		sql_query += " order by filename;"
		cursor.execute(sql_query)
		complete_files = []
		for row in cursor.fetchall():
			complete_files.append(row[0])

		return complete_files

	def get_fs_files_list(self, queue):

		fd_list = []

		if self.source_id == self.sources['call']:
			for root, dirs, files in os.walk(self.original_storage_path[self.source_id]):
				for filename in files:
					if filename[-4:] == '.wav' and not filename in queue:
						rec_source_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', filename)
						if len(rec_source_date) and len(rec_source_date[0]):
							rec_date = rec_source_date[0][:10] + ' ' + rec_source_date[0][11:].replace('-', ':')

							if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', rec_date)) == 0:
								rec_date = 'Null'
								print('0 Unable to extract date:', root, filename)

							date_string = re.findall(r'\d{4}-\d{2}-\d{2}', filename)
							if len(date_string):
								date_y = date_string[0][:4]
								date_m = date_string[0][5:-3]
								date_d = date_string[0][-2:]
								linkedid, dst, src = self.linkedid_by_filename(filename, date_y, date_m, date_d)  # cycled query

								fd_list.append({
									'filepath': root+'/',
									'filename': filename,
									'rec_date': rec_date,
									'src': src,
									'dst': dst,
									'linkedid': linkedid,
									'version': 0,
								})
						else:
							print('1 Unable to extract date:', root, filename)
							self.send_to_telegram('1 Unable to extract date: ' + str(root) + ' ' + str(filename))
							self.save_file_for_analysis(root, filename, 0)
				# break # ToDo: remove

		elif self.source_id == self.sources['master']:
			files_list = []
			for (dirpath, dirnames, filenames) in os.walk(self.original_storage_path[self.source_id]):
				files_list.extend(filenames)

			files_extracted = 0
			files_withoud_cdr_data = 0

			# get record date
			for filename in files_list:
				if not filename in queue:
					try:
						file_stat = os.stat(self.original_storage_path[self.source_id] + filename)
						# f_size = file_stat.st_size
						file_age = time.time() - file_stat.st_mtime
					except Exception as e:
						print("get_fs_files_list / file_stat Error:", str(e))
						file_age = 0
					if "h.wav" in filename:
						try:
							if file_age > 3600:
								os.remove(self.original_storage_path[self.source_id] + filename)
								# debug ++
								# self.send_to_telegram('min. get_fs_files_list. removed: ' + str(filename))
								# debug --
								print(str(round(file_age/60)), 'min. get_fs_files_list. Removed:', filename)
							else:
								print(str(round(file_age/60)), 'min. get_fs_files_list. Skipped: ', filename)
							continue
						except OSError as e:  ## if failed, report it back to the user ##
							print("Error: %s - %s." % (e.filename, e.strerror))
							self.send_to_telegram('get_fs_files_list file delete error:\n' + str(e))

					rec_date = 'Null'
					version = 0
					r_d = re.findall(r'a.*b', filename)
					if len(r_d) and len(r_d[0]) == 21:
						try:
							rec_date = r_d[0][1:][:-1].replace('t', ' ')
							#print('v.1 date', rec_date)
							src = re.findall(r'c.*d', filename)[0][1:][:-1]
							dst = re.findall(r'e.*f', filename)[0][1:][:-1]
							linkedid = re.findall(r'g.*h', filename)[0][1:][:-1]
							version = 1
						except Exception as e:
							print("Error:", str(e))
							#self.send_to_telegram('v1 filename parse error: '+ filename +'\n' + str(e))

					if version == 0:


						rec_date = 'Null'
						uniqueid = re.findall(r'\d*\.\d*', filename)[0]
						cursor = self.mysql_conn[self.source_id].cursor()
						query = "select calldate, src, dst from cdr where uniqueid = '" + uniqueid + "' limit 1;"
						cursor.execute(query)  # cycled query
						src = ''
						dst = ''
						linkedid = uniqueid

						for row in cursor.fetchall():
							rec_date = str(row[0])
							print('v.0 date', rec_date)
							src = str(row[1])
							dst = str(row[2])

						if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', rec_date)) == 0:
							print('u:', uniqueid, 'r:', rec_date, 'Unable to extract date from filename:', filename)
							rec_date = 'Null'
							files_withoud_cdr_data += 1

					if not rec_date == 'Null':

						fd_list.append({
							'filepath': self.original_storage_path[self.source_id],
							'filename': filename,
							'rec_date': rec_date,
							'src': src,
							'dst': dst,
							'linkedid': linkedid,
							'version': version,
						})
						files_extracted += 1

			print('master extracted:', files_extracted, 'without cdr data:', files_withoud_cdr_data)

		df = pd.DataFrame(fd_list, columns=['filepath', 'filename', 'rec_date', 'src', 'dst', 'linkedid', 'version'])
		df.sort_values(['rec_date', 'filename'], ascending=True, inplace=True)

		return df.values
	
	def set_shortest_queue_cpu(self):
		
		cursor = self.conn.cursor()
		sql_query = '''
		IF OBJECT_ID('tempdb..#tmp_cpu_queue_len') IS NOT NULL
		DROP TABLE #tmp_cpu_queue_len;

		CREATE TABLE #tmp_cpu_queue_len
		(
		cpu_id INT,
		files_count int
		);

		INSERT INTO #tmp_cpu_queue_len 
		'''
		for i in self.cpu_cores:
			if i==0:
				sql_query += 'select 0 as cpu_id, 0 as files_count '
			else:
				sql_query += 'union all select '+str(i)+',0 '
		sql_query += 'union all	select cpu_id, count(filename) from queue group by cpu_id; '
		sql_query += 'select top 1 cpu_id, max(files_count)  from #tmp_cpu_queue_len group by cpu_id order by max(files_count), cpu_id;'	
		cursor.execute(sql_query)
		#self.conn.commit()  # autocommit
		result = 0
		for row in cursor.fetchall():
			result += 1
			self.cpu_id = int(row[0])
			# print('selected', self.cpu_id, 'cpu')
		if result == 0:
			print('error: unable to get shortest_queue_cpu')
			self.cpu_id = 0

	def get_source_id(self, source_name):
		for source in self.sources.items():
			if source[0] == source_name:
				return source[1]
		return 0

	def get_source_name(self, source_id):
		for source in self.sources.items():
			if source[1] == source_id:
				return source[0]
		return 0

	def add_queue(self, filepath, filename, rec_date, src, dst, linkedid, naming_version):

		try:
			file_stat = os.stat(filepath + filename)
			f_size = file_stat.st_size
			st_mtime = file_stat.st_mtime
		except Exception as e:
			f_size = -1
			st_mtime = 0
			print('file stat error:', str(e))
			self.send_to_telegram(str(e))

		if time.time() - st_mtime > 600:
			file_duration = self.calculate_file_length(filepath, filename)

			if file_duration == 0:
				message = 'zero file in queue: t[' + str(time.time() - st_mtime) + ']  '
				message += 's[' + str(f_size) + ']  '
				message += 'd[' + str(file_duration) + ']  '
				message += str(filename)
				# self.save_file_for_analysis(filepath, filename, file_duration)
				print(message)
				# self.send_to_telegram(message)
			else:
				self.save_file_for_analysis(filepath, filename, file_duration)

			cursor = self.conn.cursor()
			current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

			sql_query = "insert into queue "
			sql_query += "(filepath, filename, cpu_id, date, "
			sql_query += "duration, record_date, source_id, src, dst, linkedid, version, file_size) "
			sql_query += "values ('"
			sql_query += filepath + "','"
			sql_query += filename + "','"
			sql_query += str(self.cpu_id) + "','"
			sql_query += current_date + "','"
			sql_query += str(file_duration) + "',"
			sql_query += rec_date if rec_date == 'Null' else "'"+rec_date+"'"
			sql_query += ",'"
			sql_query += str(self.source_id) + "','"
			sql_query += str(src) + "','"
			sql_query += str(dst) + "','"
			sql_query += str(linkedid) + "',"
			sql_query += str(naming_version) + ","
			sql_query += str(f_size) + ");"

			try:
				cursor.execute(sql_query)
				self.conn.commit() # autocommit
			except Exception as e:
				print('add queue error. query: '+sql_query)
				print(str(e))
		else:
			message = 'queue skipped: t[' + str(time.time() - file_stat.st_mtime) + ']  '
			message += 's[' + str(file_stat.st_size) + ']  '
			# message += 'd[' + str(file_duration) + ']  '
			message += str(filename)
			# self.save_file_for_analysis(filepath, filename, file_duration)
			print(message)
			# self.send_to_telegram(message)

	def calculate_file_length(self, filepath, filename):
		file_duration = 0
		try:
			fname = filepath + filename
			with contextlib.closing(wave.open(fname, 'r')) as f:
				frames = f.getnframes()
				rate = f.getframerate()
				file_duration = frames / float(rate)
		except Exception as e:
			print('file length calculate error:', str(e))
			# self.save_file_for_analysis(filepath, filename, file_duration)
			# self.send_to_telegram('file length calculate error:\n'+fname+'\n'+str(e))
		return file_duration

	def delete_old_results(self):

		cur_date = datetime.datetime.now()
		DD = datetime.timedelta(days=int(365 / 2))
		crop_date = str(cur_date - DD)
		crop_date = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', crop_date)[0]
		bottom_limit = datetime.datetime.strptime(str(crop_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
		cursor = self.conn.cursor()
		sql_query = "delete from transcribations where record_date<'"+str(bottom_limit)+"';"
		cursor.execute(sql_query)
		self.conn.commit() # autocommit

	def wer_file_exist(self):
		
		current_date = datetime.datetime.now().strftime('%Y-%m-%d')
		comparator = 'cpu'+str(self.cpu_id)+'_'+current_date+'_'
		for root, dirs, files in os.walk(self.saved_for_analysis_path + 'wer'):
			for filename in files:
				if comparator in filename:
					return True
		return False

	def save_file_for_analysis(self, file_path, file_name, duration):

		try:
			midlle_confidence = 0.8697060696547252
			"""prefix = 'any/'
			# query = "SELECT avg(conf) FROM transcribations where not text = '';"			
			confidence_treshold_top = midlle_confidence + 0.1
			confidence_treshold_bottom = midlle_confidence - 0.1"""

			if duration == 0:
				prefix = 'zero/'
				copyfile(file_path + file_name, self.saved_for_analysis_path + prefix + file_name)
			#else:
			#	prefix = 'any/'
				#copyfile(file_path + file_name, self.saved_for_analysis_path + prefix + file_name)
			current_date = datetime.datetime.now().strftime('%Y-%m-%d')
			#self.confidence_of_file > 0.9 and \
			if	duration > 50 and duration < 60 and not self.wer_file_exist():
				prefix = 'wer/cpu'+str(self.cpu_id)+'_'+current_date+'_'
				copyfile(file_path + file_name, self.saved_for_analysis_path + prefix + file_name)
			else:
				print(
					'save_file_for_analysis skip:\nduration:', duration, 
					'\nconfidence', self.confidence_of_file
					)

			"""if duration > 10 and duration < 60:
				if self.confidence_of_file > confidence_treshold_top:
					prefix = 'hi/'
				elif self.confidence_of_file < confidence_treshold_bottom:
					prefix = 'low/'

				if prefix == 'hi/' or prefix == 'low/':
					#print('cp', file_path + file_name, 'to', self.saved_for_analysis_path + prefix + file_name)
					copyfile(file_path + file_name, self.saved_for_analysis_path + prefix + file_name)"""

		except Exception as e:
			print("Error:", str(e))
			self.send_to_telegram('save_file_for_analysis error:\n' + str(e))
