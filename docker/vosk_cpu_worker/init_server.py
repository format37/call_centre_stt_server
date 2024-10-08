#from vosk import Model, KaldiRecognizer, SetLogLevel
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
import asyncio
import websockets
import socket
import urllib
import glob
import uuid
import logging
import httpx
import difflib
from ruts import DiversityStats


class stt_server:

	def __init__(self):

		# settings ++
		self.cpu_id = self.get_worker_id()
		cores_count = int(os.environ.get('WORKERS_COUNT', '0'))
		self.cpu_cores = [i for i in range(0, cores_count)]

		# enable logging
		logging.basicConfig(level=logging.INFO)
		self.logger = logging.getLogger(__name__)
		
		self.gpu_uri = os.environ.get(
			'VOSK_SERVER_WORKER_'+str(self.cpu_id), 
			os.environ.get('VOSK_SERVER_DEFAULT', '')
			)
		
		message = str(datetime.datetime.now())+'\n'
		message += 'New vosk worker: '+str(self.cpu_id)+' # '+self.gpu_uri
		# self.send_to_telegram(message)

		# ms sql
		self.sql_name = 'voice_ai'

		# mysql
		self.mysql_name = {
			1: 'MICO_96',
			2: 'asterisk',
		}

		self.source_id = 0
		self.sources = {
			'call': 1,
			'master': 2,
		}

		self.original_storage_path = {
			1: 'audio/stereo/', # call centre records path
			2: 'audio/mono/' # masters records path
		}
		self.saved_for_analysis_path = 'audio/wer/'
		self.confidence_of_file = 0
		# settings --

		self.temp_file_path = ''
		self.temp_file_name = ''		
			
		self.conn = self.connect_sql()
		"""self.mysql_conn = {
			1: self.connect_mysql(1),
			2: self.connect_mysql(2),
		}"""

		self.send_to_telegram('cpu '+str(self.cpu_id)+' started')

	def log_deletion(self, filename):
		current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		connector = mysql.connect(
			host = '10.2.4.87',
			user = 'root',
			passwd = 'root'
		)
		connector.autocommit(True)
		cursor = connector.cursor()
		cursor.execute("use ml")
		cursor.execute("INSERT INTO deletions(date, filename) VALUES ('"+current_date+"', '"+filename+"');")

	def get_worker_id(self):

		workers_count = int(os.environ.get('WORKERS_COUNT', '0'))
		# hostname = str(socket.gethostname())
		# generate unique id
		unique_id = str(uuid.uuid4())

		with open('id_garden/'+unique_id, "w") as f:
			f.write('')

		files = []
		while len(files)<workers_count:
			for root, dirs, files in os.walk('id_garden'):
				filenames = sorted([filename for filename in files])
				break

		for i in range(0, len(filenames)):
			if filenames[i] == unique_id:
				break

		return i

	def send_to_telegram(self, message):
		try:
			current_date = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
			token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
			chat_id = os.environ.get('TELEGRAM_CHAT', '')
			session = requests.Session()
			get_request = 'https://api.telegram.org/bot' + token	
			get_request += '/sendMessage?chat_id=' + chat_id
			get_request += '&text=' + urllib.parse.quote_plus(current_date + ' vosk_worker: ' + message)
			session.get(get_request)
		except Exception as e:
			self.logger.info('send_to_telegram error: '+str(e))
			self.logger.info('message: '+message)
			
	def connect_sql(self):

		return pymssql.connect(
			server=os.environ.get('MSSQL_SERVER', ''),
			user=os.environ.get('MSSQL_LOGIN', ''),
			password=os.environ.get('MSSQL_PASSWORD', ''),
			database=self.sql_name,
			tds_version=r'7.0'
			#autocommit=True
		)		

	"""def connect_mysql(self, source_id):

		return mysql.connect(
			host=os.environ.get('MYSQL_SERVER', ''),
			user=os.environ.get('MYSQL_LOGIN', ''),
			passwd=os.environ.get('MYSQL_PASSWORD', ''),
			db=self.mysql_name[source_id],
			# autocommit = True
			# cursorclass=mysql.cursors.DictCursor,
		)"""

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

	def delete_current_queue(self, original_file_name, linkedid):

		cursor = self.conn.cursor()
		"""if self.source_id == self.sources['master']:
			sql_query = "delete from queue where linkedid = '" + linkedid + "';"
		else:"""
		sql_query = "delete from queue where filename = '"+original_file_name+"';"
		cursor.execute(sql_query)
		self.conn.commit() # autocommit

	def delete_source_file(self, original_file_path, original_file_name, linkedid):

		#if self.source_id == self.sources['call']:
		myfile = original_file_path + original_file_name
		try:			
			os.remove(myfile)
			self.log_deletion(myfile)
			print('succesfully removed', myfile)
			# debug ++
			# self.send_to_telegram('delete_source_file removed: ' + str(myfile))
			# debug --
		except OSError as e:  ## if failed, report it back to the user ##
			print("Error: %s - %s." % (e.filename, e.strerror))
			self.send_to_telegram('delete_source_file error:\n' + str(e))

	def summarization_add_queue_deprecated(self, linkedid, record_date, side, phrases_count, text, version, source_id):
		current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		text_length = len(text)
		query = "insert into summarization_queue"
		query += "(linkedid, record_date, append_date, side, phrases_count, text, text_length, version, source_id) "
		query += " values("
		query += "'"+str(linkedid)+"',"
		query += "'"+str(record_date)+"',"
		query += "'"+str(current_date)+"',"
		query += str(side)+","
		query += "'"+str(phrases_count)+"',"
		query += "'"+str(text)+"',"
		query += "'"+str(text_length)+"',"
		query += "'"+str(version)+"',"
		query += "'"+str(source_id)+"'"
		query += ");"

		cursor = self.conn.cursor()
		cursor.execute(query)


	def accept_feature_extractor(self, sentences, accept):
		if len(accept) > 1 and accept['text'] != '':
			accept_text = str(accept['text'])
			conf_score = []
			i = 0
			accept_start = 0
			accept_end = 0
			for result_rec in accept['result']:
				if i==0:
					accept_start = result_rec['start']					
				conf_score.append(float(result_rec['conf']))			
				i+=1
			if i>0:
				accept_end = result_rec['end']
			sentences.append(
				{
					'text': accept_text,
					'start': accept_start,
					'end': accept_end,
					'conf': sum(conf_score)/len(conf_score)
				}
			)
		return sentences


	def accept_feature_extractor_whisper(self, sentences, accept, max_length=900, check_repetitions=False, segment_repetitions=False):
		if segment_repetitions:
			current_texts = set()
			segments_to_keep = []
			for segments_rec in accept["segments"]:
				segment_text = str(segments_rec["text"]).replace("'", "")[:max_length]
				if segment_text not in current_texts or len(segment_text) <= 9:
					segments_to_keep.append(segments_rec)
					if len(segment_text) > 9:
						current_texts.add(segment_text)
			accept["segments"] = segments_to_keep

		for segments_rec in accept["segments"]:
			segment_text = str(segments_rec["text"]).replace("'", "")[:max_length]

			if check_repetitions:
				phrases = [phrase.strip() for phrase in re.split(r"[.!?]\s*", segment_text) if " " in phrase and len(phrase) >= 10]
				s = difflib.SequenceMatcher(None)
				repetitions = []
				for i, phrase_i in enumerate(phrases[:-1]):
					for j, phrase_j in enumerate(phrases[i+1:], start=i+1):
						s.set_seqs(phrase_i, phrase_j)
						if s.ratio() >= 0.9:
							repetitions.append((i, j))
				if repetitions:
					end_indices = [segment_text.find(phrases[j]) + len(phrases[j]) for _, j in repetitions]
					last_repetition_end_index = max(end_indices)
					segment_text = segment_text[:last_repetition_end_index+1]

			segment_start = segments_rec["start"]
			segment_end = segments_rec["end"]
			try:
				conf_score = float(segments_rec["confidence"])
			except:
				conf_score = 0
				print("Conf_score did not calculated")
			sentences.append(
				{
					"text": segment_text,
					"start": segment_start,
					"end": segment_end,
					"confidence": conf_score,
				}
			)
		return sentences


	async def transcribation_process(
		self,
		duration, 
		side, 
		original_file_name, 
		rec_date, 
		src, 
		dst, 
		linkedid, 
		file_size, 
		queue_date,
		transcribation_date,
		max_length=900
		):
		trans_start = time.time()		
		logger_text = ' size: ' + str(file_size)
		logger_text += ' file: ' + self.temp_file_path + self.temp_file_name			

		self.logger.info(logger_text)

		transcriber = 0
		self.logger.info(f'self.gpu_uri: {self.gpu_uri}')

		# VOSK
		if self.gpu_uri[:3] == 'ws:':
			self.logger.info('vosk transcriber')
			while True:
				try:
					async with websockets.connect(
						self.gpu_uri,
						open_timeout=20,
						ping_interval=40,
						ping_timeout=40,
						close_timeout=20
						) as websocket:

						sentences = []

						wf = wave.open(self.temp_file_path + self.temp_file_name, 'rb')
						await websocket.send(
							'{ "config" : { "sample_rate" : %d } }' % (wf.getframerate())
							)

						buffer_size = int(wf.getframerate() * 0.2)  # 0.2 seconds of audio
						while True:
							data = wf.readframes(buffer_size)

							if len(data) == 0:
								break

							await websocket.send(data)
							try:
								accept = json.loads(await websocket.recv())
								sentences = self.accept_feature_extractor(sentences, accept)
							except websockets.exceptions.ConnectionClosedError:
								self.logger.error("The connection was closed during part of audio file processing")
								raise

						await websocket.send('{"eof" : 1}')
						try:
							accept = json.loads(await websocket.recv())
							sentences = self.accept_feature_extractor(sentences, accept)
						except websockets.exceptions.ConnectionClosedError:
							self.logger.error("The connection was closed during audio file processing")
							raise
					break
				except websockets.exceptions.ConnectionClosedError:
					self.logger.error("The connection was closed, reconnecting")
					continue
			# trans_end = time.time() # datetime.datetime.now()
			# self.perf_log(2, trans_start, trans_end, duration, linkedid)
		
		# WHISPER
		else:
			self.logger.info("whisper transcriber")
			transcriber = 1
			sentences = []
			file_path = self.temp_file_path + self.temp_file_name

			async with httpx.AsyncClient(timeout=None) as client:
				file = {"file": (os.path.basename(file_path), open(file_path, "rb"), "audio/wav")}
				attempt = 0
				max_attempts = 2

				try:
					while attempt < max_attempts:
						vad = "silero" if attempt == 0 else "auditok"
						data = {
							"source_id": self.source_id,
							"vad": vad
						}

						response = await client.post(
							self.gpu_uri, files=file, data=data
						)
						if response.status_code == 200:
							accept = response.json()
							check_repetitions = False
							segment_repetitions = False

							if len(accept) > 1 and accept["text"] != "":
								current_texts = set()
								for segments_rec in accept["segments"]:
									segment_text = str(segments_rec["text"]).replace("'", "")[:max_length]
									if segment_text in current_texts and len(segment_text) > 9:
										segment_repetitions = True
										self.logger.warning(f"Found this repeating text segment in the transcription: {segment_text}")
										break
									else:
										current_texts.add(segment_text)

								for segments_rec in accept["segments"]:
									segment_text = str(segments_rec["text"]).replace("'", "")[:max_length]
									ds = DiversityStats(segment_text).get_stats()
									if (len(segment_text) > 99 and ds["mttr"] > 0.1395 and ds["dttr"] < 7.2 and ds["simpson_index"] < 18.3):
										check_repetitions = True
										self.logger.warning(f"Found artifacts in this text segment: {segment_text}")
# self.save_file_for_analysis(self.temp_file_path, self.temp_file_name, duration)
										break

								if (check_repetitions or segment_repetitions) and attempt < max_attempts - 1:
									attempt += 1
									continue
								else:
									sentences = self.accept_feature_extractor_whisper(
										sentences,
										accept,
										check_repetitions=check_repetitions,
										segment_repetitions=segment_repetitions
									)
									break
							else:
								break
						else:
							self.logger.error(f"Error in file processing: {response.text}")
				except Exception as e:
					self.logger.warning("Whisper connection error: " + str(e))

		trans_end = time.time() # datetime.datetime.now()
		self.perf_log(2, trans_start, trans_end, duration, linkedid)

		# save to sql
		for i in range(0, len(sentences)):
			conf = sentences[i]['confidence'] if transcriber else sentences[i]['conf']
			self.save_result(
				duration,
				sentences[i]['text'],
				sentences[i]['start'],
				sentences[i]['end'],
				side,
				transcribation_date,
				str(conf),
				original_file_name,
				rec_date,
				src,
				dst,
				linkedid,
				file_size,
				queue_date,
				transcriber
				)
				
		# phrases for summarization
		phrases = [sentences[i]['text'] for i in range(len(sentences))]
		# confidences for analysis
		if transcriber:
			confidences = [sentences[i]['confidence'] for i in range(len(sentences))]
		else:			
			confidences = [sentences[i]['conf'] for i in range(len(sentences))]
		return len(sentences), phrases, confidences


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

		file_saved_for_analysis = False

		transcribation_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		
		phrases_count = 0
		# If server address starting from "ws://" then use vosk
		# if self.gpu_uri[:3] == 'ws:':
		phrases_count, phrases, confidences = asyncio.get_event_loop().run_until_complete(
			self.transcribation_process(
				duration, 
				side, 
				original_file_name, 
				rec_date, 
				src, 
				dst, 
				linkedid, 
				file_size, 
				queue_date,
				transcribation_date
				)
			)

		if len(confidences):
			self.confidence_of_file = sum(confidences)/len(confidences)
		else:
			self.confidence_of_file = 0
		
		# save for analysis if phrases count < 3 and duration > 300
#		if phrases_count < 3 and duration > 300 and not file_saved_for_analysis:
#			self.save_file_for_analysis(self.temp_file_path, self.temp_file_name, duration)

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
				queue_date,
				0
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
			queue_date,
			transcriber
		):
		if not str(rec_date) == 'Null' and \
				len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(rec_date))) == 0:
			self.logger.error(str(linkedid)+' save_result - wrong rec_date: '+str(rec_date)+' converting to Null..')
			rec_date = 'Null'

		cursor = self.conn.cursor()
		
		# Transcribation_date should be After transcribation
		transcribation_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		
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
		sql_query += " queue_date,"
		sql_query += " model)"
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
		sql_query += " '" + str(queue_date) + "',"
		sql_query += " " + str(transcriber) + ");"

		try:
			cursor.execute(sql_query)
			self.conn.commit() # autocommit
		except Exception as e:
			self.logger.error(str(linkedid)+' query error: '+sql_query+' '+str(e))
			sys.exit('save_result')

	def remove_temporary_file(self):
		if self.source_id == self.sources['call']:
			print('removing',self.temp_file_path + self.temp_file_name)
			try:
				os.remove(self.temp_file_path + self.temp_file_name)
				self.log_deletion(self.temp_file_path + self.temp_file_name)
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
			#else:
			#	self.save_file_for_analysis(filepath, filename, file_duration)

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
		return file_duration

	def wer_file_exist(self):
		
		wav_count = len(glob.glob(self.saved_for_analysis_path + '/*.wav'))
		files_count_limit = int(os.environ.get('WER_FILES_COUNT_LIMIT', '0'))
		
		if files_count_limit == 0:
			return True
		else:
			return wav_count>files_count_limit

	def save_file_for_analysis(self, file_path, file_name, duration):
		if int(os.environ.get('SAVE_FOR_ANALYSIS', '0'))==1:	
			current_date = datetime.datetime.now().strftime('%Y-%m-%d')
			prefix = 'cpu'+str(self.cpu_id)+'_duration'+str(duration)+'_'+current_date+'_'
			copyfile(file_path + file_name, self.saved_for_analysis_path + prefix + file_name)
