import sys
import time
import datetime
from init_server import stt_server
import re
import os

print('cpu', sys.argv[1])
server_object = stt_server(sys.argv[1])
cursor = server_object.conn.cursor()
sql_query = "select filepath, filename, duration, source_id, "
sql_query += "record_date, src, dst, linkedid from queue "
sql_query += "where cpu_id='"+str(server_object.cpu_id)+"' "
sql_query += "and source_id = '2' " # debug
sql_query += "order by record_date, linkedid, filename;"
processed = 0
cursor.execute(sql_query)
linkedid = ''
for row in cursor.fetchall():

	original_file_path = row[0]
	original_file_name = row[1]
	original_file_duration = row[2]
	server_object.source_id = row[3]
	rec_date = row[4]
	src = row[5]
	dst = row[6]
	linkedid = row[7]

	if original_file_duration>5:

		files_converted = 0

		if server_object.source_id == server_object.sources['master']:
			server_object.temp_file_path = original_file_path
			server_object.temp_file_name = original_file_name
			if os.path.isfile(temp_file_path + temp_file_name):
				side = 0 if 'in' in original_file_name else 1
				server_object.transcribe_to_sql(side, original_file_name, rec_date, src, dst, linkedid)
			server_object.delete_current_queue(original_file_name, linkedid)

		elif server_object.source_id == server_object.sources['call']:

			if server_object.make_file_splitted(0, original_file_path, original_file_name, linkedid):
				server_object.transcribe_to_sql(0, original_file_name, rec_date, src, dst, linkedid)
				server_object.remove_temporary_file()
				files_converted += 1

			if server_object.make_file_splitted(1, original_file_path, original_file_name, linkedid):
				server_object.transcribe_to_sql(1, original_file_name, rec_date, src, dst, linkedid)
				server_object.remove_temporary_file()
				files_converted += 1

			if files_converted == 0:
				print('file not found:', server_object.linkedid, 'removing from queue..')
				server_object.delete_current_queue(original_file_name, linkedid)

		#else:
		print('files_converted', files_converted)
		#server_object.delete_current_queue(original_file_name, linkedid)
		server_object.delete_source_file(original_file_path, original_file_name, linkedid)
		break

	else:
		print(original_file_name, 'duration', original_file_duration)
		trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

		if server_object.source_id == server_object.sources['master']:
			side = 0 if 'in' in original_file_name else 1
			server_object.save_result('', '0', '0', side, trans_date, 0, original_file_name, rec_date, src, dst, linkedid)
		elif server_object.source_id == server_object.sources['call']:
			server_object.save_result('', '0', '0', 0, trans_date, 0, original_file_name, rec_date, src, dst, linkedid)
			server_object.save_result('', '0', '0', 1, trans_date, 0, original_file_name, rec_date, src, dst, linkedid)

		server_object.delete_current_queue(original_file_name, linkedid)
		server_object.delete_source_file(original_file_path, original_file_name, linkedid)

	processed += 1
	if files_converted > 0:
		break

print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'exit to next job..')
if processed == 0:
	time.sleep(3)
