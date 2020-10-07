import sys
import os
import time
import datetime
from stt_miner import get_file_splitted
from stt_miner import transcribe_to_sql
from init_server import server_settings
from init_server import connect_sql
from init_server import delete_queue
import os.path

def remove_temporary_files(temp_storage_path,splitted_file_name):
	os.remove(temp_storage_path+splitted_file_name+'_l.wav')
	os.remove(temp_storage_path+splitted_file_name+'_r.wav')

cpu_id = sys.argv[1]
settings = server_settings()
conn = connect_sql(settings)
cursor = conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d from queue where cpu_id='"+cpu_id+"' order by date;"

while True:
	cursor.execute(sql_query)
	for row in cursor.fetchall():
		try:
			filepath			= row[0]
			filename_original	= row[1]
			date_y				= row[2]
			date_m				= row[3]
			date_d				= row[4]
			# split channels
			splitted_file_name = get_file_splitted(filepath+filename_original, settings.script_path)
			temp_storage_path = settings.script_path+'files/'
			if os.path.isfile(temp_storage_path+splitted_file_name+'_l.wav') and os.path.isfile(temp_storage_path+splitted_file_name+'_r.wav'):
				# transcribe
				transcribe_to_sql(temp_storage_path, splitted_file_name+'_l.wav', conn, settings, 0, date_y, date_m, date_d, filename_original)	
				transcribe_to_sql(temp_storage_path, splitted_file_name+'_r.wav', conn, settings, 1, date_y, date_m, date_d, filename_original)
				# delete from queue
				delete_queue(conn,filename_original)
				# remove temporary splitted audio files
				remove_temporary_files(temp_storage_path,splitted_file_name)
				print('+++',filename_original)
			else:
				print('---',filename_original)
		except Exception as e:
			print("### error:",str(e),'file:',filename_original)
	print(cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
	time.sleep(10)