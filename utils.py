import os, json
import requests
import oss2

from PIL import Image
from io import BytesIO

from kafka import KafkaProducer, KafkaConsumer

import pymysql, pymysql.cursors
# ########### Prometheus NOT Used ###########################
# from prometheus_client import multiprocess
# from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST, Gauge, Counter, Histogram
# ###########################################################

##################################################
############# Kafka & DB Setting ENV #############
##################################################
# set mySQL ENV in dockerfile
SQL_IP = os.environ.get("SQL_IP")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PWD")
db_name = os.environ.get("DB_NAME")

# set Kafka ENV in dockerfile
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
POST_SERVER_BACKEND_TOPIC = os.environ.get("POST_SERVER_BACKEND_TOPIC")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP")

# set OSS ENV in dockerfile
default_key_id = os.environ.get("key_id")
default_key_secret = os.environ.get("key_secret")
default_bn = os.environ.get("bn")
default_ep = os.environ.get("ep")
##################################################
############### Kafka ############################
##################################################
producer = KafkaProducer(
		bootstrap_servers=[KAFKA_BROKER_URL],
		value_serializer=lambda value: json.dumps(value).encode(),
		)

consumer = KafkaConsumer(
		# POST_SERVER_BACKEND_TOPIC,
		bootstrap_servers=[KAFKA_BROKER_URL],
		group_id = KAFKA_GROUP,
		# max_poll_records = 50,
		max_poll_interval_ms = 500000,
		# auto_offset_reset = "latest",
		enable_auto_commit = False,
		# auto_commit_interval_ms = 5000,
		value_deserializer=lambda value: json.loads(value),
		)

consumer.subscribe([POST_SERVER_BACKEND_TOPIC])

###########################################
######### Request Data Handle  ###########
###########################################
def post_json_parser(request_data, code_msg):
	predefine_key = ["uuid", "url", "callback", "source", "priority", "dpappkey"]
	must_key = ["uuid", "url", "callback", "source"]
	if request_data.keys() is not None:
		key_list = request_data.keys()
		for i in key_list:
			if not(i in predefine_key):
				code_msg["code"] = 301101
				code_msg["message"] = "Wrong field {} in JSON".format(i)
				return False
		for k in must_key:
			if not(k in key_list):
				code_msg["code"] = 301101
				code_msg["message"] = "Lack of {} field".format(k)
				return False
	else:
		return False
	return True

def download_file(url, file_dir, filename, code_msg):
	r = requests.get(url)
	if not os.path.exists(file_dir):
		os.makedirs(file_dir)
	try:
		with open(os.path.join(file_dir, filename), "wb") as f:
			f.write(r.content)
	except:
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: file is not availble for download"
		return False
	return True

def get_name_from_url(url, code_msg:dict, EXT=".mp4"):
	if url.find("/"):
		item = url.rsplit("/", 1)[1]
		mp4_prefix = item.split(".")[0]
		return mp4_prefix+EXT
	else:
		code_msg["code"] = 301103
		code_msg["message"] = "[Error]: can't get filename"
		return None
###########################################
############# Download Image###############
###########################################
def download_image(url, file_dir, file_name, code_msg: dict):
	if not os.path.exists(file_dir):
		os.makedirs(file_dir)
	try:
		r = requests.get(url)
		image_data = r.content
		# with open(os.path.join(file_dir, filename), "wb") as f:
		# 	f.write(r.content)
	except:
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: Could not download image %s from %s" % (file_name, url)
		return False
	try:
		pil_image = Image.open(BytesIO(image_data))
	except:
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: Failed to parse image"
		return False
	try:
		pil_rgb = pil_image.convert("RGB")
	except:
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: Failed to convert image to RGB"
		return False
	try:
		pil_rgb = pil_rgb.save(os.path.join(file_dir, file_name), format="JPEG", quality=90)
	except:
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: Failed to save image"
		return False
	return True

def post_server(request_data, task_uuid, code_msg, ret_dict, task_name="image_classifier", ROOTDIR="/data", OSS_DOWN = 0):
	code_msg.update({"code":0, "message":"success"})
	url, source, handled_url = "", "", ""
	##########
	# 1. parse json data
	if post_json_parser(request_data, code_msg):
		url = request_data["url"]
		source = request_data["source"]
		ret_dict.update({"uuid": request_data["uuid"]})
		if OSS_DOWN:  handled_url = request_data["handled_url"]
	else:
		code_msg["code"] = 301105
		code_msg["message"] = "[Error]: wrong request json data"
		print(code_msg)
		return None
	##########
	# 2. get file name from url and local file path
	# task_uuid = str(uuid.uuid1())
	request_data["task_uuid"] = task_uuid
	file_dir = os.path.join(ROOTDIR, task_name, source, task_uuid, "source", "image")
	if not os.path.exists(file_dir): os.makedirs(file_dir)

	EXT = url.rsplit(".", 1)[-1]
	file_name = None
	if EXT in ["jpg", "jpeg", "png"]:
		file_name = get_name_from_url(url, code_msg, EXT="."+EXT)
	else:
		code_msg.update({"code": 301101, "message": "[Error]: Wrong type image. [.jpg, .jpeg, .png]"})
		print(code_msg)
		return None

	if file_name is None:
		code_msg.update({"code": 301101, "message": "[Error]: can't get input file name"})
		print(code_msg)
		return None
	##########
	# 3. download file and backup to my OSS
	# if not os.path.exists(file_dir): os.makedirs(file_dir)
	local_file = os.path.join(file_dir, file_name)
	my_oss_path, inpainted_oss_path = "", ""
	try:
		if OSS_DOWN:
			if source=="default":
				oss_download(local_file, url)
				my_oss_path = "server_data_backup/image_delogo_2020/"+source+"/"+url
				# inpainted_oss_path = "server_data_backup/image_delogo_2020/"+source+"/inpainted/"+handled_url
			else:
				oss_dict = get_oss_dict(source)
				if oss_dict is not None:
					oss_download(local_file, url, key_id=oss_dict["accessKey"], key_secret=oss_dict["accessSecret"], bn=oss_dict["bucket"], ep=oss_dict["endpoint"])
					my_oss_path = "server_data_backup/image_delogo_2020/"+source+"/"+url
					# inpainted_oss_path = "server_data_backup/image_delogo_2020/"+source+"/inpainted/"+handled_url
					oss_upload(local_file, my_oss_path)
				else:
					code_msg.update({"code": 301103, "message": "[Error]: No OSS info, register firstly"})
					return None
		else:
			if source != "default":
				oss_dict = get_oss_dict(source)
				if oss_dict == None:
					code_msg.update({"code": 301102, "message": "[Error]: contact admin to assign source"})
					print(code_msg)
					return None
			down_res = download_image(url, file_dir, file_name, code_msg)
			if down_res:
				my_oss_path = "server_data_backup/%s_2020/%s/%s"%(task_name, source, file_name)
				oss_upload(local_file, my_oss_path)
			else:
				code_msg.update({"code": 301102, "message": "[Error]: could not download file"})
				print(code_msg)
				return None
	except Exception as e:
		print(e)
		code_msg["code"] = 301102
		code_msg["message"] = "[Error]: could not download file"
		print(code_msg)
		return None
	##########
	# 4. return dict with local info
	request_data.update(code_msg)
	source_dict: dict = {"task": request_data, "task_uuid": task_uuid, "file_local_path": local_file, "file_oss_path": my_oss_path}
	print(" **** Data Processed:   ", source_dict, " ****")
	if os.path.exists(local_file): os.remove(local_file)
	return source_dict

###########################################
############# OSS Backup   ###############
###########################################
def oss_upload(local_file_path, oss_file_path, key_id = default_key_id, key_secret=default_key_secret, bn=default_bn, ep=default_ep):
	access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', key_id)
	access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', key_secret)
	bucket_name = os.getenv('OSS_TEST_BUCKET', bn)
	endpoint = os.getenv('OSS_TEST_ENDPOINT', ep)
	#
	for param in (access_key_id, access_key_secret, bucket_name, endpoint):
		assert '<' not in param, 'Please set parameters: ' + param
	#
	bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
	result = bucket.put_object_from_file(oss_file_path, local_file_path)
	print("   Upload {} to {} successfully.".format(local_file_path, oss_file_path))

def oss_download(local_file_path, oss_file_path, key_id = default_key_id, key_secret=default_key_secret, bn=default_bn, ep=default_ep):
	#
	auth = oss2.Auth(key_id, key_secret)
	bucket = oss2.Bucket(auth, ep, bn)
	bucket.get_object_to_file(oss_file_path, local_file_path)
	print("   Download {} to {} successfully.".format(oss_file_path, local_file_path))
##################################################

def callback_endpoint(callback_url, ret_dict):
	r = requests.post(callback_url, json=ret_dict)
	if r.status_code == 200:
		print(" *** callback OK")
		return True
	else:
		print(" *** callback Fail")
		return False

def produce_message(producer,t, d):
	# producer.send(POST_SERVER_DECODER_TOPIC, value=video_source_dict)
	producer.send(t, value=d)

def consume_message(c, callback_url):
	for message in c:
		video_delogo: dict = message.value
	
		ret_async = {"code": video_delogo["task"]["code"],
					 "message": video_delogo["task"]["message"],
					 "url": video_delogo["task"]["url"],
					 "uuid": video_delogo["task"]["uuid"],
					 "handler": video_delogo["task"]["source"]}
		# if callback_endpoint(callback_url, ret_async): return ret_async
		# else: return None

####################################################
############# Database MySQL and OSS ###############
####################################################
def insert_logo_keyword(ip, user, pwd, dbname, source, keyword):
	connection = pymysql.connect(host=ip, user=user, password=pwd, db=dbname, charset='utf8mb4',
								 cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			sql = "INSERT INTO keywords_imdelogo (source, keyword) VALUES (%s, %s)"
			cursor.execute(sql, (source, keyword))
		connection.commit()
	except Exception as e:
		return False
	finally:
		connection.close()
	return True

def get_keywords(ip, user, pwd, dbname, source):
	result = None
	connection = pymysql.connect(host=ip, user=user, password=pwd, db=dbname, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			sql = "SELECT keyword FROM keywords_imdelogo WHERE source=%s"
			cursor.execute(sql, (source,))
			result = cursor.fetchall()
			print(result)
	except Exception as e:
		print(e)
		return None
	finally:
		connection.close()
	return result

def insert_oss_record(ip, user, pwd, dbname, source, ep, bk,ak, sk, ut):
	connection = pymysql.connect(host=ip, user=user, password=pwd, db=dbname, charset='utf8mb4',
								 cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			sql = "INSERT INTO oss_user (source, password, endpoint, bucket, access_key, access_secret, update_time) VALUES (%s, %s,%s,%s,%s,%s,%s)"
			cursor.execute(sql, (source, "xxxx", ep, bk, ak, sk, ut))
		connection.commit()
	except Exception as e:
		return False
	finally:
		connection.close()
	return True

def get_oss_dict(source, ip=SQL_IP, user=db_user, pwd=db_pwd, dbname=db_name):
	result = None
	connection = pymysql.connect(host=ip, user=user, password=pwd, db=dbname, charset='utf8mb4',
	                             cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			sql = "SELECT endpoint, bucket, access_key, access_secret FROM oss_user WHERE source=%s"
			cursor.execute(sql, (source,))
			result = cursor.fetchone()
			print(result)
	except Exception as e:
		print(e)
		return None
	finally:
		connection.close()
	return result

def insert_db(source, task_uuid, userinfo_uuid, url, status, stage, message, update_time, ip=SQL_IP, user=db_user,
              passwd=db_pwd, database=db_name):
	db = pymysql.connect(ip, user, passwd, database)
	cursor = db.cursor()
	sql = "INSERT INTO tasks_imclassifier(source, \
	task_uuid, userinfo_uuid, url, status, stage, message, update_time) \
	VALUES ('%s', '%s',  '%s', '%s', %d,  %d , '%s', '%s' )" % (
	source, task_uuid, userinfo_uuid, url, status, stage, message, update_time)
	
	try:
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
	except Exception as e:
		print(e)
		# 如果发生错误则回滚
		db.rollback()
		return False
	return True

def update_db(source,task_uuid,userinfo_uuid,url,status,stage,message,update_time, ip=SQL_IP,user=db_user,passwd=db_pwd,database=db_name):
	db = pymysql.connect(ip,user,passwd,database)
	cursor = db.cursor()
	sql = "UPDATE tasks_imclassifier SET source = '%s' , url = '%s', userinfo_uuid = '%s' ,status = %d ,stage = %d ,message = '%s' , update_time ='%s' WHERE task_uuid = '%s' " % (
	source, url, userinfo_uuid, status, stage, message, update_time, task_uuid)

	try:
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
	except Exception as e:
		print(e)
		# 如果发生错误则回滚
		db.rollback()
		return False
	return True
