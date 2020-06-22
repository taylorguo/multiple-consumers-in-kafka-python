
import os, time, datetime, shutil, uuid

from classifier.predict import classifier_init, classifier_predict, get_cn_class
from classifier_DCL.infer import load_models_DCL, DCL_test

from utils import consumer
from utils import oss_upload, get_oss_dict, update_db, oss_download, insert_db
from utils import post_server
from utils import callback_endpoint

#######################################################
############# Pretrained Model Loaded   ###############
#######################################################

model_DCL = load_models_DCL()
print(" ****** DCL Fine Grained Classification Model Loaded  ****** ")

ossmodel = "code-2020/models/xception_weights_tf_dim_ordering_tf_kernels_notop.h5"
tardir = "/root/.keras/models"
target = "/root/.keras/models/xception_weights_tf_dim_ordering_tf_kernels_notop.h5"
if not os.path.exists(tardir) and not os.path.exists(target):
	os.makedirs(tardir)
	print(" ****** Downloading Xception pretrained model ****** ")
	oss_download(target, ossmodel)

class_file = "./classifier/checkpoints/Xception_cpc-redpacket-game-appicon_class_list_20200421T170020.txt"
weight_path = "./classifier/checkpoints/Xception_model_weights_20200421T170020.h5"
model_cls, cls_list = classifier_init(class_file, weight_path)
print(" ****** Xception Classification Model Loaded  ****** ")

mapping_txt = "./classifier/checkpoints/Xception_cpc-redpacket-game-appicon_class_list-mapping.txt"

#######################################################
#############      Model Inference      ###############
#######################################################
def classification_image(modelDCL, clsmodel, clslist, mapping_txt,image_dir, anno_dir, DCL_THRESHOLD = 52.0, CLS_PROB=0.85):

	sub_name, clses, probs = DCL_test(modelDCL, image_dir=image_dir, anno_dir= anno_dir, THRESHOLD=DCL_THRESHOLD)
	# del modelDCL

	if len(clses)==2:
		# clsmodel, clslist = classifier_init(class_file, weight_path)
		cn_cls_name = get_cn_class(mapping_txt)
		im_path = os.path.join(image_dir, sub_name)
		class_name, conf = classifier_predict(clsmodel, clslist, im_path)
		# print(" **** Xception Classifying {} 【{}】 with {}  *** ".format(im_path, cn_cls_name[class_name[0]], conf))
		if conf != None and conf >= CLS_PROB:
			cn_name = cn_cls_name[class_name[0]]
			print(" **** Xception Classified {} 【{}】 with {}  *** ".format(im_path, cn_name, conf))
			return cn_name
		print(" **** DCL Classifying {} 【{}】 with {}  *** ".format(im_path, clses, probs))
		clses = " "
		return clses
	else:
		print(" **** DCL Classifying {} 【{}】 with {}  *** ".format(sub_name, clses, probs))
		return clses

#######################################################
############# 		main pipeline       ###############
#######################################################
# DL housekeeping
def main_backend(image_dir, anno_dir, modelDCL=model_DCL, modelcls=model_cls, clslist=cls_list, mappingtxt=mapping_txt, code=0, message="success"):
	t0 = time.time()
	class_res = classification_image(modelDCL, modelcls, clslist, mappingtxt, image_dir, anno_dir)
	if len(class_res) > 2:
		message = "[Info]: Classification success in {:.3f}s".format(time.time()-t0)
		print(message)
		shutil.rmtree(image_dir)
		shutil.rmtree(anno_dir)
		return class_res
	else:
		code = 320203
		message = "[Info]: Classification Fail in {:.3f}s".format(time.time()-t0)
		print(message)
		shutil.rmtree(image_dir)
		shutil.rmtree(anno_dir)
		return None

#######################################################
###### 	main pipeline with DB & Callback   ############
#######################################################

def main_oss_callback_db(ret_dict, code_msg):
	task = ret_dict["task"]
	url = task["url"]
	callback_url = task["callback"]
	source = task["source"]
	task_uuid = task["task_uuid"]

	# print("*"*15, " main_oss_callback_db, received: ", ret_dict)

	file_local_path = ret_dict["file_local_path"]
	file_my_oss_path = ret_dict["file_oss_path"]
	file_local_path_list = file_local_path.rsplit("/", 1)
	file_local_dir = file_local_path_list[0]

	image_dir = os.path.join(file_local_dir, "image")
	im_path = os.path.join(image_dir, file_local_path_list[-1])
	anno_dir = os.path.join(file_local_dir, "anno")

	if not os.path.exists(image_dir):
		os.makedirs(image_dir)
		oss_download(im_path, file_my_oss_path)
	if not os.path.exists(anno_dir):
		os.makedirs(anno_dir)

	ret_backend = main_backend(image_dir, anno_dir, code=code_msg["code"], message=code_msg["message"])

	if ret_backend != None:

		task.update({"product_catelog": ret_backend})
		task.update(code_msg)

		print("*"*20, " Classifier Result: ", task)

		try:
			if callback_endpoint(callback_url, task):
				update_db(source, task["task_uuid"], task["uuid"], task["url"], 0, 2, code_msg["message"],
						  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
			else:
				code_msg["message"] = "[Error]: no response from callback"
				update_db(source, task["task_uuid"], task["uuid"], task["url"], 1, 2, code_msg["message"],
						  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
		except Exception as e:
			print(e)
		# del task["task_uuid"]
		return True
	else:
		task.update({"product_catelog": ""})
		task.update({"code": 320201, "message": "[Error]: no keyword this image"})

		print("*" * 40, " Classifier Result: ", task)

		try:
			if callback_endpoint(callback_url, task):
				update_db(source, task["task_uuid"], task["uuid"], task["url"], 1, 2, code_msg["message"],
						  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
			else:
				code_msg["message"] = "[Error]: no response from callback"
				update_db(source, task["task_uuid"], task["uuid"], task["url"], 1, 2, code_msg["message"],
						  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
			code_msg.update({"code": 320201, "message": "[Error]: no keyword this image"})
			# update_db(source, task["task_uuid"], task["uuid"], task["url"], 1, 2, code_msg["message"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
		except Exception as e:
			print(e)
		return False


##################################################################
## msg bridge: convert kafak msg-dict to dict used in DL App ##
##################################################################
def convert_io(in_dict):
	ret_dict = {}
	code_msg = {"code": 0, "message": "success"}
	ret_dict.update(code_msg)
	# print("2. {}  --- Input Data: {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), in_dict))
	task_uuid = str(uuid.uuid1())

	ret_post = post_server(in_dict, task_uuid, code_msg, ret_dict)
	if ret_post is None:
		print(ret_dict)
		return None

	task = ret_post["task"]
	insert_db(task["source"], task_uuid, task["uuid"], task["url"], 0, 1, "OK",
			  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

	return ret_post

##########################
## main app using kafka ##
##########################
def main_kafka():
	for msg in consumer:
		t_1 = time.time()
		consumer.commit_async()
		t0 = time.time()
		print("\n****** 1. {:%Y-%m-%d %H:%M:%S} = Consumer submit {:.3f} s & Get consumer data:  ".format(datetime.datetime.now(), t0-t_1),
			  " ###### %s:: %d-%d key=%s value=%s ######" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value))
		msg_value = msg.value
		try:
			# 1. 获取输入消息json格式后, 对消息进行校验和处理, 转换成 DL App 需要的字典格式
			ret_dict = convert_io(msg_value)
			print("****** 2. {:%Y-%m-%d %H:%M:%S} = Convert data to:  {}".format(datetime.datetime.now(), ret_dict) )
			if ret_dict is not None:
				code_msg = {"code":0, "message":"success"}
				# 2. DL App 主处理函数, 如果上述转换失败, 则不进入此处;
				main_oss_callback_db(ret_dict, code_msg)
				print(" ********* 3. {:%Y-%m-%d %H:%M:%S} = Finish process in {:.3f} s".format(datetime.datetime.now(), time.time()-t0), "*"*40)
		except Exception as e:
			print(e)
			consumer.close()

def test():
	t0 = time.time()
	image_source = "inpaint/examples/places/images/place2_08.jpg"
	image_dir = "./tmp_dir/image"
	anno_dir = "./tmp_dir/anno"
	main_backend(image_dir, anno_dir)
	print(" Time Cost:   ", time.time() - t0)

if __name__ == '__main__':
	main_kafka()
	# test()