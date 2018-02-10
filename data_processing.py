import os
import json
import pandas as pd
import numpy as np
from collections import defaultdict, Counter

# create dataframe for call, sms, contacts
def generate_df(file_paths, d_type):
    # filter file_paths by type
    print 'generating {} dataframe ...'.format(d_type)
    file_paths = [path for path in file_paths if '{}.txt'.format(d_type) in path]
    user_info = []
    
    for file_path in file_paths:
        f_dir, user_id, dev_id, f_name = file_path.split('/')
        user_id = user_id.split('-')[1]
        dev_id = dev_id.split('-')[1]

        raw_data = open(file_path)
        phone_log = json.loads(raw_data.read())
        phone_log = pd.DataFrame(phone_log)
        
        if 'user_id' not in phone_log.columns:
            phone_log.insert(0, 'user_id', user_id)
        if 'dev_id' not in phone_log.columns:
            phone_log.insert(1, 'dev_id', dev_id)

        user_info.append(phone_log)
    
    final_df = pd.concat(user_info)
    return final_df.reset_index(drop=True)

if __name__=="__main__":
	status_df = pd.read_csv('user_logs/user_status.csv')

	# assemble file paths
	file_paths = []
	device_count = Counter()
	for user_id in os.listdir('user_logs'):
	    if 'user-' not in user_id:
	        continue

	    for device in os.listdir('user_logs/'+user_id):  
	        if 'device-' not in device:
	            continue
	        id_num = user_id.split('-')[1]
	        device_count[id_num] += 1
	        path = '/'.join(['user_logs', user_id, device])
	        f_names = os.listdir(path)
	        file_paths += ['/'.join([path,f_name]) for f_name in f_names]

	# generate and store datasets
	call_df = generate_df(file_paths, d_type='call_log')
	call_df['device_count'] = [device_count[u_id] for u_id in call_df['user_id']]
	call_df.to_csv('user_calls.csv', encoding='utf-8', index=False)

	sms_df = generate_df(file_paths, d_type='sms_log')
	sms_df['message_body'] = sms_df['message_body'].replace(np.nan, '', regex=True)
	sms_df['message_len'] = [len(s) for s in sms_df['message_body']]
	sms_df.to_csv('user_sms.csv', encoding='utf-8', index=False)

	contact_df = generate_df(file_paths, d_type='contact_list')
	contact_df.to_csv('user_contacts.csv', encoding='utf-8', index=False)


