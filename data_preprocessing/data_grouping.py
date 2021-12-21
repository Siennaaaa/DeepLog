"""
@Time ： 2021/12/16 1:43 PM
@Auth ： Sek Hiunam
@File ：data_grouping.py
@Desc ：group the logs by block_id
"""
import os
import pickle
import re
import pandas as pd

input_dir  = '../logs/HDFS/'  # The input directory of log file
log_file   = 'HDFS.log'  # The input log file name
label_file = 'anomaly_label.csv'
log_path = os.path.join(input_dir, log_file)
label_path = os.path.join(input_dir, label_file)


blockID_rex = r'blk_(|-)[0-9]+'
log_messages = []
linecount = 0

# Group logs by block id
# block_groups = {} #block_groups['block_id'] = [ 1,2,3...] # a list of lineID
# with open(log_path, 'r', encoding='UTF-8') as fin:
#     for line in fin.readlines():
#         try:
#             blk_id = re.search(blockID_rex, line.strip()).group(0)
#             print(blk_id)
#             # message = [match.group(header) for header in headers]
#             # log_messages.append(message)
#             linecount += 1
#             if blk_id in block_groups.keys():
#                 block_groups[blk_id].append(linecount)
#             else:
#                 block_groups[blk_id] = []
#         except Exception as e:
#             pass
#
# try:
#     with open('block_groups.pickle','wb') as file:
#         pickle.dump(block_groups,file)
# except:
#     print('error save!')

# Group block by normal and anomaly
label_df = pd.read_csv(label_path)

with open('block_groups.pickle','rb') as file:
    load_block_groups = pickle.load(file)

normal_logs = []
normal_blk = []
anomlay_logs = []
anomlay_blk = []
for idx,line in label_df.iterrows():
    blk_id = line['BlockId']
    label = line['Label']
    logs = load_block_groups[blk_id]
    logs = map(lambda x:str(x),logs)
    if label == 'Normal':
        normal_blk.append(blk_id)
        normal_logs.append(' '.join(logs))
    else:
        anomlay_blk.append(blk_id)
        anomlay_logs.append(' '.join(logs))

normal_df = pd.DataFrame()
normal_df['BlockId'] = normal_blk
normal_df['Logs'] = normal_logs

anomlay_df = pd.DataFrame()
anomlay_df['BlockId'] = anomlay_blk
anomlay_df['Logs'] = anomlay_logs

normal_df.to_csv('hdfs_normal.csv', index=False, columns=['BlockId', 'Logs'])
anomlay_df.to_csv('hdfs_abnormal.csv', index=False, columns=['BlockId', 'Logs'])


