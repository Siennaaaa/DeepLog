"""
@Time ： 2021/12/17 1:40 PM
@Auth ： Sek Hiunam
@File ：log_template_mappping.py
@Desc ：mapping template id to block id
"""
import pickle
import re
import pandas as pd
import numpy as np
from sklearn.utils import shuffle
import os
import random

block_groups_indir = '../data_preprocessing/'
normal_logs_file = 'hdfs_normal.csv'
abnormal_logs_file = 'hdfs_abnormal.csv'
normal_mapping_logs_file = 'hdfs_mapping_normal.csv'
abnormal_mapping_logs_file = 'hdfs_mapping_abnormal.csv'
structure_logs_file = 'HDFS_structured.csv'
output_dir = '../parsing/METING_result/'

def block_lineId_mapping_eventId():
    normal_log_groups = pd.read_csv(os.path.join(block_groups_indir,normal_logs_file))
    abnormal_log_groups = pd.read_csv(os.path.join(block_groups_indir,abnormal_logs_file))
    parsing_result  = pd.read_csv(os.path.join(output_dir,structure_logs_file),encoding='utf-8',error_bad_lines=False)

    normal_block_groups = {} #block_groups['block_id'] = [ 1,2,3...] # a list of template id
    block_count = 0
    for idx,line in normal_log_groups.iterrows():
        blk_id = line['BlockId']
        log_ids = list(map(int, line['Logs'].split()))
        templates_ids = []
        for lid in log_ids:
            t_id = parsing_result[parsing_result['LineId'] == lid]['EventId'].values[0]
            templates_ids.append(t_id)
        normal_block_groups[blk_id] = templates_ids
        # if blk_id in block_groups.keys():
        #     block_groups[blk_id].append(linecount)
        # else:
        #     block_groups[blk_id] = []
        block_count +=1
        if block_count % 1000 == 0 or block_count == len(normal_log_groups):
            print('Normal Processed {0:.1f}% of log lines.'.format(block_count * 100.0 / len(normal_log_groups)))

    try:
        with open('normal_block_templates.pickle','wb') as file:
            pickle.dump(normal_block_groups,file)
    except:
        print('error save!')

    abnormal_block_groups = {} #block_groups['block_id'] = [ 1,2,3...] # a list of template id
    block_count = 0
    for idx,line in abnormal_log_groups.iterrows():
        blk_id = line['BlockId']
        log_ids = list(map(int, line['Logs'].split()))
        templates_ids = []
        for lid in log_ids:
            try:
                t_id = parsing_result[parsing_result['LineId'] == lid]['EventId'].values[0]
                templates_ids.append(t_id)
                abnormal_block_groups[blk_id] = templates_ids
            except:
                print('can not find this log!')
        block_count += 1
        if block_count % 1000 == 0 or block_count == len(abnormal_log_groups):
            print('Abnormal Processed {0:.1f}% of log lines.'.format(block_count * 100.0 / len(abnormal_log_groups)))

    try:
        with open('abnormal_block_templates.pickle','wb') as file:
            pickle.dump(abnormal_block_groups,file)
    except:
        print('error save!')

def load_block_eventId():
    try:
        with open('abnormal_block_templates.pickle','rb') as file:
            abnormal_block_templates = pickle.load(file)
    except:
        print('error load!')
    try:
        with open('normal_block_templates.pickle','rb') as file:
            normal_block_templates = pickle.load(file)
    except:
        print('error load!')

    # for k,v in abnormal_block_templates.items():
    #     print("Block : {} has Logs  {}  ".format(k,v))
    return abnormal_block_templates,normal_block_templates


# union templates according to file HDFS_templates.csv obtained from METING parsing
templates_mapping = {
1:['84a50f10'],
2:['9012fa98'],
3:['94c808ec'],
4:['f5a5ba2f'],
5:['674a8535'],
6:['0c9cccc5'],
7:['a7d4e3bf' ,'7ff817a8', '52cc6bc8'],
8:['028029dd', 'c640b20e', 'b09acecd' ,'4ab40a26', 'da9af8b1', 'ba9962e2',
 '80efcf9f', 'a85e26b9', '603669d3', 'c932713f', '2f8efc51', '888015f2',
 '9cc947cf', '75a0fb8c', 'dbeb0ae2', '61c66490', '7d4d1ae9', '0e111c78',
 '95dbb84e', '3162441a', 'bd0ad0e1', '01ab34db', 'e05cce43', '7627c676',
 'f5475b6f', 'dc10f7f2', 'bd390c70', 'e53633cf', '2e45e806', 'c6ec9679',
 '7bd7467e', '388212d2', '25af8e75', 'db6212db', 'f82d8b1a', '67fbc8b1',
 'bf61ae04', '872ccaad', 'c3c42c0e', 'b01437d3', '4694b2f2', '223352fe',
 'cc16f16c', 'e514f530', '7a21596c', '9cd7156b', 'c5fc98af', '2c6accdf',
 'b7f5582b', '98feea84', '7a15f117', 'da6c48fd', '5a356ad1', '012a96cf',
 '2f8e9961', '49b11e89', '5a904f53', '1bf429d2', '1752a755', '345fc393'],
9:['d43ca034'],
10:['85c1504d'],
11:['b0114d94'],
12:['9114cabe','62a2a9a0','7aac6f8c','5145918d','fb654107','f62170c5',
 '333c3240', '58ef4b8a', 'ec286ba7', '5876a27b', 'a737d7d2','3bb2c014',
 '23c85956', '78af5209', 'ff484cb6', '84da5851', '4ce9d850' ,'bf93af40',
 '5c80dcab', '12069c52', '88d10b63', '53c22dde', '57bf56e3', 'b5b52f00',
 'cefe1fb7', 'bc0a8bc3', '23b2456c', 'e366adb4', 'ea076478', 'bac5892a',
 '8bb4d10e', '29c6a3c9', '7f766356', '22b30012', '38fdb5e4', 'b9bc5e9a',
 '0c8ff6ea', '57c52218', '53d2ca0f', 'c692befd', 'a8fabe2f', '91089e30',
 '10f3ec9b', 'dd414d8f', 'f1715f9d', '812d0dad', '339f8943', '06fbb701',
 'cd9742b6', '43133038', '51954248', 'a7f3ea10', 'fd88060c', 'fba34a70',
 'f4307eca', 'fccb8108', 'a979f739', '301a42ff', '7cee1b4d', '2fcfa9c8',
 '1e897059', 'da5136fd', '9ec332d9', '5f45068c', 'a449612a'],
13:['763719a8', '02a73e98', '34a73f28', 'ccdd2dff'],
14:['9b5ee23e'],
15:['bc134034'],
16:['d77a5d65'],
17:['f8f3b456'],
18:['5b704ea5'],
19:['a2a7e398'],
20:['2dcdbf9b'],
21:['50036b78'],
22:['1740be00'],
23:['68cf4170', '771a1132', '6bb73b17', '446d2a5a', 'a57a3c5d', 'a1794cc3'],
24:['000ac1dd'],
25:['766d1a73'],
26:['10f52ae7'],
27:['1897f420'],
28:['982b21dc'],
29:['9eeaf9ca'],
30:['a44faf34'],
31:['2d93dd64'],
32:['13561229'],
33:['9132659b'],
34:['c646fb83'],
35:['ff3e65aa'],
36:['6acc1945', '0fef50bb', '49e4ffba', '6c76e282' ,'0eb2908b', 'cf7de4f4',
 '6c847854'],
37:['08967ffe' ,'e6b2fcf7', '8c37a78a', '67e5924e'],
38:['8b924b0b']
}

def block_templateId_mapping_templateNum():
    abnormal_block_templates, normal_block_templates = load_block_eventId()
    normal_blk = []
    normal_logs = []
    for blk, t_id in normal_block_templates.items():
        lstm_input = ' '.join(t_id)
        for templates_mapping_id, templates_id in templates_mapping.items():
            sub_templates_id = list(set(t_id) & set(templates_id))
            for parsing_t_id in sub_templates_id:
                lstm_input = re.sub(parsing_t_id, str(templates_mapping_id), lstm_input)
        normal_blk.append(blk)
        normal_logs.append(lstm_input)

    normal_df = pd.DataFrame()
    normal_df['BlockId'] = normal_blk
    normal_df['Logs'] = normal_logs

    try:
        normal_df.to_csv('hdfs_mapping_normal.csv', index=False, columns=['BlockId', 'Logs'])
        print('normal logs transform finished!')
    except:
        print('error save!')

    abnormal_blk = []
    abnormal_logs = []
    for blk, t_id in abnormal_block_templates.items():
        lstm_input = ' '.join(t_id)
        for templates_mapping_id, templates_id in templates_mapping.items():
            sub_templates_id = list(set(t_id) & set(templates_id))
            for parsing_t_id in sub_templates_id:
                lstm_input = re.sub(parsing_t_id, str(templates_mapping_id), lstm_input)
            if '4882099' in t_id:
                lstm_input = re.sub('4882099', '29', lstm_input)
        abnormal_blk.append(blk)
        abnormal_logs.append(lstm_input)

    abnormal_df = pd.DataFrame()
    abnormal_df['BlockId'] = abnormal_blk
    abnormal_df['Logs'] = abnormal_logs

    try:
        abnormal_df.to_csv('hdfs_mapping_abnormal.csv', index=False, columns=['BlockId', 'Logs'])
        print('abnormal logs transform finished!')
    except:
        print('error save!')

def data_format():
    '''
    get hdfs train/test file
    :return:
    '''
    abnormal_df = pd.read_csv('hdfs_mapping_abnormal.csv')
    normal_df = pd.read_csv('hdfs_mapping_normal.csv')
    normal_df = shuffle(normal_df)
    # for i in range(normal_df.shape[0]):
    #     print(len(normal_df.iloc[i]['Logs'].split()))

    with open("../data/hdfs_test_abnormal_meting.txt", "w", encoding="UTF-8") as f:
        for idx,line in abnormal_df.iterrows():
            f.write(line['Logs']+'\n')

    normal_train_size = 2*int(normal_df.shape[0]/3)

    with open("../data/hdfs_train_meting.txt", "w", encoding="UTF-8") as f:
        for idx in range(0,normal_train_size):
            f.write(normal_df.iloc[idx]['Logs']+'\n')

    with open("../data/hdfs_test_normal_meting.txt", "w", encoding="UTF-8") as f:
        for idx in range(normal_train_size,normal_df.shape[0]):
            f.write(normal_df.iloc[idx]['Logs']+'\n')

data_format()