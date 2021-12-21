"""
@Time ： 2021/7/11 1:32 下午
@Auth ： Sek Hiunam
@File ：METING_demo.py
@Desc ：MetiNg算法
"""
#!/usr/bin/env python
import sys
import pandas as pd
import re
sys.path.append('../')
from log_parsing_algorithm import METING

# input_dir  = '../logs/Dm/'  # The input directory of log file
# output_dir = 'METING_result/'  # The output directory of parsing results
# log_file   = 'psc_WinEventLog_System_12_01-12_31.csv'  # The input log file name
# log_format = '<date> <time> <ap>  <LogName>  <SourceName>  <EventCode>  <EventType>  <Type>  <ComputerName>  <User>  <Sid>  <SidType>  <TaskCategory>  <OpCode>  <RecordNumber>  <Keywords>  Message=<Content>'   # HDFS log format

input_dir  = '../logs/HDFS/'  # The input directory of log file
output_dir = 'METING_result/'  # The output directory of parsing results
log_file   = 'HDFS.log'  # The input log file name
log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # HDFS log format

regex      = {
    'blk_ID':r'blk_(|-)[0-9]+' , # block id -->针对HDFS
    'SystemTime':r'[\w]*.[\w]*.[\S].[\w].[\S].[0-9]*[T].*[Z]',# System Time
    'IP':r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', # IP
    'Number':r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$', # Number
    'Dir':r'^\/(\w+\/?)+$'
}

n = 2 # the length of the n-grams
ht = 0.99# the homogeneity tolerance is a rate, with 0 < ht ≤ 1, that controls how divisive the algorithm is.
#
parser = METING.LogParser(log_format=log_format,indir=input_dir, outdir=output_dir, n_gram=n, ht=ht,rex=regex)
parser.parse(log_file)

# text_1 = r'Served block blk_-5919767990596301121 to /10.251.215.16'
# text_2 = r'Deleting block blk_2278708746675870163 file /mnt/hadoop/dfs/data/current/subdir27/blk_2278708746675870163'
#
# line = re.sub(regex['IP'],'*', text_1)
# if re.findall(regex['IP'],text_1):
#     print(re.sub('[0-9]','*',text_1))
# for word in text_2.split():
#     for varName, currentRex in regex.items():
#         if re.match(currentRex, word):
#             if varName == 'IP':
#                 word = re.sub('[0-9]+', '<*>', word)
#             elif varName == 'Dir':
#                 word = re.sub('\w+', '<*>', word)
#             else:
#                 word = re.sub(currentRex, word, varName)
#     print(word)

#
# BaseManager.register('LogParser', METING.LogParser)
# manager = BaseManager()
# manager.start()
# parser = manager.LogParser(log_format=log_format,indir=input_dir, outdir=output_dir, n_gram=n, ht=ht,rex=regex)

# def build_dendrogram(obj, pid,procs):
#     procsid = uuid.uuid4().hex
#     procs[procsid] = True
#     # 此层循环为树一层节点之间的循环
#     try:
#         print("=" * 10,"{} iteration start calculating...".format(parser.get_iter()),"=" * 10)
#         parseTree = obj.get_parseTree()
#         if parseTree.depth() < 1:
#             # 当树里面没有节点的时候 pid里面装的是所有的lineID【即循环外的G】
#             G = pid
#         else:
#             # 当树里面有节点的时候，waiting node这个列表存储的是这层所有节点的id
#             p_node = obj.get_parseTreeNode(pid)
#             G = p_node.data
#
#         freq = obj.get_freq(G)
#         G_l, G_r, mf_G = obj.group_splitting(freq, G)
#         scoreG = obj.get_homogeneity_score(freq, G)
#         freq_ = obj.get_freq(G_l)
#         scoreG_ = obj.get_homogeneity_score(freq_, G_l)
#         nb_selection = parseTree.depth() + 1
#         u = float((1 / 3) * (1 / obj.get_ht() + 1 / nb_selection + freq[mf_G]))
#
#         if (scoreG < u * scoreG_):
#             obj.set_iter()
#             node_l = Node(tag='G' + str(len(parseTree.all_nodes()) + 1) + '_' + mf_G, data=G_l)
#             node_r = Node(tag='G' + str(len(parseTree.all_nodes()) + 2) + '_' + mf_G, data=G_r)
#             if nb_selection == 1:
#                 # root = Node(tag='Root',identifier='root',data=G)
#                 # parseTree.create_node(tag='Root_' + mf_G, identifier='root', data=G)
#                 node = Node(tag='Root', identifier='root', data=G)
#                 # G1、G2表示分裂后的两个节点
#                 obj.add_parseTreeNode(node, None, True)
#                 obj.add_parseTreeNode(node_l, node, False)
#                 obj.add_parseTreeNode(node_r, node, False)
#                 p_node = obj.get_parseTreeNode('root')
#             else:
#                 obj.add_parseTreeNode(node_l, p_node, False)
#                 obj.add_parseTreeNode(node_r, p_node, False)
#
#             print("=" * 10,'{} iteration most freq G is {}, homogeneity_score is {}, G1 size:{}, G2 size:{}'.format(
#                 obj.get_iter() + 1, mf_G, scoreG, len(G_l), len(G_r)),"=" * 10)
#             obj.add_waitingList(node_l.identifier)
#             obj.add_waitingList(node_r.identifier)
#             # return node_l.identifier, node_r.identifier
#         else:
#             print("*" * 10,"{} iteration scoreG < u*scoreG1 ,stop.".format(obj.get_iter() + 1),"*" * 10)
#     except:
#         print("*" * 10,"{} iteration can't split ,stop.".format(obj.get_iter() + 1),"*" * 10)
#
#     # lock.acquire()
#     # pro_count.value -= 1
#     # lock.release()
#     procs[procsid] = False
#
# if __name__ == '__main__':
#
#     print('Parsing file: ' + os.path.join(parser.get_path(), log_file))
#     start_time = datetime.now()
#     parser.set_logName(log_file)
#     parser.load_data(parser.get_logName())
#
#     count = 0
#     parser.word_sequences = []
#     parser.words_set = []
#     G = []
#     N_G = set()
#     ngram_list = []
#     df_log = parser.get_df()
#     # 得到每行日志的单词sequence wl和日志 Wl
#     for idx, line in df_log.iterrows():
#         # <date> <time> <ap>  <LogName>  <SourceName>  <EventCode>  <EventType>  <Type>  <ComputerName>  <User>  <Sid>  <SidType>  <TaskCategory>  <OpCode>  <RecordNumber>  <Keywords>  Message=<Content>'
#         logID = line['LineId']
#         G.append(logID)
#         # nltk 划分n-gram 词
#         logmessageL = [u' '.join(w) for w in ngrams(parser.preprocess(line['Content']).strip().split(), n)]
#         ngram_list.append(list(set(logmessageL)))
#         # 使用update()方法可以一次性给set添加多个元素
#         N_G.update(logmessageL)
#
#         count += 1
#         if count % 10000 == 0 or count == len(df_log):
#             print('Processed {0:.1f}% of log lines.'.format(count * 100.0 / len(df_log)))
#
#     parser.set_n_gram_list(ngram_list)
#     parser.set_NG(N_G)
#
#
#     waiting_level = [[G]] # 存储待分割的节点
#     manager = multiprocessing.Manager()
#     procs = manager.dict()
#
#     pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
#     lock = manager.Lock()
#     # pro_count = manager.Value('i',0)
#     sema = Semaphore(multiprocessing.cpu_count()) # 通过信号量统计工作进程
#     parser.add_waitingList(G)
#     pids = []
#
#     while len(parser.get_waitingList())!=0 or (len(pids) != 0):
#         jobs = []
#         for n in parser.get_waitingList():
#             parser.remove_waitingList(n)
#             # pro_count.value += 1
#             pool.apply_async(func=build_dendrogram,args=(parser,n,procs,))
#         time.sleep(1)
#         pids = [pid for pid, running in procs.items() if running]
#
#     pool.close()
#     pool.join()
#
#
#     if not os.path.exists(parser.get_path()):
#         os.makedirs(parser.get_path())
#
#     parser.outputResult()
#     print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - start_time))
