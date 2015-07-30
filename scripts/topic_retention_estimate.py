import os
import re
import collections
import csv
from collections import defaultdict
from datetime import datetime

topics_ds = defaultdict(list)
topic_names = list()

filename = "kafka08_a-datasize.txt"
kafka_log_path = "/var/kafka-spool/"
prev_line = " "

def get_topic_name(line):
         
        med_steps = line.split("/")
        topic_name =  med_steps[3]  
        topic_name = topic_name[0:len(topic_name)-1]
        m = re.search("[.]*-[0-9]+$",topic_name)   
        return topic_name[0:(len(topic_name) - len(m.group(0)))]

def get_partition_no(line):

        med_steps = line.split("/")
        topic_name =  med_steps[3]
        topic_name = topic_name[0:len(topic_name)-1]
        m = re.search("[.]*-[0-9]+$",topic_name)
        med_str = m.group(0)      
        num = int(med_str[1:len(med_str)])
        return num    
 
fptr = open('./' + filename,'r')
for line in fptr:
        line = line.rstrip()
        if not line.strip():
                    continue
        if "recovery-point-offset-checkpoint" in line:
                    continue
        if "replication-offset-checkpoint" in line:
                    continue
        if kafka_log_path in line:
                    topic_name = get_topic_name(line)
                    if ("index" in topic_name):
                                  continue
                    partition_no = get_partition_no(line)
                    if topic_name not in topic_names:
                                  topic_names.append(topic_name)
                                  topics_ds[topic_name] = dict()

                                  topics_ds[topic_name][partition_no] = dict()
                                  topics_ds[topic_name][partition_no]["sizes"] = list()
                                  topics_ds[topic_name][partition_no]["times_list"] = list()
                                  topics_ds[topic_name][partition_no]["total_size"] = 0
                                  topics_ds[topic_name][partition_no]["retention_hrs"] = 0
                                  topics_ds[topic_name][partition_no]["rentention_per_hr"] = 0.0
                                  prev_line = "path"
                                  continue
                    else:
                                  if partition_no not in topics_ds[topic_name]:
                                         topics_ds[topic_name][partition_no] = dict()
                                         topics_ds[topic_name][partition_no]["sizes"] = list()
                                         topics_ds[topic_name][partition_no]["times_list"] = list()
                                         topics_ds[topic_name][partition_no]["total_size"] = 0                                        
                                         topics_ds[topic_name][partition_no]["retention_hrs"] = 0
                                         topics_ds[topic_name][partition_no]["rentention_per_hr"] = 0.0
                                  prev_line = "path"
                                  continue
 
        if prev_line == "path":
                    prev_line = "total"
                    times_list = list()
                    size_list  = list()
                    continue

        if prev_line == "total":   
                    dir_line = line.split()
                    file_size = int(dir_line[4])  #file size of a file
                    topics_ds[topic_name][partition_no]["sizes"].append(file_size)                    
                    time_stamp = dir_line[5]+" "+dir_line[6]+" "+dir_line[7]  #time stamp of a file
                    topics_ds[topic_name][partition_no]["times_list"].append(time_stamp)


size_zero_topics = 0
less_than_hour_topics = 0
less_than_two_files_topics = 0

one_topic_size = 0

fptrc_csv = open("./analysis.csv","wt")
writer = csv.writer(fptrc_csv)
writer.writerow( ('topic','partition','total_size','retention_hrs','retention_per_hr') )
#add the sizes of all the partitions
total_size_for_seven_days = 0
for k1,v1 in topics_ds.iteritems():
             for k2,v2 in topics_ds[k1].iteritems():
                            sum = 0
                            for num_inst in  topics_ds[k1][k2]["sizes"]: 
                                          sum = sum + num_inst
                            topics_ds[k1][k2]["total_size"] = sum

                            if topics_ds[k1][k2]["total_size"] == 0:
                                   size_zero_topics += 1
                            
                            if len(topics_ds[k1][k2]["times_list"]) < 2:
                                   less_than_two_files_topics += 1
                                   one_topic_size += topics_ds[k1][k2]["total_size"]

                            if len(topics_ds[k1][k2]["times_list"]) >= 2:
                                   topics_ds[k1][k2]["times_list"] = sorted(topics_ds[k1][k2]["times_list"]) 
                                   ts1 = topics_ds[k1][k2]["times_list"][0]
                                   ts2 = topics_ds[k1][k2]["times_list"][len(topics_ds[k1][k2]["times_list"])-1]                                   
                                   t1 = datetime.strptime(ts1, "%b %d %H:%M")
                                   t2 = datetime.strptime(ts2, "%b %d %H:%M") 
                                   diff = t2 - t1
                                   topics_ds[k1][k2]["retention_hrs"] = (diff.total_seconds())/3600
                                   if topics_ds[k1][k2]["retention_hrs"] == 0:
                                                   less_than_hour_topics += 1
                                                   topics_ds[k1][k2]["rentention_per_hr"] = topics_ds[k1][k2]["total_size"]/1
                        
                                   if topics_ds[k1][k2]["retention_hrs"]>0:
                                      topics_ds[k1][k2]["rentention_per_hr"] = topics_ds[k1][k2]["total_size"]/topics_ds[k1][k2]["retention_hrs"]
                                      writer.writerow( (k1,k2,topics_ds[k1][k2]["total_size"],topics_ds[k1][k2]["retention_hrs"], topics_ds[k1][k2]["rentention_per_hr"]) )

                            total_size_for_seven_days += topics_ds[k1][k2]["rentention_per_hr"]*168

print total_size_for_seven_days
print "size zero topics:",size_zero_topics
print "less_than_hour_topics:", less_than_hour_topics
print "less_than_two_files_topics:", less_than_two_files_topics
print "one_file_per_topic_aggregate:", one_topic_size
