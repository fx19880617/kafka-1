from commands import *
import argparse

parser = argparse.ArgumentParser(description='Where is my topic !', add_help=True)
parser.add_argument('--topic', help='topic of your interest', type=str)
args = parser.parse_args()
topic = args.topic

if (topic is None):
    parser.print_help()
    exit()

status, text = getstatusoutput("ssh kloakzk01-sjc1 'zkcli ls /kloak-sjc1a/brokers/topics'")
if topic in text:
   print "topic is in kloak-sjc1a"

status, text = getstatusoutput("ssh kloakzk01-sjc1 'zkcli ls /kloak-sjc1b/brokers/topics'")
if topic in text:
   print "topic is in kloak-sjc1b"

status, text = getstatusoutput("ssh kloakzk01-dca1 'zkcli ls /kloak-dca1a/brokers/topics'")
if topic in text:
   print "topic in kloak-dca1a"

status, text = getstatusoutput("ssh kloakzk01-dca1 'zkcli ls /kloak-dca1b/brokers/topics'")
if topic in text:
   print "topic in kloak-dca1b"

status, text = getstatusoutput("ssh kloakzk21-sjc1 'zkcli ls /kloak-sjc1c/brokers/topics'")
if topic in text:
   print "topic in kloak-sjc1c"

status, text = getstatusoutput("ssh kloakzk03-sjc1 'zkcli ls /kloak-sjc1-agg1/brokers/topics'")
if topic in text:
   print "topic in kloak-agg"

#check if the topic exists in the kafka 7 world
print "checking for the topic in the kafka 7 world...."

for i in range (1,20):
     if i < 10:
         box_name = "logs" + "0"+str(i) + "-sjc1"
     else:
         box_name = "logs" + str(i) + "-sjc1"
     command = "ssh " + box_name + " "
     status, text = getstatusoutput(command + 'zkcli -p 2181 ls /brokers/topics')
     if topic in text:
         print "topic in:" + box_name

for i in range (1,20):
     if i < 10:
         box_name = "logs" + "0"+str(i) + "-dca1"
     else:
         box_name = "logs" + str(i) + "-dca1"
     command = "ssh " + box_name + " "
     status, text = getstatusoutput(command + 'zkcli -p 2181 ls /brokers/topics')
     if topic in text:
         print "topic in:" + box_name
