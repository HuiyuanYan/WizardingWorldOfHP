#!/bin/bash

# 设置颜色变量
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

INFO='\033[0;32m[INFO]\033[0m '
ERRORStr='\033[0;31m[ERROR]\033[0m '

hadoop_dfs_path='/home/yhy/hadoop_installs/hadoop-3.3.5/sbin/start-dfs.sh'
hadoop_yarn_path='/home/yhy/hadoop_installs/hadoop-3.3.5/sbin/start-yarn.sh'

nameList_path='/user/yhy/nameList.txt'
dataSet_path='/user/yhy/dataSet'
task1_output_path='/user/yhy/output1'
task2_output_path='/user/yhy/output2'
task3_output_path='/user/yhy/output3'
task4_output_path='/user/yhy/output4'
filter_output_path='/user/yhy/filterOutput'
task5_output_path='/user/yhy/output5'

task1_jar_path='./target/WizardingWorldOfHPTask1-1.0-SNAPSHOT.jar'
task2_jar_path='./target/WizardingWorldOfHPTask2-1.0-SNAPSHOT.jar'
task3_jar_path='./target/WizardingWorldOfHPTask3-1.0-SNAPSHOT.jar'
task4_jar_path='./target/WizardingWorldOfHPTask4-1.0-SNAPSHOT.jar'
task5_jar_path='./target/WizardingWorldOfHPTask5-1.0-SNAPSHOT.jar'
filter_jar_path='./target/WizardingWorldOfHPFilter-1.0-SNAPSHOT.jar'

echo -e "${INFO}running projects, please make sure you have set the right file path, load nameList and dataSet to hdfs, and all output paths DO NOT exist in hdfs."


# start hdfs
echo -e "${INFO}starting Hadoop hdfs."
if jps | grep -q "NameNode" && jps | grep -q "DataNode"; then
  echo -e "${INFO}dfs is already started"
else
  ${hadoop_dfs_path}
fi

if jps | grep -q "ResourceManager" ; then
  echo -e "${INFO}yarn is already started"
else
  ${hadoop_yarn_path}
fi

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"



# task1
echo -e "${INFO}running task1"
hadoop jar ${task1_jar_path} ${nameList_path} ${dataSet_path} ${task1_output_path}

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"



# task2
echo -e "${INFO}running task2"
hadoop jar ${task2_jar_path} ${task1_output_path} ${task2_output_path}

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"


# task3
echo -e "${INFO}running task3"
hadoop jar ${task3_jar_path} ${task2_output_path} ${task3_output_path}

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"

# filter
echo -e "${INFO}running filter"
hadoop jar ${filter_jar_path} ${task3_output_path} ${filter_output_path}

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"


# task5
echo -e "${INFO}running task5"
hadoop jar ${task5_jar_path} ${filter_output_path} ${task5_output_path}

if [ $? -ne 0 ]; then
  echo -e "${ERROR}failed"
  exit 1
fi
echo -e "${INFO}finished"
# 所有命令执行成功


echo -e "${INFO}all tasks finished"
exit 0
