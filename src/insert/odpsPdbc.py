#-*- coding: UTF-8 -*-
#!/bin/usr/python3
from odps import ODPS
import sys,os
import logging
from odps.models import Schema, Column, Partition

accessId = "LTAIWNem0WmPwJCK"
secretAccessKey = "mOCt0AhvfUBKbZrjyhkBEzww6E0f0m"
projectName = "hadoop_odps"

odps = ODPS(accessId,secretAccessKey,projectName)

if not os.path.exists("log"):
    os.mkdir("log")
logger = logging.getLogger("exception")
logger.setLevel(level = logging.INFO)
exceptionInfo = logging.FileHandler("log/exceptionInfo.log")
exceptionInfo.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
exceptionInfo.setFormatter(formatter)
logger.addHandler(exceptionInfo)


def isTableExists(tableName):
    try:
        if odps.get_table(tableName):
            return 0
        else:
            return -1
    except:
        return -100

def createTable(tableName):
    try:
        columns = [Column(name='user_id', type='string', comment='用户ID'),
                Column(name='node_id', type='string', comment='节点号'),
                Column(name='label_type', type='string', comment='标签类型'),
                Column(name='label_value', type='string', comment='标签值'),
                Column(name='precision_value', type='string', comment='准确率'),
                Column(name='sampleCoverNum', type='string', comment='样本覆盖量'),
                Column(name='sameLabelTotalNum', type='string', comment='样本同一标签总量'),
                Column(name='sampleTotalNum', type='string', comment='样板总量'),
                Column(name='model_name', type='string', comment='模型名称'),
                Column(name='created_at', type='string', comment='插入时间')]
        partitions = [Partition(name='pt', type='string', comment='批次分区')]
        schema = Schema(columns=columns, partitions=partitions)
        table = odps.create_table(tableName, schema, if_not_exists=True)
        return 0
    except:
        logger.warning("creatTable warning",exc_info = True)
        return -100

def insertSql(sql):
    try:
        #异步执行
        result = odps.run_sql(sql)
        #print(result.get_logview_address())
        #同步执行
        #odps.execute_sql("sql")
        return 0
    except:
        logger.warning("insertSql warning",exc_info = True)
        return -100


