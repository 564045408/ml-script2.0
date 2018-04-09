#-*- coding: UTF-8 -*-
#!/usr/bin/python3
from bs4 import BeautifulSoup
import glob
import pdb
import os,time
import csv
import threading
import configparser
import re
import logging
import logging.config

logging.config.fileConfig('logging.conf')

logger = logging.getLogger('transform')

conf=configparser.ConfigParser()
conf.read('config.conf')
htmlOutputPath=conf.get('autoGetHtml','htmlOutputPath')
xmlOutputPath = conf.get('autoGetHtml','xmlOutputPath')
csvOutputPath = conf.get('autoGetHtml','csvOutputPath')
sqlOutputPath = conf.get('autoGetHtml','sqlOutputPath')
#最小文件大小阈值
minInputSize = int(conf.get('conf','minInputSize'))
#标签类型
labelName = conf.get('conf','labelName')
#实验批次
batch = conf.get('conf','batch')
#文件总数
fileTotal = 0
fileComplete = 0

def html2xml(htmlPath,thread,labelName):
    htmlFile = open(htmlPath, 'r',encoding="utf-8")
    text = htmlFile.read()
    soup = BeautifulSoup(text, 'html.parser')
    cells = soup.select('div.cell-content')
    xmlFilename = os.path.split(htmlPath)[-1].replace('html', 'xml')
    xmlFilename = xmlFilename.split('.')[0] + ".xml"
    xmlFile = open(os.path.join(xmlOutputPath, xmlFilename), 'w')
    for cell in cells:
        if len(cell.text) < 100:
            continue
        content = cell.text
        content = content.replace('\t', '')
        content = content.replace('\n', '')
        xmlFile.write(content)
    htmlFile.close()
    xmlFile.close()
    logger.info('\033[1;33m[线程%s]HTML -> XML : %s 转换完毕\033[0m' % (thread,xmlFilename))
    return xmlFilename

def xml2csv(xmlPath,threshold,thread):
    try:
        xmlFile = open(xmlPath, 'r')
        text = xmlFile.read()
        soup = BeautifulSoup(text, 'xml')
        rootNode = soup.find(id="0")
        sampleTotalNum = rootNode.attrs['recordCount']
        scoreDistributions = findChildren(rootNode, 'ScoreDistribution')
        sameLabelTotals = {}
        for scoreDistribution in scoreDistributions:
            sameLabelTotals[int(scoreDistribution.attrs['value'])] = int(scoreDistribution.attrs['recordCount'])
        #Init Csv
        csvFilename = os.path.split(xmlPath)[-1].replace('xml', 'csv')
        csvFile = open(os.path.join(csvOutputPath, csvFilename), 'w',encoding="utf-8")
        csvFile.write('nodeId,precision,recall,rightSampleNum,sampleCoverNum,sameLabelTotalNum,sampleTotalNum,labelValue,rule\n')
        #Get attributes
        scoreDistributions = soup.findAll('ScoreDistribution')
        for scoreDistribution in scoreDistributions:
            if float(scoreDistribution.attrs['probability']) >= threshold:
                node = scoreDistribution.parent
                nodeId = node.attrs['id']
                scoreDistributions = findChildren(node, 'ScoreDistribution')
                rule = getRuleByNode(node)
                for scoreDistribution in scoreDistributions:
                    precision = scoreDistribution.attrs['probability']
                    rightSampleNum = int(scoreDistribution.attrs['recordCount'])
                    sampleCoverNum = int(node.attrs['recordCount'])
                    labelValue = int(scoreDistribution.attrs['value'])
                    sameLabelTotalNum = sameLabelTotals[labelValue]
                    csvFile.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                        #节点号
                        nodeId,
                        #准确率
                        precision,
                        #样本覆盖率
                        rightSampleNum / sameLabelTotalNum,
                        #该属性正确样本量
                        rightSampleNum,
                        #样本覆盖量
                        sampleCoverNum,
                        #该标签样本总量
                        sameLabelTotalNum,
                        #样本总量
                        sampleTotalNum,
                        #样本属性值
                        labelValue,
                        #规则
                        rule
                    ))
        csvFile.close()
        logger.info('\033[1;33m[线程%s]XML -> CSV : %s 转换完毕\033[0m' % (thread,csvFilename))
        return csvFilename
    except:
        csvFilename = xmlPath.split('/')[-1].replace('xml', 'csv')
        logger.info('\033[1;31m[线程%s]XML -> CSV : %s 转换异常\033[0m' % (thread,csvFilename))
        return -1

def csv2sql(csvFile,thread):
    model_name = csvFile.split('/')[-1].split('.')[0]
    label_type = model_name.split('_')[-1]
    from_table_name = model_name_to_table(model_name)
    sqlFilename = model_name + '.sql'
    sqlFile = open(os.path.join(sqlOutputPath, sqlFilename) , 'w')
    with open(csvFile) as file:
        reader = csv.DictReader(file)
        for row in reader:
            sql = "insert into table seanTest partition(pt='%s') (user_id, node_id, label_type, label_value, precision_value, sampleCoverNum, sameLabelTotalNum, sampleTotalNum, model_name, created_at) select user_id, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' from %s where %s;" % (
                batch,
                row['nodeId'],
                label_type,
                row['labelValue'],
                row['precision'],
                row['sampleCoverNum'],
                row['sameLabelTotalNum'],
                row['sampleTotalNum'],
                model_name,
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                from_table_name,
                row['rule']
            )
            sqlFile.write(sql + '\n')
    sqlFile.close()
    logger.info('\033[1;33m[线程%s]CSV -> SQL : %s 转换完毕\033[0m' % (thread,sqlFilename))
    return sqlFilename

def model_name_to_table(model_name):
    arr = model_name.lower().split('_')
    arr.pop()
    table_name = 'train_data_' + ('_').join(arr)
    if 'before' in table_name or 'after' in table_name:
        table_name += '_half'
    return table_name

def replaceOperator(operator):
    if operator == 'lessOrEqual': return '<='
    if operator == 'greaterThan': return '>'

def getRuleByNode(node):
    conditions = []
    operator = node.find('SimplePredicate')
    while True:
        simplePredicate = node.find('SimplePredicate')
        conditions.append(simplePredicate.attrs['field'] + ' ' + replaceOperator(simplePredicate.attrs['operator']) + ' ' + simplePredicate.attrs['value'])
        if node.parent.parent.name != 'Node':
            break
        node = node.parent
    conditions.reverse()
    return (' and ').join(conditions)

def findChildren(node, tagName):
    children = []
    for child in node.children:
        if child != '\n' and child.name == tagName:
            children.append(child)
    return children


def startTransform(htmlFileName,threshold,threadNo,labelName):
    global fileComplete
    xmlName = html2xml(os.path.join(htmlOutputPath,htmlFileName).replace("\\","/"),threadNo,labelName)
    if xmlName != -1:
        csvName = xml2csv(os.path.join(xmlOutputPath,xmlName).replace("\\","/"),1,threadNo)
        if csvName != -1:
            sqlName = csv2sql(os.path.join(csvOutputPath,csvName).replace("\\","/"),threadNo)
            fileComplete += 1
            logger.info("\033[1;32m[线程%s] %s 转换完成\33[0m" %(threadNo,htmlFileName))

if __name__=="__main__":
    threadNo = 1
    thread = {}
    for (root, dirs, files) in os.walk(htmlOutputPath,):
        for htmlfile in files:
            if htmlfile.endswith(".html") and os.path.getsize(os.path.join(htmlOutputPath,htmlfile)) > minInputSize:
                fileTotal += 1
        for htmlfile in files:
            if htmlfile.endswith(".html") and os.path.getsize(os.path.join(htmlOutputPath,htmlfile)) > minInputSize:
                thread[threadNo] = threading.Thread(target=startTransform, args=(htmlfile,1,threadNo,labelName,))
                thread[threadNo].start()
                thread[threadNo].join()
                threadNo += 1
