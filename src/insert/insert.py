#-*- coding: UTF-8 -*-
import odpsPdbc
import glob
import sys,os
import time
import logging

global logger

def init():
    global logger
    if not os.path.exists("log"):
        os.mkdir("log")
    if not os.path.exists("sql"):
        os.mkdir("sql")
    logger = logging.getLogger("insertSql")
    logger.setLevel(level = logging.INFO)
    info = logging.FileHandler("log/info.log")
    info.setLevel(logging.INFO)
    warning = logging.FileHandler("log/warning.log")
    warning.setLevel(logging.WARNING)
    error = logging.FileHandler("log/error.log")
    error.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    info.setFormatter(formatter)
    warning.setFormatter(formatter)
    error.setFormatter(formatter)

    logger.addHandler(info)
    logger.addHandler(warning)
    logger.addHandler(error)



def stdout_u(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()

def dealTime(remainTime):
    if remainTime >= 60 and remainTime < 3600:
        remainTime = str(remainTime/60) + "min"
    elif remainTime >= 3600 and remainTime < 86400:
        remainTime = str(remainTime/60/60) + "hour"
    elif remainTime >= 86400:
        remainTime = str(remainTime/60/60/24) + "day"
    elif remainTime < 60:
        remainTime = str(remainTime) + "sec"
    return remainTime

if __name__ == "__main__":
    init()
    fileTotal = 0
    currentFile = 0
    currentLine = 0
    hasExecuteLine = 0
    ErrorLineNum = 0
    fileLineTotal = 0
    allLineTotal = 0
    tableName = sys.argv[1]
    odpsPdbc.createTable(tableName)
    startTime = time.time()
    for (root, dirs, files) in os.walk("sqls"):
        for sqlFile in files:
            if sqlFile.endswith(".sql"):
                fileTotal += 1
                f = open(os.path.join("sqls",sqlFile),"r")
                sqlLines = f.readlines()
                f.close()
                allLineTotal += len(sqlLines)
        for sqlFile in files:
            if sqlFile.endswith(".sql"):
                currentFile += 1
                currentLine = 0
                f = open(os.path.join("sqls",sqlFile),"r")
                sqlLines = f.readlines()
                fileLineTotal = len(sqlLines)
                for sql in sqlLines:
                    currentLine += 1
                    hasExecuteLine += 1
                    ret = odpsPdbc.insertSql(sql)
                    if ret == 0:
                        logger.info("[%s(line:%s)] - %s" % (sqlFile,currentLine,sql))
                    else:
                        ErrorLineNum += 1
                        logger.error("[%s(line:%s)] - %s" % (sqlFile,currentLine,sql))
                    currentTime = time.time()
                    hasUsedTime = currentTime-startTime
                    if hasUsedTime == 0:
                        hasUsedTime = 1
                    speed = float(hasExecuteLine)/float(hasUsedTime)
                    remainTime = int(allLineTotal/speed)
                    remainTime = dealTime(remainTime)
                    stdout_u("\r\033[1;32mFile(%s/%s) Lines(%s/%s) \033[1;31mErrorLines:%s  \033[1;32m Speed:%.2fLine/sec  Remain:%s\033[0m" %(currentFile,fileTotal,currentLine,fileLineTotal,ErrorLineNum,speed,remainTime))
                f.close()
