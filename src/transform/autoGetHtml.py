
# -*- coding:utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import os,time
import configparser
import transform
import logging
import logging.config
import re
import threading

conf=configparser.ConfigParser()
conf.read('config.conf')
url=conf.get('autoGetHtml','url')

modelRules=conf.get('autoGetHtml','modelRules')
htmlOutputPath = conf.get('autoGetHtml','htmlOutputPath')
xmlOutputPath = conf.get('autoGetHtml','xmlOutputPath')
csvOutputPath = conf.get('autoGetHtml','csvOutputPath')
sqlOutputPath = conf.get('autoGetHtml','sqlOutputPath')
batch = conf.get('conf','batch')
labelName = conf.get('conf','labelName')
logging.config.fileConfig('logging.conf')

logger_autoGetHtml = logging.getLogger('autoGetHtml')

def log(msg,level='info'):
    if level == "info":
        logger_autoGetHtml.info("\033[1;33m:%s\033[0m" % msg)
    elif level == "debug":
        logger_autoGetHtml.debug("\033[1;33m:%s\033[0m" % msg)
    elif level == "warning":
        logger_autoGetHtml.warning("\033[1;31m:%s\033[0m" % msg)
    elif level == "error":
        logger_autoGetHtml.error("\033[1;31m:%s\033[0m" % msg)

if __name__ == "__main__":
    thread = {}
    driver = webdriver.Chrome()
    #driver.implicitly_wait(20)
    driver.get(url)
    input_string = ""
    print("\033[1;33m 参数确认\n模型匹配规则:%s\n标签类型名称:%s\n批次名:%s\n\033[0m" % (modelRules,labelName,batch))
    while(input_string!="ok"):
        input_string = input("\033[1;33m准备就绪输入OK:\033[0m\n")
    print("\033[1;33m等待网页加载完成..\033[0m")
    log("获取窗口句柄",level='debug')
    # 获取打开的多个窗口句柄
    windows = driver.window_handles
    #print(windows)
    for window in windows:
        log("切换至最后一个窗口",level='debug')
        driver.switch_to.window(window)
        log("最大化窗口",level='debug')
        driver.maximize_window()
        log("获取列表树ID",level='debug')
        table_trees = driver.find_elements_by_xpath("""//table[@class="tree-wrapper table table-hover"]""")
        experiment_table_id = table_trees[0].get_attribute("id")
        log("获取列表树元素",level='debug')
        experiment_table = driver.find_elements_by_xpath("""//table[@id=\""""+experiment_table_id+"""\"]/tbody/tr[@class='tree-node']""")
        times = 1
        for experiment_tree_node in experiment_table:
            #print(experiment_tree_node.text)v
            pattern = re.compile(r''+modelRules)
            result = re.match(pattern,experiment_tree_node.text)
            if not result:
                continue
            experiment_tree_node.click()
            title = experiment_tree_node.text
            time.sleep(1)
            log("[第%d组]切换至 %s 实验" % (times, title))
            experiment_models = driver.find_elements_by_xpath("""//*[name()='svg']/*[name()='g']/*[name()='g'][3]/*[name()='g']""")
            experiment_model = experiment_models[len(experiment_models)-1]
            log("[第%d组]获取第%d个学习节点" % (times, len(experiment_models)-1),level='debug')
            ActionChains(driver).context_click(experiment_model).perform()
            time.sleep(1)
            experiment_select = driver.find_elements_by_xpath("""//li[@data-id="node-showData"]""")
            log("[第%d组]显示数据" % (times),level='debug')
            experiment_select[0].click()
            log("[第%d组]15秒后保存文件" % (times),level='debug')
            time.sleep(10)
            save_times = 1
            retry_times = 0
            log("[第%d组]5秒后保存文件(%d)" % (times,save_times))
            time.sleep(5)
            log("[第%d组]开始保存文件(%d)" % (times,save_times))
            htmlFile = open(os.path.join(htmlOutputPath,title+".html"),"w",encoding='utf8')
            htmlFile.write(str(driver.page_source))
            htmlFile.close()
            log("[第%d组]%s 页面保存成功(%d)" % (times,title,save_times))
            time.sleep(1)
            thread[times] = threading.Thread(target=transform.startTransform, args=(title+".html",1,times,labelName,))
            thread[times].start()
            time.sleep(1)
            window_close = driver.find_elements_by_xpath("""//button[@class="ant-modal-close"]""")
            log("[第%d组]关闭页面" % (times),level='debug')
            window_close[0].click()
            time.sleep(1)
            times += 1

