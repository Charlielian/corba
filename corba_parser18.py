

import xml.etree.ElementTree as ET

import glob,pymysql
import gzip,os,sys
import shutil
import datetime
import configparser
import multiprocessing
import time
import re
import numpy as np

def mysql_many(db,sql_str,datalist):
    #db = {'ip': mysql_ip,'db':mysql_db,'acc':'root','pw':'10300','port':50014}
    mysql_ip =db['ip']
    mysql_acc = db['acc']
    mysql_pw = db['pw']
    mysql_port =db['port']
    mysql_db = db['db']
    try:
         db = pymysql.Connect(
            host=mysql_ip,
            port=mysql_port,
            user= mysql_acc,
            passwd=mysql_pw,
            db=mysql_db,
            charset='utf8'
          )
     # print("连接成功！！")
         cursor = db.cursor()
     # print(sql_str)
         cursor.executemany(sql_str,datalist)
         #cursor.execute(sql_str)
         db.commit()
     # print("导入数据成功!!")
         db.close()
    except Exception as e:
         print(e)
         print(sql_str)



def un_gz(localpath,file_name_gz,remote):

    #print(remote)
    file_name = file_name_gz.replace(".gz", '')
    g = gzip.GzipFile(mode='rb', fileobj=open(r'%s\\%s' % (localpath, file_name_gz), 'rb'))
    open(r'%s\%s' % (remote, file_name), 'wb').write(g.read())
    return file_name


def mk_dir(path):
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)
def time_get():
    start_time, end_time, s_time = '','',''
    time_cur = datetime.datetime.now()
    hour_cur = time_cur.strftime('%Y%m%d%H')
    hour_last = (time_cur - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')
    st_time = time_cur.strftime('%Y-%m-%d %H')
    last_time = (time_cur - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H')
    print("当前时段为", time_cur.strftime('%Y%m%d%H%M'))
    if int(time_cur.strftime('%M')) >= 0 and int(time_cur.strftime('%M')) < 15:
        start_time = hour_last + '45'
        end_time = hour_cur + '00'
        s_time = last_time + ':45:00'

    if int(time_cur.strftime('%M')) >= 15 and int(time_cur.strftime('%M')) < 30:
        start_time = hour_cur + '00'
        end_time = hour_cur + '15'
        s_time = st_time + ':00:00'

    if int(time_cur.strftime('%M')) >= 30 and int(time_cur.strftime('%M')) < 45:
        start_time = hour_cur + '15'
        end_time = hour_cur + '30'

        s_time = st_time + ':15:00'
    if int(time_cur.strftime('%M')) >= 45 and int(time_cur.strftime('%M')) < 60:
        start_time = hour_cur + '30'
        end_time = hour_cur + '45'

        s_time = st_time + ':30:00'
    return start_time[0:8], start_time[8:12]



def readcsv(filename,data_dict):
    com_pp = []
    title= {}

    key_list = ['rmUID','阳江']
    with open(filename, mode='r', encoding='utf-8') as f:
        n = 1

        con = []
        for line in f:


            #print(line,type(line))
            if 'UserLabel' in line :
                tt = line.split("|")
                #print(tt)

                for  n in range(4,len(tt)):
                    print(tt[n])
                    title[n] = tt[n]

                #com_pp.append(line)
                #n = n + 1
            if  'NOKIA-CMGD-SZ' in line:
                tmp =[]


                tt = line.split("|")
                print(len(tt))
                start_time =tt[3]
                enbid =tt[1].split(",")[3].replace("EnbFunction=LNBTS-","")
                lcrid =tt[1].split(",")[4].replace("EutranCellTdd=LNCEL-", "")
                cellname =tt[2].replace('"','')
                #print(tmp)


                if   enbid in data_dict.keys():
                    if lcrid in data_dict[enbid].keys():
                        data_dict[enbid][lcrid]['start_time'] =   start_time
                        data_dict[enbid][lcrid]['enbid'] = enbid
                        data_dict[enbid][lcrid]['lcrid'] = lcrid
                        data_dict[enbid][lcrid]['cellname'] = cellname


                for  n in range(4,len(tt)):
                    print(title[n],tt[n])
                    data_dict[enbid][lcrid][title[n]] =  tt[n]
                    #print(tmp)
                con.append(tmp)
            else:
                pass


    #print(title)


    #print(com_pp)
    return data_dict

def readrr(key,com_pp,title):
    #file_list = dirfile(path)
    #print(filename,key,dt)
    key_item = key +"_"
    #print(key)

    try:
        title_con = ''
        title_num = []
        com_tt = []
        sy = '%s,%s,%s,%s,'

        title_con = title_con + title[0] +","
        title_con = title_con + title[1] + ","
        title_con = title_con + title[2] + ","
        title_con = title_con + title[3] + ","
        title_num.append(0)
        title_num.append(1)
        title_num.append(2)
        title_num.append(3)
        for num in range(4,len(title)) :
            if  title[num].split(".")[0] == key:
            #print(item)
                title_con = title_con +title[num].replace(".",'_').replace("\n",'') +","
                sy  = sy + '%s' + ","
                title_num.append(num)
        #print(title_con[:-1])


        for line in com_pp:
            con_tt = []
            for num in range(len(title_num)) :
                if line[num] == '' or line[num] == None:
                    con_tt.append(None)
                else:
                    con_tt.append(line[num])
            #print(len(title_num),len(con_tt))


            com_tt.append(tuple(con_tt))





        #print(com_tt)

        in_sql = 'insert ignore into ' + key + ' (' + title_con[:-1]  + ') VALUES (' + sy[:-1] + ') ;'
        return in_sql,tuple(com_tt)
    except Exception as e:

        print(e)



def main(start_time,xml_path,remote_path):
    s_timt = start_time[0:4] + "-" + start_time[4:6] + "-" + start_time[6:8] + " " + start_time[8:10] + ":" + start_time[10:12] + ":00"
    #key_item = start_time
    print(start_time )
    filelist = os.listdir(xml_path)
    title = {}
    text_list = []
    for file in filelist:

        if start_time in file:
            # 新建文件夹，如有则pass，没有则建立
            remotepath = remote_path + "/" + start_time
            mk_dir(remotepath)
            # 按照文件名进行分类，解析
            # day_str, raw_str=mo_path(xmlfile)
            # 搬迁文件至时间文件夹
            shutil.copyfile(xml_path + "/" + file, remotepath + "/" + file)
            # 进行解压到目标文件夹
            g = un_gz(remotepath, file, remotepath)
            print(remotepath + "/" + g)
            title,com_pp = readcsv(remotepath +"/" +g,title)
            os.remove(remotepath + "/" + g)
            #print(title)
    print(title)
            #for line in com_pp:
            #    print(line)
        #key_list = ['CONTEXT','HO', 'PHY', 'S1SIG', 'RRC', 'PAG', 'MAC', 'ERAB', 'IRATHO', 'RRU', 'PDCP']
        #for key in key_list:
        #    in_sql,datalist = readrr(key, com_pp, title)
                #in_sql = 'insert ignore into ' + key + ' (' + titler + ') VALUES (' + sy + ') ;'
                #print(in_sql)

        #    mysql_many(db, in_sql, datalist)
                #print(sql_str)
                #mysql_many(db, sql_str, datalist)
            #xml_to_csv(remotepath + "/" + g, xml_dict, text_list)
            # 解析后删除数据


    # print(len(text_list))

    # pp = multiprocessing.Pool(10)
    #key_list = ['RRU']
    #key_list = ['HO' ,'PHY', 'S1SIG', 'RRC', 'PAG', 'MAC', 'ERAB', 'IRATHO', 'RRU', 'PDCP', 'CONTEXT']
    #for key in key_list:
        # dict2list(xml_dict, text_list,s_timt)
    #    cel_tup, title_list, sy = mysql_write(key, xml_dict, text_list, s_timt)

        #print(title_list)
    #    in_sql = 'insert ignore into ' + key + ' (' + title_list + ') VALUES (' + sy + ') ;'
        # print(in_sql)
    #    mysql_many(db, in_sql, cel_tup)




            # pp.apply_async(mysql_write, (key, xml_dict, text_list, s_timt,))

            # print("----开始----")  # 关闭进程池,不再接收新的任务,开始执行任务
            # pp.close()  # 主进程等待所有子进程结束
            # pp.join()
    #return filelist


def judge(xml_path,key):
    filelist = os.listdir(xml_path)
    n =0
    for file in filelist:
        if key in file:
            n +=1
    if n >=2 :
        return True
    else:
        print("文件未生成齐！！！")
        return False





    







if __name__ == '__main__':

    exe_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
    config = configparser.ConfigParser()
    config.read(exe_path + "\\" + "conf.ini", encoding="utf-8-sig")
    xml_path = config.get("main","local_path") #'e:/python/corba_parser/gz'
    remote_path =  config.get("main","xml_path")  #'e:/python/corba_parser/xml'
    mysql_ip = config.get("mysql_config", "ip")
    mysql_db = config.get("mysql_config", "db")

    db = {'ip': mysql_ip,'db':mysql_db,'acc':'root','pw':'10300','port':3306}
    #xmlfile = 'ENB-PM-V2.7.0-EutranCellTdd-20190616-0815P04.xml.gz'
    #start_time, end_time =time_get()
    timer_conf = config.get("timer", "conf")
    if timer_conf == '1' :
        print("开启补数模式！！！")
        key_timer = config.get("timer", "key_time").split(",")
        for item in key_timer :
            print("补采......", item)
            start = time.time()

            main(item, xml_path, remote_path)
            end = time.time()

            print("完成时间: %0.2f s" % (end - start))
    else:
        start = time.time()

        tt =3
        while True:
            print("\n-----------------------------")
            start_time, end_time = time_get()

            if judge(xml_path, str(start_time)+"-"+str(end_time)) == True:
                main(start_time,  xml_path, remote_path)
            else:
                tt -=1
            if tt < 0 :
                break
            else:
                #print("重试第%s次，等待100S"%(3-tt))
                for i in range(100):
                    time.sleep(1)
                    print('\r重试第%s次，等待%s秒'%(3-tt,100-i), end='')







        end = time.time()

        print("完成时间: %0.2f s" % (end - start))




















