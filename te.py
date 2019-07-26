import os
import numpy as np
import pandas as pd
import pymssql
import datetime
import configparser

cf = configparser.ConfigParser()
cf.read('D:\python\code\db_config.ini')

col_name = ['文件名','账号','账务流水号','业务流水号','商户订单号','商品名称','发生时间','对方账号','收入金额','支出金额','账户余额','交易渠道','业务类型','备注']
vals =  ('%s,'* len(col_name))[:-1]

conn = pymssql.connect(cf.get('db_config','ip'),cf.get('db_config','user'),cf.get('db_config','pwd'),cf.get('db_config','db'))
#conn = pymssql.connect('172.10.1.231','sa','******','Alipay')
cursor = conn.cursor()

cursor.execute('select * into Alipay..支付宝账单明细_201906_check_1 from Alipay..支付宝账单明细 where 1 <> 1')

def file_name(file_dir):   
    L=[]
    for root, dirs, files in os.walk(file_dir):  
        for file in files:  
            if os.path.splitext(file)[1] == '.csv':
                tmp = (os.path.join(root, file),file.replace('.csv',''))
                L.append(tmp)
    return L


start_t = datetime.datetime.now()

file = file_name(r'D:\李康康\财务支付宝账单\支付宝账单_201906\明细_check')
#file = file_name('D:\李康康\财务支付宝账单\支付宝账单_test')

for i in range(len(file)):
    data = pd.read_csv(file[i][0],sep=',',skiprows=4,skipfooter=4)  #encoding='gb2312',
    data.insert(0,'filename',file[i][1])
    data.insert(1,'account',file[i][1][:20])
    data_list = data.values.tolist()

    new_list=[]
    
    for j in data_list:
        new_list.append(j[0:14])
        
    list_c = []
    list_a = []

    for i in new_list:
        for j in range(len(i)):
            list_c.append(str(i[j]).strip('\t').strip('\n'))
        list_a.append(tuple(list_c))
        list_c = []

    for t in range(0,len(list_a),1000):
        #print(l[t:t+3])
        cursor.executemany('insert into Alipay..支付宝账单明细_201906_check_1(' + ','.join(col_name) + ') values(' + vals + ')',list_a[t:t+1000])
        conn.commit()

    # for i in list_a:
    #     cursor.executemany('insert into Alipay..支付宝账单明细_201906_check_1(' + ','.join(col_name) + ') values(' + vals + ')',[i])
    #     conn.commit()

conn.close()

end_t = datetime.datetime.now()
print((end_t - start_t).seconds)