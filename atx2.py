#@XCram XCram Update draft_auto_trading

import datetime
import time
#import MySQLdb
#from utils.mysql import MySqlOperation
import pymysql
import config
import uuid
from binance import Binance

# инициализация подключения к БД
#input: strategy (string)
#output: 2 lists of tuples
def FAccountInfo(user,account,strategy,symbol):
  db = pymysql.connect(
    config.MySqlConfig.host,
    config.MySqlConfig.mysql_user,
    config.MySqlConfig.mysql_passwd,
    config.MySqlConfig.db,
  )
  
  
  #try:
    #db = MySQLdb.connect(CONF_MYSQL_HOST,CONF_MYSQL_USER,CONF_MYSQL_PASS,CONF_MYSQL_DB)
  cursor = db.cursor()
  sql = 'SELECT user,account,type,comment,price,summa,balance,symbol FROM events WHERE user = "'+user+'"  AND  account = "'+account+'"'
    #.format(', '.join('{}'.format(BOTS_COLUMNS[i]) for i in range(len(BOTS_COLUMNS))))
    #q_tuple = (1, strategy)
    #try:
  #print(sql)
  cursor.execute(sql)
  results = cursor.fetchall()
  maxbalance=0
  symbol1=symbol[0:3]
  symbol2=symbol[3:]
  balance1=0
  balance2=0
  lp=0
  lv=0
  for s in results: 
    type=s[2]
    comment=s[3]
    price=s[4]
    summa=s[5]
    balance=s[6]
    symbol=s[7]
    if(balance>maxbalance): maxbalance=balance
    if(symbol==symbol1): balance1=balance
    if(symbol==symbol2): balance2=balance
    if(comment==strategy):
      #print(" comment  ",comment," type ",type," s ",symbol ," s1 ",symbol1," strategy ",strategy)
      if(type=='Buy' and symbol==symbol1):
       lp=price
       lv=summa 
	  if(type=='Sell' and symbol==symbol1):
       lp=0
       lv=0
  res=[balance1,balance2,symbol1,symbol2,lp,lv]
  cursor.close()
  db.close()
  return res
  
  
  
def FCPrice(access,symbol):
  db = pymysql.connect(
    config.MySqlConfig.host,
    config.MySqlConfig.mysql_user,
    config.MySqlConfig.mysql_passwd,
    config.MySqlConfig.db,
  )
  cursor = db.cursor()
  sql = 'SELECT api,api_secret FROM accesses WHERE name = "'+access+'"'
  cursor.execute(sql)
  res = cursor.fetchone()
  api=res[0]
  secret=res[1]
  #print(res[0],res[1])
  #  t
  cursor.close()
  db.close()
  bot = Binance(
    API_KEY=api,
    API_SECRET=secret
  )
  price=bot.tickerPrice(
    symbol=symbol
  ) 


  return  price['price']
  

def FSendOrder(user,strategy,symbol1,symbol2,balance1,balance2,volume,price):
  db = pymysql.connect(
    config.MySqlConfig.host,
    config.MySqlConfig.mysql_user,
    config.MySqlConfig.mysql_passwd,
    config.MySqlConfig.db,
  ) 
  cursor = db.cursor()
  #date = datetime.datetime.utcfromtimestamp(cur_utime).strftime('%Y-%m-%d %H:%M:%S')
  date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  account="Demo"

  type="Sell"
  balance=float(balance2)-float(volume*price)
  sql = 'insert into events (date,user,account,type, comment,price,summa,balance,symbol,contractid) values ("'+date+'","'+user+'","'+account+'","'+type+'","'+strategy+'","'+str(price)+'","'+str(volume*price)+'","'+str(balance)+'","'+symbol2+'",1)'
  #print("=insert sql=",sql)
  cursor.execute(sql)
  
  type="Buy"
  balance=float(balance1)+float(volume)

  sql = 'insert into events (date,user,account,type, comment,price,summa,balance,symbol,contractid) values ("'+date+'","'+user+'","'+account+'","'+type+'","'+strategy+'","'+str(price)+'","'+str(volume)+'","'+str(balance)+'","'+symbol1+'",1)'
  #print("=insert sql=",sql)
  cursor.execute(sql) 
  
 
  db.commit() 
  cursor.close()
  db.close()
 
  


def FBots(strategy):
  db = pymysql.connect(
    config.MySqlConfig.host,
    config.MySqlConfig.mysql_user,
    config.MySqlConfig.mysql_passwd,
    config.MySqlConfig.db,
  )
  table_name = 'bots'
  results_from_bots_list = []
  first_trade_list = []
  #try:
    #db = MySQLdb.connect(CONF_MYSQL_HOST,CONF_MYSQL_USER,CONF_MYSQL_PASS,CONF_MYSQL_DB)
  cursor = db.cursor()
  sql = 'SELECT user,name,access,strategy,enabled,volumefirst,volume_value_first_order,volume_percent_first_order,percent_step_insurance_order,cycle_limit,takeprofit,martingale,symbol FROM bots WHERE enabled =1 AND strategy = "'+strategy+'"'
    #.format(', '.join('{}'.format(BOTS_COLUMNS[i]) for i in range(len(BOTS_COLUMNS))))
    #q_tuple = (1, strategy)
    #try:
  #print(sql)
  cursor.execute(sql)
  results = cursor.fetchall()
  # oneRow = cursor.fetchone()
  # print ("Row Result: ", oneRow)
  #print(results)
      #for row in results:
        # if row[9] == 0: #order_num_current
          # first_trade_list.append(row)
         # else:
          # results_from_bots_list.append(row)
    #except:
     # print ('Error: unable to fetch data from %s table'%table_name)
      # logging.info('Error: unable to fetch data from %s table'%table_name)
  cursor.close()
  db.close()
  return results
 
    #return first_trade_list, results_from_bots_list
  #except (MySQLdb.Error, MySQLdb.Warning) as e:
    #print ('Connect to MSQL database error %d %s'%(e.args[0],e.args[1]))
    # logging.info('Connect to MSQL database error: %d (%s)'%(e.args[0],e.args[1])
    #return e.args[0]




def FOrders(bots_list): #open orders
  if bots_list:
    #table_name = 'transactions'
      main_table_name = 'bots'
    #try:
      db = pymysql.connect(
       config.MySqlConfig.host,
       config.MySqlConfig.mysql_user,
       config.MySqlConfig.mysql_passwd,
       config.MySqlConfig.db,
      )
      cursor = db.cursor()
      for i in range(len(bots_list)):
        user = bots_list[i][0]
        name = bots_list[i][1]
        access = bots_list[i][2]
        strategy = bots_list[i][3]
        enabled = bots_list[i][4]
        volumefirst = bots_list[i][5]
        volume_value_first_order = bots_list[i][6]
        volume_percent_first_order = bots_list[i][7]
        percent_step_insurance_order = bots_list[i][8]
        cycle_limit = bots_list[i][9] #ORDERS EXECUTED IN SINGLE CYCLE (int)
        takeprofit = bots_list[i][10] #MAX ORDERS TOTAL (int)
        martingale = bots_list[i][11] #POSITIVE PERCENTAGE VALUE (XX.YY%)
        symbol = bots_list[i][12] #POSITIVE PERCENTAGE VALUE (XX.YY%)
        
        info=FAccountInfo(user,"Demo",strategy,symbol)
        cp=float(FCPrice(access,symbol)) #current price
        
        print(info)
       
        #balance1, 2, symbol 1,2 , lp, lv
        balance1=info[0]
        balance2=info[1]
        symbol1=info[2]
        symbol2=info[3]
        lp=info[4]
        lv=info[5] 
        print("= cp ",cp," lp ",lp," lv ",lv)
        if(volumefirst==0): vol=volume_value_first_order
        else: vol=(info[2]*volume_percent_first_order)/(100*cp)
        if(lv>0): vol=float(lv)*float(martingale)

        delta=100-lp*100/cp
        print(" delta",delta," percent ",percent_step_insurance_order)
        if(delta==0 or delta>percent_step_insurance_order):
         FSendOrder(user,strategy,symbol1,symbol2,balance1,balance2,vol,cp)
        
        
        
        
        #exit()
       
      cursor.close()
      db.close()
    # except (MySQLdb.Error, MySQLdb.Warning) as e:
      # print ('Connect to MSQL database error %d %s'%(e.args[0],e.args[1]))
      # logging.info('Connect to MSQL database error: %d (%s)'%(e.args[0],e.args[1])
      #return e.args[0]
 
###############################################SCRIPTS 
#SCRIPTS FOR TESTS END

if __name__ == '__main__':
  
 #initial orders
  
  while True:
    bots = FBots('Long') # Get data from events and bots tables where strategy/comment is 'long'
    print (bots)
    FOrders(bots) 
    #data = mysql_db_get_data_from_bots('long')
    #process_auto_trade(data[1])
    #index = next(pg)
    #print ('Index: %d'%index)
    #update_price_current_bots(pl[index], 'X/Y')
    time.sleep(30)
