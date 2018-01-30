import pandas as pd
import requests
import datetime
from lxml import etree

def get_html(url):  #取得HTML文本
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""

#转换list维度，从1维到2维
def split_list(datas, n):
    length = len(datas)
    size = length // n + 1 if length % n  else length//n
    _datas = []
    for i in range(size):
        start = i * n
        end = (i + 1) * n
        _datas.append(datas[i * n: (i+1)*n])
    return _datas

#取得基金具体信息，未使用，未完成
def get_fund_info(code):
    url ='http://fund.eastmoney.com/#code.html'
    url = url.replace('#code',code)
    result = ['type','risk','size','manager','managercode','cop','copcode']
    HTML = get_html(url)
    s = HTML.lower()
    page = etree.HTML(s)
    if page==None:
        return
    keys = page.xpath('//div/div/div/div/div/div/table/tr/td/a')
    if len(keys)<1:
        return None

    result[0]=keys[0].text   #type

    s = keys[0].tail #risk
    result[1] = s

    s = keys[1].tail
    if s != None:
        i = s.find('元')
        s = s[1:i-1]
        result[2] = s            #size

    result[3] = keys[2].text #manager

    s = str( keys[2].values())  #manager code
    i = s.find('.html')
    s = s[i-6:i]
    result[4] = s

    result[5] = keys[3].text #cop

    s = str(keys[3].values()) #copcode
    i = s.find('.html')
    s = s[i - 6:i]
    result[6] = s
    return result

#取得详细信息，未完成，未使用
def get_full_data():
    print("Getting fund information......")
    frame = get_fund()
    print('Getting fund details....')

    for i in range(0, len(frame)):
        try:
            lst = get_fund_info(frame.iloc[i,0])
            print(str(i+1)+'/'+str(len(frame)) + ' - ' + frame.iloc[i,0])
            if lst==None or len(lst)<7:
                i+=1
                continue
            frame.loc[i,'type']=lst[0]
            frame.loc[i, 'risk'] = lst[1]
            frame.loc[i, 'size'] = lst[2]
            frame.loc[i, 'manager'] = lst[3]
            frame.loc[i, 'managercode'] = lst[4]
            frame.loc[i, 'cop'] = lst[5]
            frame.loc[i, 'copcode'] = lst[6]
        finally:
            i += 1
    frame.to_csv('fundall.csv')

#取得基金列表
def get_fund():
    #先凑一个我们需要的URL出来
    max_jj='5000' #调试5 工作5000
    fromstr = datetime.datetime.now().strftime('%Y-%m-%d')
    tostr = (datetime.datetime.now()- datetime.timedelta(days=5*365+1)).strftime('%Y-%m-%d')
    url = "http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&sd={}&ed={}&qdii=&tabSubtype=,,,,,&pi=1&pn={}&dx=1"
    url=url.format(tostr, fromstr, max_jj)

    #取得文本
    s = get_html(url)
    #去掉冗余信息，这里应该有更好的方法，但懒得折腾了，这样简单，十分钟搞定
    s = s[22:-159]
    for x in ['"',"'",']','[']:
        s = s.replace(x,'')
    lst = s.split(',')
    lst = split_list(lst,25)
    frame = pd.DataFrame(lst,columns=['code','name','py','3','4','jz','day1','week1','month1','month3','month6','year1','year2','year3','year0','yearall','fromdate','17','year5','19','20','21','22','23','24'])
    frame = frame.iloc[:,[0,1,2,5,6,7,8,9,10,11,12,13,14,15,16,18]]
    frame.to_csv('fund.csv')
    return frame

def main():
    #get_full_data()
    get_fund()    #如果每次都需要用最新数据，用这句
    df_full = pd.DataFrame.from_csv('fund.csv')  #节省网络流量，就用这句
    X = 50  #取排名前多少的基金

    df_full =  df_full.sort_values(by='year1', axis=0, ascending=False)
    df = df_full.head(X)
    for xx in ['year1','year2','year3','year5','month6']:
        tmp = df_full.sort_values(by=xx, axis=0, ascending=False).head(X)
        df=df.merge(tmp,on=['code'])

    df=df.iloc[:,0:30]
    df.to_csv('result.csv')
    print(df)

main()