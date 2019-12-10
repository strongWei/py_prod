#!/bin/python
# get the chateau
# @author strong
# @email 1025155365@qq.com
from requests_html import HTMLSession
import pymysql.cursors

def splitAreaText(area_text):
    try:
        startI = area_text.index('(',0)
        endI = area_text.index(')',startI)
        num = area_text[startI+1:endI]
        return num
    except ValueError:
        return False

def splitAreaName(area):
    areas = area.split('  ')
    return areas

def requestChateau(chateau_obj,session,connection):
    req = session.get(chateau_obj['url'])
    lis = req.html.find('.wineRegion2 li a')

    for value in lis:
       if 'class' in value.attrs and 'curr' in value.attrs['class'] :
          #get the child chateau
          childs = req.html.find('.sch-kng ul li a p span.cname')
          for child in childs:
              area_names = child.text.split(' ') 
              name = area_names[0]
              en_name = area_names[1]
              layer_ids = '';
              if 'layer_ids' in chateau_obj:
                  layer_ids = str(chateau_obj['layer_ids']) +  '_' + str(chateau_obj['id'])
              else:
                  layer_ids = '0'
              chateau = {
                  'name': name,
                  'en_name': en_name,
                  'child_num': 0,
                  'parent_id': chateau_obj['id'],
                  'layer_ids': layer_ids
              }
              persistentChateau(chateau, connection)

          return    
    for value in lis:
        area_names = splitAreaName(value.attrs['title']) 
        name = area_names[0]
        en_name = area_names[1]
        child_num = splitAreaText(value.text) 
        layer_ids = '';
        if 'layer_ids' in chateau_obj:
            layer_ids = str(chateau_obj['layer_ids']) +  '_' + str(chateau_obj['id'])
        else:
            layer_ids = '0'
        chateau = {
            'name': name,
            'en_name': en_name,
            'child_num': child_num,
            'url': value.attrs['href'],
            'parent_id': chateau_obj['id'],
            'layer_ids': layer_ids
        }
        persistentChateau(chateau, connection)
        requestChateau(chateau, session, connection)
    
def persistentChateau(chateau_obj,connection):
    with connection.cursor() as cursor:
        sql = """insert into b_chateau(name,en_name,child_num,parent_id,layer_ids)
        values(%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, (chateau_obj['name'], chateau_obj['en_name'], chateau_obj['child_num'], chateau_obj['parent_id'],
            chateau_obj['layer_ids']))
        
        connection.commit()
        chateau_obj['id'] = cursor.lastrowid 
        return chateau_obj

session = HTMLSession()
connection = pymysql.connect(host='172.19.0.4', user='root', password='123456', db='buy', charset='utf8', cursorclass=pymysql.cursors.DictCursor)

try:
    root={
        'id':0,
        'url':'https://www.wine-world.com/winery/area'
    }
    requestChateau(root, session, connection)
finally:
    connection.close()
    print('complete')
