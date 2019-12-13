#!/bin/python
# get the chateau
# @author strong
# @email 1025155365@qq.com
from requests_html import HTMLSession
import pymysql.cursors

def splitGrapeText(grape_text):
    try:
        startI = grape_text.index('(',0)
        endI = grape_text.index(')',startI)
        name = grape_text[:startI]
        en_name = grape_text[startI+1:endI]
        return [name, en_name] 
    except ValueError:
        return False


def requestGrapeVariety(session,connection):
    req = session.get('https://www.wine-world.com/grape')
    grapeSets = req.html.find('.itemcard2 .grape-set')
    
    alphas = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V',
            'W','X','Y','Z']
    for grapeSet in grapeSets:
        data = grapeSet.find('li a')
        alpha = alphas[grapeSets.index(grapeSet)]
        for item in data:
            grape_names = splitGrapeText(item.text.strip())
            grape = {
                'name': grape_names[0],
                'en_name': grape_names[1],
                'alpha': alpha,
            }
            persisGraphVariety(grape, connection)


def persisGraphVariety(grape, connection):
    with connection.cursor() as cursor:
        sql = """insert into b_grape_variety(name,en_name,alpha)
        values(%s,%s,%s)
        """
        cursor.execute(sql, (grape['name'], grape['en_name'], grape['alpha']) )
        connection.commit()

session = HTMLSession()
connection = pymysql.connect(host='172.19.0.4', user='root', password='123456', db='buy', charset='utf8', cursorclass=pymysql.cursors.DictCursor)
try:
    requestGrapeVariety( session, connection)
finally:
    connection.close()
    print('complete')
