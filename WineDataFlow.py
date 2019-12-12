#!/bin/python
# get the chateau
# @author strong
# @email 1025155365@qq.com
from requests_html import HTMLSession
import pymysql.cursors

class WineDataFlow:

    def __init__(self, connection,session):
        self._session = session
        self._connection = connection
        
    def getChateau(self):
        """ get all chateau """
        page = 1
        countries = None
        while True:
            rets = self.__getChateauByPage(page, 'https://www.wine-world.com/winery/area')
            if  page == 1:
                countries = rets
            if  rets:
                page = page + 1
            else:
                break
        
        producing_areas = []
        for country in countries:
            producing_areas_tmp = self.__getCountries(resp, parent_area = country)
            producing_areas.append(x for producting_area in producing_areas_tmp)
        

    def __getChateauByPage(self, page, url):
        finalUrl = url + '/P' + str(page)
        resp = self._session.get(finalUrl)

        countries = None
        if page == 1:
            # get country
            countries = self.__getCountries(resp, type = 'country')
            if not countries:
                print('get countries failed!')
                return False
            

        lis = resp.html.find('.sch-kng ul li a p span.cname')
        
        if lis:
            for li in lis:
                area_names = self.__splitAreaName(li.text.strip(), ' ')
                if area_names:
                    chateau = {
                        'name': area_names[0],
                        'en_name': area_names[1],
                        'type': 2,
                        'child_num': 0,
                        'layer_ids':'0',
                        'parent_id':0,
                        'grade': 0,
                        }
                    self.__insertChateau(chateau)
            if countries:
                return countries
            return True
        else:
            return False
            
    def __getCountries(self, resp):
        lis = resp.html.find('.wineRegion2 li a')
        if lis:
            producing_areas = []
            for li in lis:
                area_names = self.__splitAreaName(li.attrs['title'], '  ')
                if area_names:
                    area_type = 0
                    layer_ids = '0'
                    parent_id = 0

                    chateau = {
                        'name': area_names[0],
                        'en_name': area_names[1],
                        'type': area_type,
                        'child_num': 0,
                        'layer_ids': layer_ids,
                        'parent_id': parent_id,
                        'grade': 0,
                        'link': li.attrs['href']
                        }
                    chateau['child_num'] = self.__splitChildNum(li.text)
                    producing_area = self.__insertChateau(chateau)
                    producing_areas.append(producing_area)

            return producing_areas
        return False
        
    def __getProducingAreas(self, parent_area, type = 'producing_areas'):
        resp = self._session.get(parent_area['link'])
        lis = resp.html.find('.wineRegion2 li a')
        if lis:
            producing_areas = []
            for li in lis:
                area_names = self.__splitAreaName(li.attrs['title'], '  ')
                if area_names:
                    area_type = 1
                    layer_ids = parent_area['layer_ids'] + str(parent_area['id'])
                    parent_id = parent_area['id']

                    chateau = {
                        'name': area_names[0],
                        'en_name': area_names[1],
                        'type': area_type,
                        'child_num': 0,
                        'layer_ids': layer_ids,
                        'parent_id': parent_id,
                        'grade': 0,
                        'link': li.attrs['href']
                        }
                    chateau['child_num'] = self.__splitChildNum(li.text)
                    producing_area = self.__insertChateau(chateau)
                    producing_areas.append(producing_area)

            return producing_areas
        return False

    def __splitAreaName(self, area_text, split=' '):
        """split the string to name and en_name"""
        try:
            index = area_text.index(split)
            name = area_text[:index]
            en_name = area_text[index + len(split):]
            return [name, en_name]
        except ValueError:
            return False

    def __splitChildNum(self,text):
        """ split the string to get child num"""
        try:
            startI = text.index('(')
            endI = text.index(')')
            child_num = text[startI + 1:endI]
            return child_num
        except ValueError:
            return False

    def __insertChateau(self, chateau):
        """add chateau to database"""
        with connection.cursor() as cursor:
           sql = """insert into b_chateau(name,en_name,child_num,parent_id,layer_ids,grade,type)
           values(%s,%s,%s,%s,%s,%s,%s)
           """
           cursor.execute(sql, (chateau['name'], chateau['en_name'], chateau['child_num'], chateau['parent_id'],
               chateau['layer_ids'], chateau['grade'], chateau['type']))
           
           connection.commit()
           chateau['id'] = cursor.lastrowid 
           return chateau
        

try:
    connection = pymysql.connect(host='172.19.0.4', user='root', password='123456', 
                    db='buy', charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    session = HTMLSession()
    wineDataFlow = WineDataFlow(connection, session)
    wineDataFlow.getChateau()

finally:
    connection.close()


