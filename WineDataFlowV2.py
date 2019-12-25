#!/bin/python
# get the chateau
# @author strong
# @email 1025155365@qq.com
from requests_html import HTMLSession
import pymysql.cursors

class WineDataFlowV2:

    def __init__(self, connection,session):
        self._session = session
        self._connection = connection
        self.producing_areas = []


    def getAllChateau(self):
        """ get all chateau"""
        page = 1
        root = {
                'link': 'https://www.wine-world.com/winery/area',
                'layer_ids': '',
                'parent_id': '',
                'grade': 0,
                'id':0
               }
        while True:
            if self.__getChateauByPage(page, root):
                page = page + 1
            else:
                break


    def getChateau(self, parent_chateau):
        """ get all chateau of area"""
        page = 1
        while True:
            if self.__getChateauByPage(page, parent_chateau):
                page = page + 1
            else:
                break


    def __getChateauByPage(self, page, parent_chateau):
        finalUrl = parent_chateau['link'] + '/p' + str(page)
        resp = self._session.get(finalUrl)

        lis = resp.html.find('.sch-kng ul li a p span.cname')

        if lis:
            for li in lis:
                area_names = self.__splitAreaName(li.text.strip(), ' ')
                if area_names:
                    layer_ids = '0'
                    if parent_chateau['layer_ids']:
                        layer_ids = parent_chateau['layer_ids'] + '_' + str(parent_chateau['id'])

                    chateau = {
                            'name': area_names[0],
                            'en_name': area_names[1],
                            'type': 2,
                            'child_num': 0,
                            'layer_ids': layer_ids,
                            'parent_id': parent_chateau['id'],
                            'grade': parent_chateau['grade']+1,
                            }
                    self.__insertChateau(chateau)
                else:
                    print(li.text.strip())
            return True
        else:
            return False

    def getCountries(self):
        resp = self._session.get('https://www.wine-world.com/winery/area')
        lis = resp.html.find('.wineRegion2 li a')
        if lis:
            countries = []
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
                    if producing_area:
                        countries.append(producing_area)

            return countries
        return False

    def getProducingAreas(self, parent_area):
        print(parent_area)
        resp = self._session.get(parent_area['link'])
        lis = resp.html.find('.wineRegion2 li a')
        links = []
        if lis: 
            for li in lis:
                links.append(li.attrs['href'])

            if parent_area['link'] in links:
                return False 

            for li in lis:
                area_names = self.__splitAreaName(li.attrs['title'], '  ')
                if area_names:
                    area_type = 1
                    layer_ids = parent_area['layer_ids'] + '_' + str(parent_area['id'])
                    parent_id = parent_area['id']

                    chateau = {
                            'name': area_names[0],
                            'en_name': area_names[1],
                            'type': area_type,
                            'child_num': 0,
                            'layer_ids': layer_ids,
                            'parent_id': parent_id,
                            'grade': parent_area['grade'] + 1,
                            'link': li.attrs['href']
                            }
                    chateau['child_num'] = self.__splitChildNum(li.text)
                    producing_area = self.__insertChateau(chateau)
                    #self.producing_areas.append(producing_area)
                    if not self.getProducingAreas(chateau):
                        break
        return True

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
            # check exists
            sql = """select * from b_chateau_1 where en_name=%s limit 1"""
            cursor.execute(sql, (chateau['en_name']))
            old_chateau = cursor.fetchone()
            if not old_chateau:
                sql = """insert into b_chateau_1(name,en_name,child_num,parent_id,layer_ids,grade,type,link)
                values(%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(sql, (chateau['name'], chateau['en_name'], chateau['child_num'], chateau['parent_id'],
                    chateau['layer_ids'], chateau['grade'], chateau['type'], chateau['link']))

                connection.commit()
                chateau['id'] = cursor.lastrowid 
                return chateau

            return False

try:
    connection = pymysql.connect(host='172.19.0.4', user='root', password='123456', 
            db='buy', charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    session = HTMLSession()
    wineDataFlow = WineDataFlowV2(connection, session)
    #wineDataFlow.getAllChateau()
    countries = wineDataFlow.getCountries()
    if countries:
        for country in countries:
            wineDataFlow.getProducingAreas(country)

finally:
    connection.close()


