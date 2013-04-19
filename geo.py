#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.internet import reactor, defer, task
from twisted.python import log
from twisted.enterprise import adbapi

import requests
import json
import sys

from random import choice

provider = ['yandex', 'google', 'microsoft']

host = 'localhost'
database = 'db'
user = 'login'
password = 'password'

provider_setting = {
    'google': {
        'format':'SET g_coord',
        'backend': 'get_google',
        },
    'yandex': {
        'format': 'SET y_coord',
        'backend': 'get_yandex',
        },
    'microsoft': {
        'format': 'SET m_coord',
        'backend': 'get_microsoft',
        },
}

def format_set(provider):
    return 

class Geo(object):

    def __init__(self):
        self.dbpool = adbapi.ConnectionPool("psycopg2", host, database, user,  password)
        log.msg("Открываем подключение к БД", system="Database")

    def get_unparsed(self):

        def get_geo(dObject, provider):

            def get_yandex(address):
                payload = {'format': 'json', 'geocode': address}
                r = requests.get('http://geocode-maps.yandex.ru/1.x/', params=payload)
                result = json.loads(r.text)
                lng, lat = result['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos'].split(" ")
                return {'lat': lat, 'lng': lng}

            def get_google(address):
                payload = {'address': address, 'sensor': 'false'}
                r = requests.get('http://maps.googleapis.com/maps/api/geocode/json', params=payload)
                result = json.loads(r.text)
                if (result['status'] == 'OK'):
                    return result['results'][0]['geometry']['location']
                else:
                    log.msg(result['status'])

            def get_microsoft(address):
                payload = {
                    'q': address,
                    'key': 'AszOE9xoyZZPFksQ9IK5JqA_RMHQDW3f-wehY-0luErkU_Qhs5ohGzbBarKyohzM',
                    'o': 'json'
                }
                r = requests.get('http://dev.virtualearth.net/REST/v1/Locations', params=payload)
                result = json.loads(r.text)
                if result['authenticationResultCode'] == 'ValidCredentials':
                    if len(result['resourceSets']) > 0:
                        if len(result['resourceSets'][0]['resources']) >0:
                            lat, lng = result['resourceSets'][0]['resources'][0]['point']['coordinates']
                            return {'lat': lat, 'lng': lng}
                else:
                    log.msg(result['authenticationResultCode'])

            if dObject is None:
                log.msg("Не был выбран адрес для получения координат")
                log.msg("Кончились свободные zipcode")
                self.dbpool.close()
                reactor.stop()

            id, gorod, street, dom = dObject[0]

            log.msg("Обрабатываем запись %s" % id)

            log.msg(gorod + ", " + street + " " + dom)

            if provider == 'yandex':
                y_coords = get_yandex(gorod + ", " + street + " " + dom)
                log.msg("Провайдер %s вернул координаты %s" % (provider, y_coords))
                return {'id': id, 'coords': y_coords}
            elif provider == 'google':
                g_coords = get_google(gorod + ", " + street + " " + dom)
                log.msg("Провайдер %s вернул координаты %s" % (provider, g_coords))
                return {'id': id, 'coords': g_coords}
            elif provider == 'microsoft':
                m_coords = get_microsoft(street + " " + dom + ", " + gorod)
                log.msg("Провайдер %s вернул координаты %s" % (provider, m_coords))
                return {'id': id, 'coords': m_coords}
            else:
                log.msg("Geo-провайдер не определен");
                return defer.fail

        def put_parsed(coord_object, provider):

            def insertError(dObject):
                log.err('При вставке данных произошла ошибка', system="Database insert")
                log.err(dObject, system="Database insert")
                return defer.fail

            def finishInsert(dObject):
                log.msg( 'Данные успешно вставлены!', system="Database insert")
                return defer.succeed

            if coord_object is None:
                log.msg("Провайдер не вернул резульатат")
                return None

            if not (coord_object['coords'] is None):
                y_lat, y_lng = (float(coord_object['coords']['lat']), float(coord_object['coords']['lng']))
                QUERY = "UPDATE address %s = point(%s, %s), date_update=NOW() WHERE id = %s" % (provider_setting[provider]['format'], y_lat, y_lng, coord_object['id'])
                dOperation = self.dbpool.runOperation(QUERY)
                log.msg("Кладем данные %s в базу данных" % provider, system="Database insert")
                dOperation.addCallback(finishInsert)
                dOperation.addErrback(insertError)

        current_provider = choice(provider)

        dOperation = self.dbpool.runQuery("SELECT id, gorod, street, dom FROM address WHERE y_coord IS NULL AND m_coord IS NULL AND g_coord IS NULL ORDER BY zipcode, street, dom LIMIT 1")
        log.msg("Берем данные из БД", system="Database select")
        
        dOperation.addCallback(get_geo, current_provider)
        dOperation.addErrback(log.msg)
        
        dOperation.addCallback(put_parsed, current_provider)
        dOperation.addErrback(log.msg)



if __name__ == '__main__':
    log.startLogging(sys.stdout)
    l = task.LoopingCall(Geo().get_unparsed)
    l.start(0.3) # call every half-second
    reactor.run()
