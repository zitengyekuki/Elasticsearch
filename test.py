#!/usr/bin/python
#-*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import datetime
import mysql.connector
from elasticsearch import helpers
import time


def search():
    es_search_options = set_search_optional()
    es_result = get_search_result(es_search_options)
    final_result = get_result_list(es_result)
    return final_result


def get_result_list(es_result):
    final_result = []
    for item in es_result:
        final_result.append(item['_source'])
    return final_result


def set_search_optional():
    # 检索选项
    es_search_options = {
        "query": {
            "match": {
                "event_type": "/other/OS1N5DF10129"
            }
        }
    }
    # es_search_options = {
    #     "query": {
    #         "match_all": {}
    #     }
    # }
    return es_search_options


def get_search_result(es_search_options, scroll='5m', doc_type='event'):
    es_result = helpers.scan(
        client=es,
        query=es_search_options,
        scroll=scroll,
        doc_type=doc_type
    )
    return es_result

if __name__ == '__main__':
    print '----------starting test----------'
    # INDEX = '_all'
    TYPE = 'event'
    # HOST = '172.16.100.81'
    HOST = '10.125.192.246'
    TIME_GAP_DAYS = 210
    WARNING_AVERAGE_SEARCH_COUNT = 20
    WARNING_AVERAGE_SEARCH_TIME_GAP = 4
    SAMPLE_TIME = 14

    es = Elasticsearch(host=HOST, port=9200)
    start_time = (datetime.datetime.now() - datetime.timedelta(days=TIME_GAP_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    last_week = (datetime.datetime.now() - datetime.timedelta(days=SAMPLE_TIME)).strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    datas = search()
    print len(datas)
    ip2last2MonthsData = {}
    ip2last7DaysData = {}
    """
    ip2last2MonthsData = {
        ip: [time_str]
    }
    """
    if datas:
        for data in datas:
            DLIP = data['src_address']  # ip
            original_log = data['original_log']  # time
            CXSJ = original_log.split('CXSJ=')[1].split(',')[0]
            if DLIP not in ip2last2MonthsData:
                ip2last2MonthsData[DLIP] = [CXSJ]
            else:
                ip2last2MonthsData[DLIP].append(CXSJ)

            # if it's in this week
            if last_week <= CXSJ <= today:
                if DLIP not in ip2last7DaysData:
                    ip2last7DaysData[DLIP] = [CXSJ]
                else:
                    ip2last7DaysData[DLIP].append(CXSJ)

    warning_list_for_searching_counts = []
    warning_list_for_searching_time = []
    warning_list_for_searching_time_detail = {}
    warning_list_for_searching_un_working_time = []
    warning_list_for_searching_un_working_time_detail = {}
    for ip in ip2last7DaysData:
        # Find unusual average searching number
        search_number_in_last_7_days = float(len(ip2last7DaysData[ip]))
        search_number_in_last_60_days = float(len(ip2last2MonthsData[ip]))
        average_count_last_week = round(search_number_in_last_7_days/SAMPLE_TIME, 3)
        average_count = round(search_number_in_last_60_days/TIME_GAP_DAYS, 3)
        if (average_count_last_week - average_count) > WARNING_AVERAGE_SEARCH_COUNT:
            warning_list_for_searching_counts.append(ip)

        # Find unusual searching time
        total_minutes = 0
        average_searching_hour = 0
        working_time_searching_amount = 0
        un_working_time_searching_amount = 0
        for time in ip2last2MonthsData[ip]:
            # Find un-working time searching amount in the past 2 months
            time_str = time.split()[1]
            if '08:30:00' < time_str < '12:00:00' or '14:00:00' < time_str < '18:00:00':
                working_time_searching_amount += 1
            else:
                un_working_time_searching_amount += 1

            hour, minute, second = time_str.split(':')
            total_minutes = total_minutes + int(hour)*60 + int(minute)

        if un_working_time_searching_amount > working_time_searching_amount:
            warning_list_for_searching_un_working_time.append(ip)
            warning_list_for_searching_un_working_time_detail[ip] = [working_time_searching_amount, un_working_time_searching_amount]

        # Find unusual searching time
        average_searching_hour = (total_minutes/search_number_in_last_60_days)/60
        total_minutes_last_week = 0
        average_searching_hour_last_week = 0

        for time in ip2last7DaysData[ip]:
            hour, minute, second = time.split()[1].split(':')
            total_minutes_last_week = total_minutes_last_week + int(hour) * 60 + int(minute)
        average_searching_hour_last_week = (total_minutes_last_week / search_number_in_last_7_days) / 60
        if (average_searching_hour_last_week - average_searching_hour) > WARNING_AVERAGE_SEARCH_TIME_GAP:
            warning_list_for_searching_time.append(ip)
            warning_list_for_searching_time_detail[ip] = [average_searching_hour, average_searching_hour_last_week]

    print '[RESULTS]warning_list_for_searching_counts:'
    print warning_list_for_searching_counts
    print '[RESULTS]warning_list_for_searching_time:'
    print warning_list_for_searching_time
    print '[RESULTS]warning_list_for_searching_un_working_time:'
    print warning_list_for_searching_un_working_time

    try:
        con = mysql.connector.connect(host=HOST, port=3399, user='hansight',
                                      password='hansight', database='hansight', charset='utf8')
        cursor = con.cursor()
        createSql_1 = 'create table if not exists warning_list_for_searching_counts(id int(32) primary key auto_increment, ip varchar(32), total_count int(32), recent_count int(32), time_data varchar(64))'
        createSql_2 = 'create table if not exists warning_list_for_searching_time(id int(32) primary key auto_increment, ip varchar(32), total_count int(32), recent_count int(32), average_time varchar(32), recent_average_time varchar(32), time_data varchar(64))'
        createSql_3 = 'create table if not exists warning_list_for_searching_un_working_time(id int(32) primary key auto_increment, ip varchar(32), working_time_search_count int(32), un_working_time_search_count int(32), time_data varchar(64))'

        cursor.execute(createSql_1)
        cursor.execute(createSql_2)
        cursor.execute(createSql_3)
        for ip in warning_list_for_searching_counts:
            total_count = len(ip2last2MonthsData[ip])
            recent_count = len(ip2last7DaysData[ip])
            sql = "insert into warning_list_for_searching_counts values(%s, %s, %s, %s, %s)"
            cursor.execute(sql, ['null', ip, total_count, recent_count, today])

        for ip in warning_list_for_searching_time:
            total_count = len(ip2last2MonthsData[ip])
            recent_count = len(ip2last7DaysData[ip])
            average_time = str(warning_list_for_searching_time_detail[ip][0])
            recent_average_time = str(warning_list_for_searching_time_detail[ip][1])
            sql = "insert into warning_list_for_searching_time values(%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, ['null', ip, total_count, recent_count, average_time, recent_average_time, today])

        for ip in warning_list_for_searching_un_working_time:
            working_time_search_count = str(warning_list_for_searching_un_working_time_detail[ip][0])
            un_working_time_search_count = str(warning_list_for_searching_un_working_time_detail[ip][1])
            sql = "insert into warning_list_for_searching_un_working_time values(%s, %s, %s, %s, %s)"
            cursor.execute(sql, ['null', ip, working_time_search_count, un_working_time_search_count, today])

        con.commit()
        cursor.close()
        con.close()
    except mysql.connector.Error, e:
        print e.message