#-*- coding: UTF-8 -*-
#__author__ = ''
#!/usr/bin/env python
#

import tornado.web
from base import *
from datetime import datetime, timedelta, date
from time import mktime,strptime

class RhAnalyzeHourHandler(BaseHandler):

    def get(self, template_variables={}):
        #当前时间上一个小时的时间戳区间
        now = datetime.now()
        last_hour = now - timedelta(hours=1)
        last_hour_start = last_hour.replace(minute=0, second=0, microsecond=0)
        start_timestamp = int(mktime(strptime(str(last_hour_start), '%Y-%m-%d %H:%M:%S')))
        end_timestamp = start_timestamp + 1*60*60

        start = start_timestamp
        end = end_timestamp

        #获取app_id和渠道
        device_app_id_channel_list = self.rh_stats_device_model.get_rh_app_id_channel_list(start=start, end=end)
        user_app_id_channel_list = self.rh_stats_user_app_model.get_rh_app_id_channel_list(start=start, end=end)
        login_app_id_channel_list = self.rh_stats_user_login_log_model.get_rh_app_id_channel_list(start=start, end=end)
        #合并所有的渠道
        app_id_channel_list = []
        app_id_channel_list_all = device_app_id_channel_list + user_app_id_channel_list + login_app_id_channel_list

        for app_c in app_id_channel_list_all:
            if app_c not in app_id_channel_list:
                app_id_channel_list.append(app_c)

        for app_channel in app_id_channel_list:
            app_id = app_channel[0]
            channel = app_channel[1]

            #拼接唯一文档ID
            document_id = last_hour_start.strftime("%Y%m%d%H") + "_" + "H" + "_" + str(app_id) + "_" + channel
            #抓出小时的统计
            new_user = self.rh_stats_user_app_model.analyze_new_user(app_id=app_id, start=start, end=end, channel=channel)
            new_device = self.rh_stats_device_model.analyze_new_device(app_id=app_id, start=start, end=end, channel=channel)
            new_role = self.rh_stats_user_app_model.analyze_new_role(app_id=app_id, start=start, end=end, channel=channel)
            act_user = self.rh_stats_user_login_log_model.analyze_act_user(app_id=app_id, start=start, end=end, channel=channel)
            act_device = self.rh_stats_device_model.analyze_act_device(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = self.rh_stats_pay_model.analyze_pay_ammount(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = pay_ammount['pay_money']
            pay_count = self.rh_stats_pay_model.analyze_pay_count(app_id=app_id, start=start, end=end, channel=channel)
            pay_count = int(len(pay_count))
            #保存小时统计到ES
            self.rh_analyze_kpi_model.add_hour_data(
                    new_user = new_user,
                    new_device = new_device,
                    new_role = new_role,
                    act_user = act_user,
                    act_device = act_device,
                    pay_ammount=pay_ammount,
                    pay_count=pay_count,
                    id = document_id,
                    app_id = app_id,
                    channel = channel,
                    day = last_hour_start.day,
                    year = last_hour_start.year,
                    month = last_hour_start.month,
                    hour = last_hour_start.hour,
                    time_str = last_hour_start.strftime("%Y-%m-%d")
            )
        self.analyze_today()

    def analyze_today(self):
        #今天时间戳区间
        today = datetime.now() - timedelta(hours=1)
        today_start = today.replace(minute=0, second=0, microsecond=0, hour=0)
        start_timestamp = (today_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
        end_timestamp = start_timestamp + 24*60*60

        start = start_timestamp
        end = end_timestamp

        #获取app_id和渠道
        device_app_id_channel_list = self.rh_stats_device_model.get_rh_app_id_channel_list(start=start, end=end)
        user_app_id_channel_list = self.rh_stats_user_app_model.get_rh_app_id_channel_list(start=start, end=end)
        login_app_id_channel_list = self.rh_stats_user_login_log_model.get_rh_app_id_channel_list(start=start, end=end)
        #合并所有的渠道
        app_id_channel_list = []
        app_id_channel_list_all = device_app_id_channel_list + user_app_id_channel_list + login_app_id_channel_list
        for app_c in app_id_channel_list_all:
            if app_c not in app_id_channel_list:
                app_id_channel_list.append(app_c)

        for app_channel in app_id_channel_list:
            app_id = app_channel[0]
            channel = app_channel[1]

            #拼接唯一文档ID
            document_id = today_start.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            #抓出天的统计
            new_user = self.rh_stats_user_app_model.analyze_new_user(app_id=app_id, start=start, end=end, channel=channel)
            new_device = self.rh_stats_device_model.analyze_new_device(app_id=app_id, start=start, end=end, channel=channel)
            new_role = self.rh_stats_user_app_model.analyze_new_role(app_id=app_id, start=start, end=end, channel=channel)
            act_user = self.rh_stats_user_login_log_model.analyze_act_user(app_id=app_id, start=start, end=end, channel=channel)
            act_device = self.rh_stats_device_model.analyze_act_device(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = self.rh_stats_pay_model.analyze_pay_ammount(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = pay_ammount['pay_money']
            pay_count = self.rh_stats_pay_model.analyze_pay_count(app_id=app_id, start=start, end=end, channel=channel)
            pay_count = int(len(pay_count))

            #当天
            self.rh_analyze_kpi_model.add_day_data(
                    new_user=new_user,
                    new_device=new_device,
                    new_role=new_role,
                    act_user=act_user,
                    act_device=act_device,
                    pay_ammount=pay_ammount,
                    pay_count=pay_count,
                    id=document_id,
                    app_id=app_id,
                    channel=channel,
                    day=today_start.day,
                    year=today_start.year,
                    month=today_start.month,
                    hour=today_start.hour,
                    time_str=today_start.strftime("%Y-%m-%d"),
                    keep_fourteen=0,
                    keep_next=0,
                    keep_seven=0,
                    keep_third=0,
                    keep_thirty=0,
                    keep_2=0,
                    keep_4=0,
                    keep_5=0,
                    keep_6=0,
                    keep_8=0,
                    keep_10=0,
                    keep_12=0,
                    keep_16=0,
                    keep_20=0,
            )

            #留存计算
            #当天的活跃
            act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=start, end=end, channel=channel)
            #昨日新增
            next_day = today - timedelta(days=1)
            next_day_start = next_day.replace(minute=0, second=0, microsecond=0, hour=0)
            next_day_start_timestamp = (next_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            next_day_end_timestamp = next_day_start_timestamp + 24*60*60
            next_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=next_day_start_timestamp, end=next_day_end_timestamp, channel=channel)
            next_day_num = len(next_new_reg_user_id_list & act_user_id_list)

            next_day_document_id = next_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_next_day_data(keep_next=next_day_num, id=next_day_document_id)

class RhAnalyzeDayHandler(BaseHandler):

    def get(self, template_variables={}):
        #昨天时间戳区间
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday_start = yesterday.replace(minute=0, second=0, microsecond=0, hour=0)
        start_timestamp = (yesterday_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
        end_timestamp = start_timestamp + 24*60*60

        start = start_timestamp
        end = end_timestamp

        #获取app_id和渠道
        device_app_id_channel_list = self.rh_stats_device_model.get_rh_app_id_channel_list(start=start, end=end)
        user_app_id_channel_list = self.rh_stats_user_app_model.get_rh_app_id_channel_list(start=start, end=end)
        login_app_id_channel_list = self.rh_stats_user_login_log_model.get_rh_app_id_channel_list(start=start, end=end)
        #合并所有的渠道
        app_id_channel_list = []
        app_id_channel_list_all = device_app_id_channel_list + user_app_id_channel_list + login_app_id_channel_list
        for app_c in app_id_channel_list_all:
            if app_c not in app_id_channel_list:
                app_id_channel_list.append(app_c)

        for app_channel in app_id_channel_list:
            app_id = app_channel[0]
            channel = app_channel[1]

            #拼接唯一文档ID
            document_id = yesterday_start.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            #抓出昨天的统计
            new_user = self.rh_stats_user_app_model.analyze_new_user(app_id=app_id, start=start, end=end, channel=channel)
            new_device = self.rh_stats_device_model.analyze_new_device(app_id=app_id, start=start, end=end, channel=channel)
            new_role = self.rh_stats_user_app_model.analyze_new_role(app_id=app_id, start=start, end=end, channel=channel)
            act_user = self.rh_stats_user_login_log_model.analyze_act_user(app_id=app_id, start=start, end=end, channel=channel)
            act_device = self.rh_stats_device_model.analyze_act_device(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = self.rh_stats_pay_model.analyze_pay_ammount(app_id=app_id, start=start, end=end, channel=channel)
            pay_ammount = pay_ammount['pay_money']
            pay_count = self.rh_stats_pay_model.analyze_pay_count(app_id=app_id, start=start, end=end, channel=channel)
            pay_count = int(len(pay_count))

            #当天
            self.rh_analyze_kpi_model.add_day_data(new_user=new_user,
                                               new_device=new_device,
                                               new_role=new_role,
                                               act_user=act_user,
                                               act_device=act_device,
                                               pay_ammount=pay_ammount,
                                               pay_count=pay_count,
                                               id=document_id,
                                               app_id=app_id,
                                               channel=channel,
                                               day=yesterday_start.day,
                                               year=yesterday_start.year,
                                               month=yesterday_start.month,
                                               hour=yesterday_start.hour,
                                               time_str=yesterday_start.strftime("%Y-%m-%d"),
                                               keep_fourteen=0,
                                               keep_next=0,
                                               keep_seven=0,
                                               keep_third=0,
                                               keep_thirty=0,
                                               keep_2=0,
                                               keep_4=0,
                                               keep_5=0,
                                               keep_6=0,
                                               keep_8=0,
                                               keep_10=0,
                                               keep_12=0,
                                               keep_16=0,
                                               keep_20=0,
                                               )

            #留存计算
            #当天的活跃
            act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=start, end=end, channel=channel)
            #次日
            now = datetime.now()
            next_day = now - timedelta(days=2)
            next_day_start = next_day.replace(minute=0, second=0, microsecond=0, hour=0)
            next_day_start_timestamp = (next_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            next_day_end_timestamp = next_day_start_timestamp + 24*60*60
            next_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=next_day_start_timestamp, end=next_day_end_timestamp, channel=channel)
            next_day_num = len(next_new_reg_user_id_list & act_user_id_list)

            next_day_document_id = next_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_next_day_data(keep_next=next_day_num, id=next_day_document_id)

            #2日
            two_day = now - timedelta(days=3)
            two_day_start = two_day.replace(minute=0, second=0, microsecond=0, hour=0)
            two_day_start_timestamp = (two_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            two_day_end_timestamp = two_day_start_timestamp + 24*60*60
            two_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=two_day_start_timestamp, end=two_day_end_timestamp, channel=channel)
            two_day_num = len(two_new_reg_user_id_list & act_user_id_list)

            two_day_document_id = two_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_two_day_data(keep_two=two_day_num, id=two_day_document_id)

            #三日
            third_day = now - timedelta(days=4)
            third_day_start = third_day.replace(minute=0, second=0, microsecond=0, hour=0)
            third_day_start_timestamp = (third_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            third_day_end_timestamp = third_day_start_timestamp + 24*60*60
            third_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=third_day_start_timestamp, end=third_day_end_timestamp, channel=channel)
            third_day_num = len(third_reg_user_id_list & act_user_id_list)

            third_day_document_id = third_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_third_day_data(keep_third=third_day_num, id=third_day_document_id)

            #4日
            four_day = now - timedelta(days=5)
            four_day_start = four_day.replace(minute=0, second=0, microsecond=0, hour=0)
            four_day_start_timestamp = (four_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            four_day_end_timestamp = four_day_start_timestamp + 24*60*60
            four_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=four_day_start_timestamp, end=four_day_end_timestamp, channel=channel)
            four_day_num = len(four_new_reg_user_id_list & act_user_id_list)

            four_day_document_id = four_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_four_day_data(keep_four=four_day_num, id=four_day_document_id)

            #5日
            five_day = now - timedelta(days=6)
            five_day_start = five_day.replace(minute=0, second=0, microsecond=0, hour=0)
            five_day_start_timestamp = (five_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            five_day_end_timestamp = five_day_start_timestamp + 24*60*60
            five_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=five_day_start_timestamp, end=five_day_end_timestamp, channel=channel)
            five_day_num = len(five_new_reg_user_id_list & act_user_id_list)

            five_day_document_id = five_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_five_day_data(keep_five=five_day_num, id=five_day_document_id)

            #6日
            six_day = now - timedelta(days=7)
            six_day_start = six_day.replace(minute=0, second=0, microsecond=0, hour=0)
            six_day_start_timestamp = (six_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            six_day_end_timestamp = six_day_start_timestamp + 24*60*60
            six_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=six_day_start_timestamp, end=six_day_end_timestamp, channel=channel)
            six_day_num = len(six_new_reg_user_id_list & act_user_id_list)

            six_day_document_id = six_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_six_day_data(keep_six=six_day_num, id=six_day_document_id)

            #七日
            seventh_day = now - timedelta(days=8)
            seventh_day_start = seventh_day.replace(minute=0, second=0, microsecond=0, hour=0)
            seventh_day_start_timestamp = (seventh_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            seventh_day_end_timestamp = seventh_day_start_timestamp + 24*60*60
            seventh_day_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=seventh_day_start_timestamp, end=seventh_day_end_timestamp, channel=channel)
            seventh_day_num = len(seventh_day_reg_user_id_list & act_user_id_list)

            seventh_day_document_id = seventh_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_seven_day_data(keep_seven=seventh_day_num, id=seventh_day_document_id)

            #8日
            eight_day = now - timedelta(days=9)
            eight_day_start = eight_day.replace(minute=0, second=0, microsecond=0, hour=0)
            eight_day_start_timestamp = (eight_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            eight_day_end_timestamp = eight_day_start_timestamp + 24*60*60
            eight_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=eight_day_start_timestamp, end=eight_day_end_timestamp, channel=channel)
            eight_day_num = len(eight_new_reg_user_id_list & act_user_id_list)

            eight_day_document_id = eight_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_eight_day_data(keep_eight=eight_day_num, id=eight_day_document_id)

            #10日
            ten_day = now - timedelta(days=11)
            ten_day_start = ten_day.replace(minute=0, second=0, microsecond=0, hour=0)
            ten_day_start_timestamp = (ten_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            ten_day_end_timestamp = ten_day_start_timestamp + 24*60*60
            ten_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=ten_day_start_timestamp, end=ten_day_end_timestamp, channel=channel)
            ten_day_num = len(ten_new_reg_user_id_list & act_user_id_list)

            ten_day_document_id = ten_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_ten_day_data(keep_ten=ten_day_num, id=ten_day_document_id)

            #12日
            twelve_day = now - timedelta(days=13)
            twelve_day_start = twelve_day.replace(minute=0, second=0, microsecond=0, hour=0)
            twelve_day_start_timestamp = (twelve_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            twelve_day_end_timestamp = twelve_day_start_timestamp + 24*60*60
            twelve_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=twelve_day_start_timestamp, end=twelve_day_end_timestamp, channel=channel)
            twelve_day_num = len(twelve_new_reg_user_id_list & act_user_id_list)

            twelve_day_document_id = twelve_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_twelve_day_data(keep_twelve=twelve_day_num, id=twelve_day_document_id)

            #14日
            fourteen_day = now - timedelta(days=15)
            fourteen_day_start = fourteen_day.replace(minute=0, second=0, microsecond=0, hour=0)
            fourteen_day_start_timestamp = (fourteen_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            fourteen_day_end_timestamp = fourteen_day_start_timestamp + 24*60*60
            fourteen_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=fourteen_day_start_timestamp, end=fourteen_day_end_timestamp, channel=channel)
            fourteen_day_num = len(fourteen_reg_user_id_list & act_user_id_list)

            fourteen_day_document_id = fourteen_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_fourteen_day_data(keep_fourteen=fourteen_day_num, id=fourteen_day_document_id)

            #16日
            sixteen_day = now - timedelta(days=17)
            sixteen_day_start = sixteen_day.replace(minute=0, second=0, microsecond=0, hour=0)
            sixteen_day_start_timestamp = (sixteen_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            sixteen_day_end_timestamp = sixteen_day_start_timestamp + 24*60*60
            sixteen_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=sixteen_day_start_timestamp, end=sixteen_day_end_timestamp, channel=channel)
            sixteen_day_num = len(sixteen_new_reg_user_id_list & act_user_id_list)

            sixteen_day_document_id = sixteen_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_sixteen_day_data(keep_sixteen=sixteen_day_num, id=sixteen_day_document_id)

            #20日
            twenty_day = now - timedelta(days=21)
            twenty_day_start = twenty_day.replace(minute=0, second=0, microsecond=0, hour=0)
            twenty_day_start_timestamp = (twenty_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            twenty_day_end_timestamp = twenty_day_start_timestamp + 24*60*60
            twenty_new_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=twenty_day_start_timestamp, end=twenty_day_end_timestamp, channel=channel)
            twenty_day_num = len(twenty_new_reg_user_id_list & act_user_id_list)

            twenty_day_document_id = twenty_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_twenty_day_data(keep_twenty=twenty_day_num, id=twenty_day_document_id)

            #30日
            thirty_day = now - timedelta(days=31)
            thirty_day_start = thirty_day.replace(minute=0, second=0, microsecond=0, hour=0)
            thirty_day_start_timestamp = (thirty_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
            thirty_day_end_timestamp = next_day_start_timestamp + 24*60*60
            thirty_reg_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=thirty_day_start_timestamp, end=thirty_day_end_timestamp, channel=channel)
            thirty_day_num = len(thirty_reg_user_id_list & act_user_id_list)

            thirty_day_document_id = thirty_day.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

            self.rh_analyze_kpi_model.add_thirty_day_data(keep_thirty=thirty_day_num, id=thirty_day_document_id)


class RhAnalyzeOldDataHandler(BaseHandler):

    def get(self, template_variables={}):

        #指定日期
        # start_date = datetime(year=2017, month=5, day=1, hour=0, minute=0, second=0, microsecond=0)
        start_date = datetime(year=2018, month=9, day=27, hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime(year=2018, month=9, day=30, hour=0, minute=0, second=0, microsecond=0)
        start_date_timestamp = (start_date - datetime(1970, 1, 1)).total_seconds() - 8*60*60
        end_date_timestamp = (end_date - datetime(1970, 1, 1)).total_seconds() - 8*60*60

        while start_date_timestamp < end_date_timestamp:
            for h in range(24):
                hour_date = start_date + timedelta(hours=h)
                start = start_date_timestamp + h*60*60
                end = start + 1*60*60

                #获取app_id和渠道
                device_app_id_channel_list = self.rh_stats_device_model.get_rh_app_id_channel_list(start=start, end=end)
                user_app_id_channel_list = self.rh_stats_user_app_model.get_rh_app_id_channel_list(start=start, end=end)
                login_app_id_channel_list = self.rh_stats_user_login_log_model.get_rh_app_id_channel_list(start=start, end=end)
                #合并所有的渠道
                app_id_channel_list = []
                app_id_channel_list_all = device_app_id_channel_list  + user_app_id_channel_list + login_app_id_channel_list
                for app_c in app_id_channel_list_all:
                    if app_c not in app_id_channel_list:
                        app_id_channel_list.append(app_c)

                for app_channel in app_id_channel_list:
                    #
                    app_id = app_channel[0]
                    channel = app_channel[1]

                    #拼接唯一文档ID
                    document_id = hour_date.strftime("%Y%m%d%H") + "_" + "H" + "_" + str(app_id) + "_" + channel
                    #抓出小时的统计
                    new_user = self.rh_stats_user_app_model.analyze_new_user(app_id=app_id, start=start, end=end, channel=channel)
                    new_device = self.rh_stats_device_model.analyze_new_device(app_id=app_id, start=start, end=end, channel=channel)
                    new_role = self.rh_stats_user_app_model.analyze_new_role(app_id=app_id, start=start, end=end, channel=channel)
                    act_user = self.rh_stats_user_login_log_model.analyze_act_user(app_id=app_id, start=start, end=end, channel=channel)
                    act_device = self.rh_stats_device_model.analyze_act_device(app_id=app_id, start=start, end=end, channel=channel)
                    pay_ammount = self.rh_stats_pay_model.analyze_pay_ammount(app_id=app_id, start=start, end=end, channel=channel)
                    pay_ammount = pay_ammount['pay_money']
                    pay_count = self.rh_stats_pay_model.analyze_pay_count(app_id=app_id, start=start, end=end, channel=channel)
                    pay_count = int(len(pay_count))

                    #保存小时统计到ES
                    self.rh_analyze_kpi_model.add_hour_data(new_user=new_user,
                                                       new_device=new_device,
                                                       new_role=new_role,
                                                       act_user=act_user,
                                                       act_device=act_device,
                                                       pay_ammount=pay_ammount,
                                                       pay_count=pay_count,
                                                       id=document_id,
                                                       app_id=app_id,
                                                       channel=channel,
                                                       day=hour_date.day,
                                                       year=hour_date.year,
                                                       month=hour_date.month,
                                                       hour=hour_date.hour,
                                                       time_str=hour_date.strftime("%Y-%m-%d")
                                                       )
            start = start_date_timestamp
            end = start_date_timestamp + 24*60*60
            day_date = start_date

            #天的数据
            #获取app_id和渠道
            device_app_id_channel_list = self.rh_stats_device_model.get_rh_app_id_channel_list(start=start, end=end)
            user_app_id_channel_list = self.rh_stats_user_app_model.get_rh_app_id_channel_list(start=start, end=end)
            login_app_id_channel_list = self.rh_stats_user_login_log_model.get_rh_app_id_channel_list(start=start, end=end)
            #合并所有的渠道
            app_id_channel_list = []
            app_id_channel_list_all = device_app_id_channel_list  + user_app_id_channel_list + login_app_id_channel_list
            for app_c in app_id_channel_list_all:
                if app_c not in app_id_channel_list:
                    app_id_channel_list.append(app_c)

            for app_channel in app_id_channel_list:
                app_id = app_channel[0]
                channel = app_channel[1]

                #拼接唯一文档ID
                document_id = day_date.strftime("%Y%m%d") + "_" + "D" + "_" + str(app_id) + "_" + channel

                #抓出天的统计
                new_user = self.rh_stats_user_app_model.analyze_new_user(app_id=app_id, start=start, end=end, channel=channel)
                new_device = self.rh_stats_device_model.analyze_new_device(app_id=app_id, start=start, end=end, channel=channel)
                new_role = self.rh_stats_user_app_model.analyze_new_role(app_id=app_id, start=start, end=end, channel=channel)
                act_user = self.rh_stats_user_login_log_model.analyze_act_user(app_id=app_id, start=start, end=end, channel=channel)
                act_device = self.rh_stats_device_model.analyze_act_device(app_id=app_id, start=start, end=end, channel=channel)
                pay_ammount = self.rh_stats_pay_model.analyze_pay_ammount(app_id=app_id, start=start, end=end, channel=channel)
                pay_ammount = pay_ammount['pay_money']
                pay_count = self.rh_stats_pay_model.analyze_pay_count(app_id=app_id, start=start, end=end, channel=channel)
                pay_count = int(len(pay_count))

                #当天
                self.rh_analyze_kpi_model.add_day_data(new_user=new_user,
                                                   new_device=new_device,
                                                   new_role=new_role,
                                                   act_user=act_user,
                                                   act_device=act_device,
                                                   pay_ammount=pay_ammount,
                                                   pay_count=pay_count,
                                                   id=document_id,
                                                   app_id=app_id,
                                                   channel=channel,
                                                   day=day_date.day,
                                                   year=day_date.year,
                                                   month=day_date.month,
                                                   hour=day_date.hour,
                                                   time_str=day_date.strftime("%Y-%m-%d"),
                                                   keep_fourteen=0,
                                                   keep_next=0,
                                                   keep_seven=0,
                                                   keep_third=0,
                                                   keep_thirty=0,
                                                   keep_2=0,
                                                   keep_4=0,
                                                   keep_5=0,
                                                   keep_6=0,
                                                   keep_8=0,
                                                   keep_10=0,
                                                   keep_12=0,
                                                   keep_16=0,
                                                   keep_20=0,
                                                   )

                #留存计算
                #当天的新增
                new_user_id_list = self.rh_stats_user_app_model.analyze_new_user_list(app_id=app_id, start=start, end=end, channel=channel)
                #次日
                next_day = day_date + timedelta(days=1)
                next_day_start = next_day.replace(minute=0, second=0, microsecond=0, hour=0)
                next_day_start_timestamp = (next_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                next_day_end_timestamp = next_day_start_timestamp + 24*60*60
                next_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=next_day_start_timestamp, end=next_day_end_timestamp, channel=channel)
                next_day_num = len(next_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_next_day_data(keep_next=next_day_num, id=document_id)

                #2日
                two_day = day_date + timedelta(days=2)
                two_day_start = two_day.replace(minute=0, second=0, microsecond=0, hour=0)
                two_day_start_timestamp = (two_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                two_day_end_timestamp = two_day_start_timestamp + 24*60*60
                two_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=two_day_start_timestamp, end=two_day_end_timestamp, channel=channel)
                two_day_num = len(two_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_two_day_data(keep_two=two_day_num, id=document_id)

                #三日
                third_day = day_date + timedelta(days=3)
                third_day_start = third_day.replace(minute=0, second=0, microsecond=0, hour=0)
                third_day_start_timestamp = (third_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                third_day_end_timestamp = third_day_start_timestamp + 24*60*60
                third_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=third_day_start_timestamp, end=third_day_end_timestamp, channel=channel)
                third_day_num = len(third_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_third_day_data(keep_third=third_day_num, id=document_id)

                #4日
                four_day = day_date + timedelta(days=4)
                four_day_start = four_day.replace(minute=0, second=0, microsecond=0, hour=0)
                four_day_start_timestamp = (four_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                four_day_end_timestamp = four_day_start_timestamp + 24*60*60
                four_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=four_day_start_timestamp, end=four_day_end_timestamp, channel=channel)
                four_day_num = len(four_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_four_day_data(keep_four=four_day_num, id=document_id)

                #5日
                five_day = day_date + timedelta(days=5)
                five_day_start = five_day.replace(minute=0, second=0, microsecond=0, hour=0)
                five_day_start_timestamp = (five_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                five_day_end_timestamp = five_day_start_timestamp + 24*60*60
                five_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=five_day_start_timestamp, end=five_day_end_timestamp, channel=channel)
                five_day_num = len(five_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_five_day_data(keep_five=five_day_num, id=document_id)

                #6日
                six_day = day_date + timedelta(days=6)
                six_day_start = six_day.replace(minute=0, second=0, microsecond=0, hour=0)
                six_day_start_timestamp = (six_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                six_day_end_timestamp = six_day_start_timestamp + 24*60*60
                six_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=six_day_start_timestamp, end=six_day_end_timestamp, channel=channel)
                six_day_num = len(six_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_six_day_data(keep_six=six_day_num, id=document_id)

                #七日
                seventh_day = day_date + timedelta(days=7)
                seventh_day_start = seventh_day.replace(minute=0, second=0, microsecond=0, hour=0)
                seventh_day_start_timestamp = (seventh_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                seventh_day_end_timestamp = seventh_day_start_timestamp + 24*60*60
                seventh_day_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=seventh_day_start_timestamp, end=seventh_day_end_timestamp, channel=channel)
                seventh_day_num = len(seventh_day_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_seven_day_data(keep_seven=seventh_day_num, id=document_id)

                #8日
                eight_day = day_date + timedelta(days=8)
                eight_day_start = eight_day.replace(minute=0, second=0, microsecond=0, hour=0)
                eight_day_start_timestamp = (eight_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                eight_day_end_timestamp = eight_day_start_timestamp + 24*60*60
                eight_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=eight_day_start_timestamp, end=eight_day_end_timestamp, channel=channel)
                eight_day_num = len(eight_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_eight_day_data(keep_eight=eight_day_num, id=document_id)

                #10日
                ten_day = day_date + timedelta(days=10)
                ten_day_start = ten_day.replace(minute=0, second=0, microsecond=0, hour=0)
                ten_day_start_timestamp = (ten_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                ten_day_end_timestamp = ten_day_start_timestamp + 24*60*60
                ten_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=ten_day_start_timestamp, end=ten_day_end_timestamp, channel=channel)
                ten_day_num = len(ten_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_ten_day_data(keep_ten=ten_day_num, id=document_id)

                #12日
                twelve_day = day_date + timedelta(days=12)
                twelve_day_start = twelve_day.replace(minute=0, second=0, microsecond=0, hour=0)
                twelve_day_start_timestamp = (twelve_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                twelve_day_end_timestamp = twelve_day_start_timestamp + 24*60*60
                twelve_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=twelve_day_start_timestamp, end=twelve_day_end_timestamp, channel=channel)
                twelve_day_num = len(twelve_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_twelve_day_data(keep_twelve=twelve_day_num, id=document_id)

                #14日
                fourteen_day = day_date + timedelta(days=14)
                fourteen_day_start = fourteen_day.replace(minute=0, second=0, microsecond=0, hour=0)
                fourteen_day_start_timestamp = (fourteen_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                fourteen_day_end_timestamp = fourteen_day_start_timestamp + 24*60*60
                fourteen_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=fourteen_day_start_timestamp, end=fourteen_day_end_timestamp, channel=channel)
                fourteen_day_num = len(fourteen_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_fourteen_day_data(keep_fourteen=fourteen_day_num, id=document_id)

                #16日
                sixteen_day = day_date + timedelta(days=16)
                sixteen_day_start = sixteen_day.replace(minute=0, second=0, microsecond=0, hour=0)
                sixteen_day_start_timestamp = (sixteen_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                sixteen_day_end_timestamp = sixteen_day_start_timestamp + 24*60*60
                sixteen_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=sixteen_day_start_timestamp, end=sixteen_day_end_timestamp, channel=channel)
                sixteen_day_num = len(sixteen_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_sixteen_day_data(keep_sixteen=sixteen_day_num, id=document_id)

                #20日
                twenty_day = day_date + timedelta(days=20)
                twenty_day_start = twenty_day.replace(minute=0, second=0, microsecond=0, hour=0)
                twenty_day_start_timestamp = (twenty_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                twenty_day_end_timestamp = twenty_day_start_timestamp + 24*60*60
                twenty_new_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=twenty_day_start_timestamp, end=twenty_day_end_timestamp, channel=channel)
                twenty_day_num = len(twenty_new_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_twenty_day_data(keep_twenty=twenty_day_num, id=document_id)

                #30日
                thirty_day = day_date + timedelta(days=30)
                thirty_day_start = thirty_day.replace(minute=0, second=0, microsecond=0, hour=0)
                thirty_day_start_timestamp = (thirty_day_start - datetime(1970, 1, 1)).total_seconds() - 8*60*60
                thirty_day_end_timestamp = next_day_start_timestamp + 24*60*60
                thirty_act_user_id_list = self.rh_stats_user_login_log_model.analyze_act_user_list(app_id=app_id, start=thirty_day_start_timestamp, end=thirty_day_end_timestamp, channel=channel)
                thirty_day_num = len(thirty_act_user_id_list & new_user_id_list)

                self.rh_analyze_kpi_model.add_thirty_day_data(keep_thirty=thirty_day_num, id=document_id)

            start_date = start_date + timedelta(days=1)
            start_date_timestamp += 24*60*60


class TestHandler(BaseHandler):
    def get(self, template_variables={}):
        # 昨天时间戳区间
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday_start = yesterday.replace(minute=0, second=0, microsecond=0, hour=0)
        start_timestamp = (yesterday_start - datetime(1970, 1, 1)).total_seconds() - 8 * 60 * 60
        end_timestamp = start_timestamp + 24 * 60 * 60

        start = start_timestamp
        end = end_timestamp