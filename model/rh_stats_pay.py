#-*- coding: UTF-8 -*-
#__author__ = ''
#!/usr/bin/env python
#
from __future__ import division
from tornado.options import options
from lib.query import Query

class RhStatsPayModel(Query):
    def __init__(self, db ,es, mmc):
        self.es = es
        self.mmc = mmc
        self.db = db
        self.table_name = "super_orders"
        super(RhStatsPayModel, self).__init__()

    #分析充值额
    def analyze_pay_ammount(self, app_id=0, start=0, end=0, channel=""):
        where = "app_id=%s and status=2" % app_id
        #时间段
        if start:
            where += " and buy_time>='%s'" % int(start)
        if end:
            where += " and buy_time<='%s'" % int(end)

        if channel:
            where += " and channel='%s'" % channel
        field = "ifnull(sum(pay_money),0) as pay_money"
        return self.where(where).field(field).find()

    #分析充值人数
    def analyze_pay_count(self, app_id=0, start=0, end=0, channel=""):
        where = "app_id=%s and status=2" % app_id
        # 时间段
        if start:
            where += " and buy_time>='%s'" % start
        if end:
            where += " and buy_time<='%s'" % end

        if channel:
            where += " and channel='%s'" % channel
        fields = "buyer_id,count(0)"
        return self.where(where).group("buyer_id").field(fields).select()
