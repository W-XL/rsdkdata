#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding("utf8")
import os.path
import memcache
import torndb
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import handler.base
import handler.rh_analyze
import handler.error

from tornado.options import options
from lib.loader import Loader
from lib.session import Session, SessionManager
from elasticsearch import Elasticsearch


def format_currency(value):
        return "{:,.0f}".format(value)

def format_int(value):
        return "{:,.0f}".format(value)

class Web(tornado.web.Application):
    def __init__(self):
        settings = dict(
            blog_title=u"融合数据统计",
            cookie_secret="4analyze#66173#cn",
            debug=True
        )
        handlers = [
            #融合数据
            (r"/rh_analyze_hour", handler.rh_analyze.RhAnalyzeHourHandler),
            (r"/rh_analyze_day", handler.rh_analyze.RhAnalyzeDayHandler),
            (r"/rh_analyze_old", handler.rh_analyze.RhAnalyzeOldDataHandler),
            #系统容错
            (r".*", handler.error.BaseHandler),
            ('.*', handler.error.ErrorUrlHandle),
        ]

        tornado.web.Application.__init__(self, handlers, **settings)

        # Have one global connection to the blog DB across all handlers
        self.db = torndb.Connection(
            host = options.mysql_host,
            database = options.mysql_database,
            user = options.mysql_user,
            password = options.mysql_password,
        )

        # Have one global connection elasticsearch
        self.es = Elasticsearch([options.es_host+":"+options.es_port])

        # Have one global memcache controller
        self.mmc = memcache.Client([options.mmc_host+":"+options.mmc_port])

        # Have one global loader for loading models and handles
        self.loader = Loader(self.db, self.es, self.mmc)

        # Have one global model for db query
        self.rh_stats_device_model = self.loader.use("rh_stats_device.model")
        self.rh_stats_user_app_model = self.loader.use("rh_stats_user_app.model")
        self.rh_stats_user_login_log_model = self.loader.use("rh_stats_user_login_log.model")
        self.rh_analyze_kpi_model = self.loader.use("rh_analyze_kpi.model")
        self.rh_stats_pay_model = self.loader.use("rh_stats_pay.model")
        # Have one global session controller
        self.session_manager = SessionManager(settings["cookie_secret"], self.mmc, 7200)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Web())
    http_server.listen(options.port_web)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
