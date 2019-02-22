#!/usr/bin/env python
# coding=utf-8
#
# Copyright 2012 F2E.im
# Do have a faith in what you're doing.
# Make your life a story worth telling.

import tornado.web
import tornado.gen
import lib.session
import hashlib
import time
import json
import urllib2
import base64
import tornado.httpclient
import settings
from tornado.options import options

class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, *argc, **argkw):
        super(BaseHandler, self).__init__(*argc, **argkw)
        self.session = lib.session.Session(self.application.session_manager, self)

    def get(self):
        self.write("it works")

    @property
    def db(self):
        return self.application.db

    @property
    def db(self):
        return self.application.db_know

    @property
    def mmc(self):
        return self.application.mmc

    @property
    def loader(self):
        return self.application.loader

    @property
    def rh_stats_device_model(self):
        return self.application.rh_stats_device_model

    @property
    def rh_stats_pay_model(self):
        return self.application.rh_stats_pay_model

    @property
    def rh_stats_user_app_model(self):
        return self.application.rh_stats_user_app_model

    @property
    def rh_stats_user_login_log_model(self):
        return self.application.rh_stats_user_login_log_model

    @property
    def rh_stats_user_logout_log_model(self):
        return self.application.rh_stats_user_logout_log_model

    @property
    def rh_analyze_kpi_model(self):
        return self.application.rh_analyze_kpi_model

    def on_finish(self):
        super(BaseHandler, self).on_finish()
        self.session.save()

    def convert_app_array(self, app_list):
        apps = []
        if app_list:
            for app in app_list:
                apps.append(app['AppID'])
        return apps









