#-*- coding: UTF-8 -*-
#__author__ = 'zcl'
#!/usr/bin/env python
#

from base import *

class ErrorUrlHandle(BaseHandler):
    def get(self,template_variables = {}):
        self.session['error'] = u"您访问的url不存在"
        self.redirect("/404")
