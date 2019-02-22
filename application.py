#!/usr/bin/env python
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding("utf8")
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options

import web


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(web.Web())
    http_server.listen(options.port_web)

if __name__ == "__main__":
    main()
