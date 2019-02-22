import os
from tornado.options import define, options

define("port_web",          default = 4040, help = "Honey Web run run", type = int)

define("mysql_database",    default = "niuniu", help = "database name")
define("mysql_host",        default = "10.10.161.113:3407", help = "database host")
define("mysql_user",        default = "root", help = "database user")
define("mysql_password",    default = "0BXzT6rqlwye62Hk", help = "database password")
define("mysql_host_write",        default = "10.10.156.221:3307", help = "database host")
define("mysql_user_write",        default = "root", help = "database user")
define("mysql_password_write",    default = "0BXzT6rqlwye62Hk", help = "database password")

define("es_host",           default = "10.10.224.43", help = "elasticsearch host")
define("es_port",           default = "9200", help = "elasticsearch port")
define("es_index", 'sdk_log')

define("mmc_host",    default = "10.10.203.115", help = "memcache host")
define("mmc_port",    default = "11211", help = "memcache port")
define("domain",            default = "http://127.0.0.1", help = "domain")
define('project_root',      default = "/data/wwwroot/rsdkdata", help = "project root")