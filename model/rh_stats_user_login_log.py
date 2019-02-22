#-*- coding: UTF-8 -*-
#__author__ = ''
#!/usr/bin/env python
#
from __future__ import division
from tornado.options import options


class RhStatsUserLoginLogModel(object):
    def __init__(self, db ,es, mmc):
        self.es = es
        self.mmc = mmc
        super(RhStatsUserLoginLogModel, self).__init__()

    #获取某时间区间的app列表和对应的渠道
    def get_rh_app_id_channel_list(self, start=0, end=0):
        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"range": {
                                    "addtime": {
                                        "gte": int(start),
                                        "lt": int(end)
                                    }}
                                }
                            ]
                        }
                    }
                }
            },
            "aggs": {
                "distinct_app": {
                    "terms": {
                        "field": "appid",
                        "size": 0
                    },
                    "aggs": {
                        "distinct_channel": {
                            "terms": {
                                "field": "channel",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="ch_user_op_log", body=body)

        apps = []
        for app in result['aggregations']['distinct_app']['buckets']:
            for channel in app['distinct_channel']['buckets']:
                apps.append([app['key'],channel['key']])

        return apps

    #统计登入用户数
    def analyze_act_user(self, app_id=0, start=0, end=0, channel=""):
        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"appid": app_id}},
                                {"term": {"channel": channel}},
                                {"range": {
                                    "addtime": {
                                        "gte": int(start),
                                        "lt": int(end)
                                    }}
                                }
                            ]
                        }
                    }
                }
            },
            "aggs": {
                "distinct_user": {
                    "cardinality": {
                        "field": "userid"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="ch_user_op_log", body=body)

        return result["aggregations"]["distinct_user"]["value"]

    #统计登入用户ID列表--scroll
    def analyze_act_user_list(self, app_id=0, start=0, end=0, channel=""):
        body = {
            "size": 100,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"AppID": app_id}},
                                {"term": {"Channel": channel}},
                                {"range": {
                                    "RecordTime": {
                                        "gte": int(start),
                                        "lt": int(end)
                                    }}
                                }
                            ]
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body, scroll="1m")
        scroll_id = result['_scroll_id']

        uid_list = set()
        while result['hits']['hits']:
            for user in result['hits']['hits']:
                uid_list.add(user['_source']['UserID'])
            result = self.es.scroll(scroll_id=scroll_id, scroll="1m")

        return uid_list

    def get_user_login_list(self,user_id='', role_id='' ,app_id='', channel='', start_time='', end_time='', app_list=[]):
        must = []
        time = {}
        if user_id:
            must.append({"term": {"UserID": user_id}})
        if role_id:
            must.append({"term":{"RoleID":role_id}})
        if app_id:
            must.append({"term": {"AppID": app_id}})
        else:
            must.append({"terms": {"AppID": app_list}})
        if channel :
            must.append({"term": {"Channel": channel}})
        if start_time:
            time['gte'] = start_time
        if end_time:
            time['lte'] = end_time
        print(time)
        if time:
            must.append({"range":{"RecordTime": time}})

        body = {
            "from": 0,
            "size": 200,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must
                        }
                    }
                }
            },
            "sort": {"RecordTime": {"order": "desc"}}
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        return result["hits"]["hits"]


    #统计实时登入用户数
    def get_act_user(self, app_id=0, start=0, end=0, channel=None):
        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        if channel is not None:
            must_con.append({"terms": {"Channel": channel}})

        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must_con
                        }
                    }
                }
            },
            "aggs": {
                "distinct_user": {
                    "cardinality": {
                        "field": "UserID"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)

        return result["aggregations"]["distinct_user"]["value"]

    def get_areaserver_by_app(self, app_id=None, channel=None, yyb_check = False):
        must_con = [
            {"term": {"AppID": app_id}},
        ]

        must_not_con = []

        if channel:
            must_con.append({"terms": {"Channel": channel}})

        if yyb_check:
            must_not_con.append({"term": {"Channel": "yyb"}})

        body = {
            "aggs": {
                "areaserver": {
                    "filter": {
                        "bool": {
                            "must": must_con,
                            "must_not": must_not_con
                        }
                    },
                    "aggs": {
                        "areaservers": {
                            "terms": {
                                "field": "AreaServerID",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)

        servers = result["aggregations"]["areaserver"]["areaservers"]['buckets']
        areaservers = set()
        for server in servers:
            areaservers.add(server['key'])

        return areaservers


    #按天聚合登入人数
    def get_act_user_by_day(self, start=0, end=0, app_id=None, channel=None, servers=None, yyb_check = False):

        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        must_not_con = []

        if channel:
            must_con.append({"terms": {"Channel": channel}})

        if servers:
            must_con.append({"terms": {"AreaServerID": servers}})

        if yyb_check:
            must_not_con.append({"term": {"Channel": "yyb"}})

        body = {
            "size": 0,
            "aggs": {
                "by_days": {
                    "filter": {
                        "bool": {
                            "must": must_con,
                            "must_not": must_not_con
                        }
                    },
                    "aggs": {
                        "day_data": {
                            "date_histogram": {
                                "field": "RecordTime",
                                "interval": "day",
                                "format": "yyyy-MM-dd",
                                "time_zone": "+08:00"
                            },
                            "aggs": {
                                "distinct_user": {
                                    "cardinality": {
                                        "script": "doc['Channel'].value + ' ' + doc['AreaServerID'].value + ' ' + doc['RoleID'].value"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        day_data_list = result['aggregations']['by_days']['day_data']['buckets']

        return day_data_list

    def get_apps(self, start=0, end=0):
        must_con = [
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        body = {
            "aggs": {
                "app": {
                    "filter": {
                        "bool": {
                            "must": must_con
                        }
                    },
                    "aggs": {
                        "apps": {
                            "terms": {
                                "field": "AppID",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        apps = result["aggregations"]["app"]["apps"]['buckets']
        app_list= set()
        for app in apps:
            app_list.add(app['key'])

        return app_list

    def get_servers(self, app_id=0, start=0, end=0):
        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        body = {
            "aggs": {
                "server": {
                    "filter": {
                        "bool": {
                            "must": must_con
                        }
                    },
                    "aggs": {
                        "servers": {
                            "terms": {
                                "field": "AreaServerID",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        servers = result["aggregations"]["server"]["servers"]['buckets']
        server_list= set()
        for server in servers:
            server_list.add(server['key'])

        return server_list

    def get_channels(self, app_id=0, start=0, end=0):
        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        body = {
            "aggs": {
                "channel": {
                    "filter": {
                        "bool": {
                            "must": must_con
                        }
                    },
                    "aggs": {
                        "channels": {
                            "terms": {
                                "field": "Channel",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        channels = result["aggregations"]["channel"]["channels"]['buckets']
        channel_list= set()
        for channel in channels:
            channel_list.add(channel['key'])

        return channel_list

    def get_channel_server_list(self, app_id=0, start=0, end=0):
        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"AppID": app_id}},
                                {"range": {
                                    "RecordTime": {
                                        "gte": int(start),
                                        "lt": int(end)
                                    }}
                                }
                            ]
                        }
                    }
                }
            },
            "aggs": {
                "distinct_channel": {
                    "terms": {
                        "field": "Channel",
                        "size": 0
                    },
                    "aggs": {
                        "distinct_server": {
                            "terms": {
                                "field": "AreaServerID",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)
        channel_server_list = []
        for data in result['aggregations']['distinct_channel']['buckets']:
            for server in data['distinct_server']['buckets']:
                channel_server_list.append([data['key'], server['key']])

        return channel_server_list

    #统计活跃用户
    def get_act_user_server(self, app_id=0, channel='', server='', start=0, end=0):
        must_con = [
            {"term": {"AppID": app_id}},
            {"term": {"Channel": channel}},
            {"term": {"AreaServerID": server}},
            {"range": {
                "RecordTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must_con
                        }
                    }
                }
            },
            "aggs": {
                "distinct_user": {
                    "cardinality": {
                        "script": "doc['Channel'].value + ' ' + doc['AreaServerID'].value + ' ' + doc['RoleID'].value"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_login_log", body=body)

        return result["aggregations"]["distinct_user"]["value"]
