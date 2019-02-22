#-*- coding: UTF-8 -*-
#__author__ = ''
#!/usr/bin/env python
#
from __future__ import division
from tornado.options import options

class RhStatsDeviceModel(object):
    def __init__(self, db ,es, mmc):
        self.es = es
        self.mmc = mmc
        self.apps = self.get_games_list()
        super(RhStatsDeviceModel, self).__init__()

    #游戏列表
    def get_games_list(self):
        body = {
            "size": 0,
            "aggs": {
                "distinct_app": {
                    "terms": {
                        "field": "AppID"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        apps = []
        for app in result['aggregations']['distinct_app']['buckets']:
            apps.append(app['key'])

        return apps

    def get_chanel_list(self):
        body = {
            "size": 0,
            "aggs": {
                "distinct_app": {
                    "terms": {
                        "field": "Channel",
                        "size": 0
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        chanels = []
        for app in result['aggregations']['distinct_app']['buckets']:
            cnl = app['key']
            if cnl == '':
                cnl = u"未知"
            chanels.append(cnl)

        return chanels

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
                                    "ActTime": {
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
                        "field": "AppID",
                        "size": 0
                    },
                    "aggs": {
                        "distinct_channel": {
                            "terms": {
                                "field": "Channel",
                                "size": 0
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        apps = []
        for app in result['aggregations']['distinct_app']['buckets']:
            for channel in app['distinct_channel']['buckets']:
                apps.append([app['key'],channel['key']])

        return apps

    #统计新增设备数
    def analyze_new_device(self, app_id=0, start=0, end=0, channel=""):
        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"AppID": app_id}},
                                {"term": {"Channel": channel}},
                                {"range": {
                                    "RegTime": {
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

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        return result['hits']['total']

    #统计活跃设备数
    def analyze_act_device(self, app_id=0, start=0, end=0, channel=""):
        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"AppID": app_id}},
                                {"term": {"Channel": channel}},
                                {"range": {
                                    "ActTime": {
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

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        return result['hits']['total']

    def get_stats_device_list(self,app_id='', channel='', reg_start_time='', reg_end_time='', act_start_time='', act_end_time='', ip='', mac='', app_list=[]):
        must = []
        should = []
        reg_time = {}
        act_time = {}
        if app_id :
            must.append({"term": {"AppID": app_id}})
        else:
            must.append({"terms": {"AppID": app_list}})
        if channel:
            must.append({"term": {"Channel": channel}})
        if ip:
            should.append({"term" : { "RegIP" : ip}})
            should.append({"term" : { "ActIP" : ip}})
        if mac:
            must.append({"term": {"Mac": mac}})
        if reg_start_time:
            reg_time['gte'] = reg_start_time
        if reg_end_time:
            reg_time['lte'] = reg_end_time
        if act_start_time:
            act_time['gte'] = act_start_time
        if act_end_time:
            act_time['lte'] = act_end_time

        if reg_time:
            must.append({"range":{"RegTime": reg_time}})
        if act_time:
            must.append({"range":{"ActTime": act_time}})

        body = {
            "from": 0,
            "size": 200,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must,
                            "should": should
                        }
                    }
                }
            },
            "sort":{"ID": {"order": "desc"}}
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)
        return result["hits"]["hits"]

    #统计实时新增设备数
    def get_new_device(self, app_id=0, start=0, end=0, channel=None):

        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RegTime": {
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
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        return result['hits']['total']

    #统计实时活跃设备数
    def get_act_device(self, app_id=0, start=0, end=0, channel=None):
        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "ActTime": {
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
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_stats_device", body=body)

        return result['hits']['total']
