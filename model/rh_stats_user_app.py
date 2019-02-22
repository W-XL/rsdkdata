#-*- coding: UTF-8 -*-
#__author__ = ''
#!/usr/bin/env python
#
from __future__ import division
from tornado.options import options


class RhStatsUserAppModel(object):
    def __init__(self, db, es, mmc):
        self.es = es
        self.mmc = mmc
        super(RhStatsUserAppModel, self).__init__()

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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)

        apps = []
        for app in result['aggregations']['distinct_app']['buckets']:
            for channel in app['distinct_channel']['buckets']:
                apps.append([app['key'],channel['key']])

        return apps

    #统计新增账号数
    def analyze_new_user(self, app_id=0, start=0, end=0, channel=""):
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
            },
            "aggs": {
                "distinct_user": {
                    "cardinality": {
                        "field": "UserID"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)

        return result["aggregations"]["distinct_user"]["value"]

    #统计新增角色数
    def analyze_new_role(self, app_id=0, start=0, end=0, channel=""):
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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)

        return result['hits']['total']

    #统计新增用户ID列表--scroll
    def analyze_new_user_list(self, app_id=0, start=0, end=0, channel=""):
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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body, scroll="1m")
        scroll_id = result['_scroll_id']

        uid_list = set()
        while result['hits']['hits']:
            for user in result['hits']['hits']:
                uid_list.add(user['_source']['UserID'])
            result = self.es.scroll(scroll_id=scroll_id, scroll="1m")

        return uid_list

    def get_user_app_list(self,app_id='', channel='', user_id='', reg_start_time='', reg_end_time='', act_start_time='', act_end_time='', app_list=[]):
        must = []
        reg_time = {}
        act_time = {}
        if app_id :
            must.append({"term": {"AppID": app_id}})
        else:
            must.append({"terms": {"AppID": app_list}})
        if channel:
            must.append({"term": {"Channel": channel}})
        if user_id:
            must.append({"term": {"UserID": user_id}})
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
                            "must": must
                        }
                    }
                }
            },
            "sort": {"ID" :{"order": "desc"}}
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)
        return result["hits"]["hits"]

    #统计实时新增账号数
    def get_new_user(self, app_id=0, start=0, end=0, channel=None):
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
            },
            "aggs": {
                "distinct_user": {
                    "cardinality": {
                        "field": "UserID"
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)

        return result["aggregations"]["distinct_user"]["value"]

    #新增用户ID列表--scroll
    def get_new_user_list(self, app_id=0, start=0, end=0, channel=None, servers=None, yyb_check=False):

        must_con = [
            {"term": {"AppID": app_id}},
            {"range": {
                "RegTime": {
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
            "size": 500,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must_con,
                            "must_not": must_not_con
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body, scroll="1m")
        scroll_id = result['_scroll_id']

        uid_list = set()
        while result['hits']['hits']:
            for user in result['hits']['hits']:
                uid_list.add(user['_source']['UserID'])
            result = self.es.scroll(scroll_id=scroll_id, scroll="1m")

        return uid_list

    #新增用户ID列表--scroll
    def get_new_user_list_server(self, app_id=0, channel='', server='', start=0, end=0):

        must_con = [
            {"term": {"AppID": app_id}},
            {"term": {"Channel": channel}},
            {"term": {"AreaServerID": server}},
            {"range": {
                "RegTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        body = {
            "size": 500,
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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body, scroll="1m")
        scroll_id = result['_scroll_id']

        uid_list = set()
        while result['hits']['hits']:
            for user in result['hits']['hits']:
                uid_list.add(user['_source']['Channel'] + user['_source']['AreaServerID'] + user['_source']['RoleID'])
            result = self.es.scroll(scroll_id=scroll_id, scroll="1m")

        return uid_list


    #新增用户数
    def get_new_user_server(self, app_id=0, channel='', server='', start=0, end=0):

        must_con = [
            {"term": {"AppID": app_id}},
            {"term": {"Channel": channel}},
            {"term": {"AreaServerID": server}},
            {"range": {
                "RegTime": {
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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)

        return result["aggregations"]["distinct_user"]["value"]

    #新增用户ID列表--scroll
    def get_new_role_list_info(self, app_id=0, start=0, end=0):

        must_con = [
            {"range": {
                "RegTime": {
                    "gte": int(start),
                    "lt": int(end)
                }}
            }
        ]

        if app_id != 0:
            must_con.append({"term": {"AppID": app_id}})

        body = {
            "size": 500,
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

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body, scroll="1m")
        scroll_id = result['_scroll_id']

        role_list = []
        while result['hits']['hits']:
            for role in result['hits']['hits']:
                role_list.append({'app_id': role['_source']['AppID'], 'channel': role['_source']['Channel'], 'server': role['_source']['AreaServerID'],
                                  'role_id': role['_source']['RoleID'], 'reg_time': role['_source']['RegTime'], 'user_id': role['_source']['UserID']})
            result = self.es.scroll(scroll_id=scroll_id, scroll="1m")

        return role_list

    #是否滚服角色
    def is_moved_server_user(self, app_id='', channel='', user_id='', server='', reg_time=''):
        must_con = [
            {"term": {"AppID": app_id}},
            {"term": {"Channel": channel}},
            {"term": {"UserID": user_id}},
            {"range": {
                "RegTime": {
                    "lt": int(reg_time)
                }}
            }
        ]

        must_not_con = [
            {"term": {"AreaServerID": server}}
        ]

        body = {
            "from": 0,
            "size": 1,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": must_con,
                            "must_not": must_not_con
                        }
                    }
                }
            }
        }

        result = self.es.search(index="sdk_log", doc_type="super_user_app", body=body)
        if result["hits"]["hits"]:
            return 1
        else:
            return 0