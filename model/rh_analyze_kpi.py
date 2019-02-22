#-*- coding: UTF-8 -*-
#__author__ = 'zcl'
#!/usr/bin/env python
#
from __future__ import division

import time
from tornado.options import options
from elasticsearch import Elasticsearch

class RhAnalyzeKpiModel(object):
    def __init__(self, db ,es, mmc):
        self.es = es
        self.mmc = mmc
        super(RhAnalyzeKpiModel, self).__init__()

    def add_hour_data(self, new_user=0, new_device=0, new_role=0, act_user=0, act_device=0, pay_ammount=0.0,
                      pay_count=0, id="", app_id=0, channel="", hour=0, day=0, month=0, year=0, time_str=""):
        body = {
            "act_device": act_device,
            "act_user": act_user,
            "app_id": app_id,
            "channel": channel,
            "keep_fourteen": 0,
            "keep_next": 0,
            "keep_seven": 0,
            "keep_third": 0,
            "keep_thirty": 0,
            "new_device": new_device,
            "new_role": new_role,
            "new_user": new_user,
            "pay_count": pay_count,
            "pay_money": pay_ammount,
            "stats_type": 2,
            "day": day,
            "hour": hour,
            "month": month,
            "year": year,
            "time": time_str,
            "addtime" : int(time.time())
        }

        self.es.index(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_day_data(self, new_user=0, new_device=0, new_role=0, act_user=0, act_device=0, pay_ammount=0.0, pay_count=0,
                     id="", app_id=0, channel="", hour=0, day=0, month=0, year=0, keep_fourteen=0, keep_next=0, keep_2=0, keep_4=0, keep_5=0, keep_6=0, keep_8=0, keep_10=0, keep_12=0, keep_16=0, keep_20=0, keep_seven=0, keep_third=0, keep_thirty=0, time_str=""):
        body = {
            "act_device": act_device,
            "act_user": act_user,
            "app_id": app_id,
            "channel": channel,
            "keep_fourteen": keep_fourteen,
            "keep_next": keep_next,
            "keep_two": keep_2,
            "keep_four": keep_4,
            "keep_five": keep_5,
            "keep_six": keep_6,
            "keep_eight": keep_8,
            "keep_ten": keep_10,
            "keep_twelve": keep_12,
            "keep_sixteen": keep_16,
            "keep_twenty": keep_20,
            "keep_seven": keep_seven,
            "keep_third": keep_third,
            "keep_thirty": keep_thirty,
            "new_device": new_device,
            "new_role": new_role,
            "new_user": new_user,
            "pay_count": pay_count,
            "pay_money": pay_ammount,
            "stats_type": 1,
            "day": day,
            "hour": hour,
            "month": month,
            "year": year,
            "time": time_str,
            "addtime": int(time.time())
        }

        self.es.index(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_next_day_data(self, id, keep_next=0):

        body = {
            "doc": {
                "keep_next": keep_next
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_two_day_data(self, id, keep_two=0):

        body = {
            "doc": {
                "keep_two": keep_two
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_four_day_data(self, id, keep_four=0):

        body = {
            "doc": {
                "keep_four": keep_four
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_five_day_data(self, id, keep_five=0):

        body = {
            "doc": {
                "keep_five": keep_five
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_six_day_data(self, id, keep_six=0):

        body = {
            "doc": {
                "keep_six": keep_six
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_eight_day_data(self, id, keep_eight=0):

        body = {
            "doc": {
                "keep_eight": keep_eight
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_ten_day_data(self, id, keep_ten=0):

        body = {
            "doc": {
                "keep_ten": keep_ten
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_twelve_day_data(self, id, keep_twelve=0):

        body = {
            "doc": {
                "keep_twelve": keep_twelve
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_sixteen_day_data(self, id, keep_sixteen=0):

        body = {
            "doc": {
                "keep_sixteen": keep_sixteen
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_twenty_day_data(self, id, keep_twenty=0):

        body = {
            "doc": {
                "keep_twenty": keep_twenty
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_third_day_data(self, id, keep_third=0):

        body = {
            "doc": {
                "keep_third": keep_third
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_seven_day_data(self, id, keep_seven=0):

        body = {
            "doc": {
                "keep_seven": keep_seven
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_fourteen_day_data(self, id, keep_fourteen=0):

        body = {
            "doc": {
                "keep_fourteen": keep_fourteen
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    def add_thirty_day_data(self, id, keep_thirty=0):

        body = {
            "doc": {
                "keep_thirty": keep_thirty
            }
        }

        if self.is_day_data_exist(id=id):
            self.es.update(index='r_sdk_log', doc_type="rh_analyze_kpi", body=body, id=id)

    #判断是否有数据
    def is_day_data_exist(self, id=""):

        return self.es.exists(index='r_sdk_log', doc_type="rh_analyze_kpi", id=id)

    #获取已经接入的游戏
    def get_apps(self):

        body = {
            "size": 0,
            "aggs": {
                "distinct_app": {
                    "terms": {
                        "field": "app_id"
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        apps = []
        for app in result['aggregations']['distinct_app']['buckets']:
            apps.append(app['key'])

        return apps

    #游戏日综合数据
    def get_apps_day_data(self, year=0, month=0, day=0, app_list=None, stats_type=1, channels=None):

        must_con = [
            {"terms": {"app_id": app_list}},
            {"term": {"year": year}},
            {"term": {"month": month}},
            {"term": {"day": day}},
            {"term": {"stats_type": stats_type}}
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "sum_pay": {
                    "sum": {
                        "field": "pay_money"
                    }
                },
                "sum_new_user": {
                    "sum": {
                        "field": "new_user"
                    }
                },
                "sum_new_device": {
                    "sum": {
                        "field": "new_device"
                    }
                },
                "sum_new_role": {
                    "sum": {
                        "field": "new_role"
                    }
                },
                "sum_act_user": {
                    "sum": {
                        "field": "act_user"
                    }
                },
                "sum_act_device": {
                    "sum": {
                        "field": "act_device"
                    }
                },
                "sum_pay_count": {
                    "sum": {
                        "field": "pay_count"
                    }
                },
                "sum_keep_next": {
                    "sum": {
                        "field": "keep_next"
                    }
                },
                "sum_keep_third": {
                    "sum": {
                        "field": "keep_third"
                    }
                },
                "sum_keep_seven": {
                    "sum": {
                        "field": "keep_seven"
                    }
                },
                "sum_keep_fourteen": {
                    "sum": {
                        "field": "keep_fourteen"
                    }
                },
                "sum_keep_thirty": {
                    "sum": {
                        "field": "keep_thirty"
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        data = {}
        for key in result['aggregations']:
            data[key] = result['aggregations'][key]['value']

        return data

    #游戏趋势数据
    def get_apps_trend_data(self, app_list=None, start_date="", end_date="", source="", channels=None):

        must_con = [
            {"terms": {"app_id": app_list}},
            {"term": {"stats_type": 1}},
            {"range": {
                "time": {
                    "gte": start_date,
                    "lte": end_date
                }}
            }
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "time": {
                    "terms": {
                        "field": "time",
                        "size": 0,
                        "order": {"_term": "asc"}
                    },
                    "aggs":{
                        "sum_source": {
                            "sum": {
                                "field": source
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        total = 0
        data_list = []
        date_list = []
        date_list_long = []
        for source in result['aggregations']['time']['buckets']:
            data_list.append(source['sum_source']['value'])
            date_list.append(str(source['key_as_string'][5:10]))
            date_list_long.append(str(source['key_as_string'][0:10]))
            total += source['sum_source']['value']

        return {"data_list": data_list, "date_list": date_list, "data_list_profit": data_list[:], "data_list_cost": data_list[:], "date_list_long": date_list_long, "total": total}

    #单个游戏的日综合数据
    def get_app_day_data(self, app_list=None, start_date="", end_date="", channels=None):

        must_con = [
            {"terms": {"app_id": app_list}},
            {"term": {"stats_type": 1}},
            {"range": {
                "time": {
                    "gte": start_date,
                    "lte": end_date
                }}
            }
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "app_id": {
                    "terms": {
                        "field": "app_id",
                        "size": 0
                    },
                    "aggs":{
                        "sum_pay": {
                            "sum": {
                                "field": "pay_money"
                            }
                        },
                        "sum_new_user": {
                            "sum": {
                                "field": "new_user"
                            }
                        },
                        "sum_new_device": {
                            "sum": {
                                "field": "new_device"
                            }
                        },
                        "sum_new_role": {
                            "sum": {
                                "field": "new_role"
                            }
                        },
                        "sum_act_user": {
                            "sum": {
                                "field": "act_user"
                            }
                        },
                        "sum_act_device": {
                            "sum": {
                                "field": "act_device"
                            }
                        },
                        "sum_pay_count": {
                            "sum": {
                                "field": "pay_count"
                            }
                        },
                        "sum_keep_next": {
                            "sum": {
                                "field": "keep_next"
                            }
                        },
                        "sum_keep_third": {
                            "sum": {
                                "field": "keep_third"
                            }
                        },
                        "sum_keep_seven": {
                            "sum": {
                                "field": "keep_seven"
                            }
                        },
                        "sum_keep_fourteen": {
                            "sum": {
                                "field": "keep_fourteen"
                            }
                        },
                        "sum_keep_thirty": {
                            "sum": {
                                "field": "keep_thirty"
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        return result['aggregations']['app_id']['buckets']

    #游戏24小时综合数据
    def get_apps_hour_data(self, year=0, month=0, day=0, app_list=None, channels=None):

        must_con = [
            {"terms": {"app_id": app_list}},
            {"term": {"year": year}},
            {"term": {"month": month}},
            {"term": {"day": day}},
            {"term": {"stats_type": 2}},
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "hour": {
                    "terms": {
                        "field": "hour",
                        "size": 0,
                        "order": {"_term": "asc"}
                    },
                    "aggs":{
                        "sum_pay": {
                            "sum": {
                                "field": "pay_money"
                            }
                        },
                        "sum_new_user": {
                            "sum": {
                                "field": "new_user"
                            }
                        },
                        "sum_new_device": {
                            "sum": {
                                "field": "new_device"
                            }
                        },
                        "sum_new_role": {
                            "sum": {
                                "field": "new_role"
                            }
                        },
                        "sum_act_user": {
                            "sum": {
                                "field": "act_user"
                            }
                        },
                        "sum_act_device": {
                            "sum": {
                                "field": "act_device"
                            }
                        },
                        "sum_pay_count": {
                            "sum": {
                                "field": "pay_count"
                            }
                        },
                        "sum_keep_next": {
                            "sum": {
                                "field": "keep_next"
                            }
                        },
                        "sum_keep_third": {
                            "sum": {
                                "field": "keep_third"
                            }
                        },
                        "sum_keep_seven": {
                            "sum": {
                                "field": "keep_seven"
                            }
                        },
                        "sum_keep_fourteen": {
                            "sum": {
                                "field": "keep_fourteen"
                            }
                        },
                        "sum_keep_thirty": {
                            "sum": {
                                "field": "keep_thirty"
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        return result['aggregations']['hour']['buckets']

    #单个游戏的每日数据
    def get_single_app_day_data(self, app_id=0, start_date="", end_date="", channels=None):

        must_con = [
            {"term": {"app_id": app_id}},
            {"term": {"stats_type": 1}},
            {"range": {
                "time": {
                    "gte": start_date,
                    "lte": end_date
                }}
            }
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "time": {
                    "terms": {
                        "field": "time",
                        "size": 0,
                        "order": {"_term": "asc"}
                    },
                    "aggs":{
                        "sum_pay": {
                            "sum": {
                                "field": "pay_money"
                            }
                        },
                        "sum_new_user": {
                            "sum": {
                                "field": "new_user"
                            }
                        },
                        "sum_new_device": {
                            "sum": {
                                "field": "new_device"
                            }
                        },
                        "sum_new_role": {
                            "sum": {
                                "field": "new_role"
                            }
                        },
                        "sum_act_user": {
                            "sum": {
                                "field": "act_user"
                            }
                        },
                        "sum_act_device": {
                            "sum": {
                                "field": "act_device"
                            }
                        },
                        "sum_pay_count": {
                            "sum": {
                                "field": "pay_count"
                            }
                        },
                        "sum_keep_next": {
                            "sum": {
                                "field": "keep_next"
                            }
                        },
                        "sum_keep_two": {
                            "sum": {
                                "field": "keep_two"
                            }
                        },
                        "sum_keep_third": {
                            "sum": {
                                "field": "keep_third"
                            }
                        },
                        "sum_keep_four": {
                            "sum": {
                                "field": "keep_four"
                            }
                        },
                        "sum_keep_five": {
                            "sum": {
                                "field": "keep_five"
                            }
                        },
                        "sum_keep_six": {
                            "sum": {
                                "field": "keep_six"
                            }
                        },
                        "sum_keep_seven": {
                            "sum": {
                                "field": "keep_seven"
                            }
                        },
                        "sum_keep_eight": {
                            "sum": {
                                "field": "keep_eight"
                            }
                        },
                        "sum_keep_ten": {
                            "sum": {
                                "field": "keep_ten"
                            }
                        },
                        "sum_keep_twelve": {
                            "sum": {
                                "field": "keep_twelve"
                            }
                        },
                        "sum_keep_fourteen": {
                            "sum": {
                                "field": "keep_fourteen"
                            }
                        },
                        "sum_keep_sixteen": {
                            "sum": {
                                "field": "keep_sixteen"
                            }
                        },
                        "sum_keep_twenty": {
                            "sum": {
                                "field": "keep_twenty"
                            }
                        },
                        "sum_keep_thirty": {
                            "sum": {
                                "field": "keep_thirty"
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        total_pay = 0
        total_act_user = 0
        total_new_user = 0
        total_act_device = 0
        total_new_device = 0
        date_list = []
        short_date_list = []
        pay_list = []
        new_user_list = []
        act_user_list = []
        new_device_list = []
        act_device_list = []
        next_list = []
        two_list = []
        third_list = []
        four_list = []
        five_list = []
        six_list = []
        seven_list = []
        eight_list = []
        ten_list = []
        twelve_list = []
        fourteen_list = []
        sixteen_list = []
        twenty_list = []
        thirty_list = []
        pay_count_list = []
        pay_list_graph = []
        sum_new_user = []
        sum_act_user = []
        sum_keep_next = []
        sum_keep_two = []
        sum_keep_third = []
        sum_keep_four = []
        sum_keep_five = []
        sum_keep_six = []
        sum_keep_seven = []
        sum_keep_eight = []
        sum_keep_ten = []
        sum_keep_twelve = []
        sum_keep_fourteen = []
        sum_keep_sixteen = []
        sum_keep_twenty = []
        sum_keep_thirty = []
        for source in result['aggregations']['time']['buckets']:
            date_list.append(str(source['key_as_string'][:10]))
            short_date_list.append(str(source['key_as_string'][5:10]))
            pay_count_list.append(source['sum_pay_count']['value'])
            pay_list_graph.append(source['sum_pay']['value'])
            pay_list.append(source['sum_pay']['value'])
            total_pay += source['sum_pay']['value']
            sum_new_user.append(int(source['sum_new_user']['value']))
            sum_act_user.append(int(source['sum_act_user']['value']))
            new_user_list.append(int(source['sum_new_user']['value']))
            total_new_user += source['sum_new_user']['value']
            act_user_list.append(int(source['sum_act_user']['value']))
            total_act_user += source['sum_act_user']['value']
            act_device_list.append(int(source['sum_act_device']['value']))
            total_act_device += source['sum_act_device']['value']
            new_device_list.append(int(source['sum_new_device']['value']))
            total_new_device += source['sum_new_device']['value']
            next_list.append(int(source['sum_keep_next']['value']))
            two_list.append(int(source['sum_keep_two']['value']))
            third_list.append(int(source['sum_keep_third']['value']))
            four_list.append(int(source['sum_keep_four']['value']))
            five_list.append(int(source['sum_keep_five']['value']))
            six_list.append(int(source['sum_keep_six']['value']))
            seven_list.append(int(source['sum_keep_seven']['value']))
            eight_list.append(int(source['sum_keep_eight']['value']))
            ten_list.append(int(source['sum_keep_ten']['value']))
            twelve_list.append(int(source['sum_keep_twelve']['value']))
            fourteen_list.append(int(source['sum_keep_fourteen']['value']))
            sixteen_list.append(int(source['sum_keep_sixteen']['value']))
            twenty_list.append(int(source['sum_keep_twenty']['value']))
            thirty_list.append(int(source['sum_keep_thirty']['value']))
            if int(source['sum_new_user']['value']) == 0:
                next_rate = 0
                two_rate = 0
                third_rate = 0
                four_rate = 0
                five_rate = 0
                six_rate = 0
                seven_rate = 0
                eight_rate = 0
                ten_rate = 0
                twelve_rate = 0
                fourteen_rate = 0
                sixteen_rate = 0
                twenty_rate = 0
                thirty_rate = 0
            else:
                next_rate = (int(source['sum_keep_next']['value']) / int(source['sum_new_user']['value'])) * 100
                two_rate = (int(source['sum_keep_two']['value']) / int(source['sum_new_user']['value'])) * 100
                third_rate = (int(source['sum_keep_third']['value']) / int(source['sum_new_user']['value'])) * 100
                four_rate = (int(source['sum_keep_four']['value']) / int(source['sum_new_user']['value'])) * 100
                five_rate = (int(source['sum_keep_five']['value']) / int(source['sum_new_user']['value'])) * 100
                six_rate = (int(source['sum_keep_six']['value']) / int(source['sum_new_user']['value'])) * 100
                seven_rate = (int(source['sum_keep_seven']['value']) / int(source['sum_new_user']['value'])) * 100
                eight_rate = (int(source['sum_keep_eight']['value']) / int(source['sum_new_user']['value'])) * 100
                ten_rate = (int(source['sum_keep_ten']['value']) / int(source['sum_new_user']['value'])) * 100
                twelve_rate = (int(source['sum_keep_twelve']['value']) / int(source['sum_new_user']['value'])) * 100
                fourteen_rate = (int(source['sum_keep_fourteen']['value']) / int(source['sum_new_user']['value'])) * 100
                sixteen_rate = (int(source['sum_keep_sixteen']['value']) / int(source['sum_new_user']['value'])) * 100
                twenty_rate = (int(source['sum_keep_next']['value']) / int(source['sum_new_user']['value'])) * 100
                thirty_rate = (int(source['sum_keep_thirty']['value']) / int(source['sum_new_user']['value'])) * 100

            sum_keep_next.append(round(next_rate, 2))
            sum_keep_two.append(round(two_rate, 2))
            sum_keep_third.append(round(third_rate, 2))
            sum_keep_four.append(round(four_rate, 2))
            sum_keep_five.append(round(five_rate, 2))
            sum_keep_six.append(round(six_rate, 2))
            sum_keep_seven.append(round(seven_rate, 2))
            sum_keep_eight.append(round(eight_rate, 2))
            sum_keep_ten.append(round(ten_rate, 2))
            sum_keep_twelve.append(round(twelve_rate, 2))
            sum_keep_fourteen.append(round(fourteen_rate, 2))
            sum_keep_sixteen.append(round(sixteen_rate, 2))
            sum_keep_twenty.append(round(twenty_rate, 2))
            sum_keep_thirty.append(round(thirty_rate, 2))

        #逆排序
        date_list.reverse()
        pay_list.reverse()
        new_user_list.reverse()
        act_user_list.reverse()
        new_device_list.reverse()
        act_device_list.reverse()
        next_list.reverse()
        two_list.reverse()
        third_list.reverse()
        four_list.reverse()
        five_list.reverse()
        six_list.reverse()
        seven_list.reverse()
        eight_list.reverse()
        ten_list.reverse()
        twelve_list.reverse()
        fourteen_list.reverse()
        sixteen_list.reverse()
        twenty_list.reverse()
        thirty_list.reverse()
        pay_count_list.reverse()

        return {"date_list":date_list, "short_date_list":short_date_list, "pay_list":pay_list, "new_user_list":new_user_list, "act_user_list":act_user_list,
                "new_device_list":new_device_list, "act_device_list":act_device_list, "pay_count_list":pay_count_list,"next_list":next_list, "third_list":third_list,
                "two_list":two_list,"four_list":four_list,"five_list":five_list,"six_list":six_list,"eight_list":eight_list,"ten_list":ten_list,"twelve_list":twelve_list,
                "sixteen_list":sixteen_list,"twenty_list":twenty_list,
                "seven_list":seven_list, "fourteen_list":fourteen_list, "thirty_list":thirty_list,"total_pay":total_pay, "total_new_user":total_new_user, "total_act_user":total_act_user,
                "total_act_device":total_act_device, "total_new_device":total_new_device, "sum_keep_next":sum_keep_next, "sum_keep_third":sum_keep_third, "sum_keep_seven":sum_keep_seven,
                "sum_keep_two":sum_keep_two,"sum_keep_four":sum_keep_four,"sum_keep_five":sum_keep_five,"sum_keep_six":sum_keep_six,"sum_keep_eight":sum_keep_eight,"sum_keep_ten":sum_keep_ten,
                "sum_keep_twelve":sum_keep_twelve,"sum_keep_sixteen":sum_keep_sixteen,"sum_keep_twenty":sum_keep_twenty,
                "sum_keep_fourteen":sum_keep_fourteen, "sum_keep_thirty":sum_keep_thirty,"pay_list_graph":pay_list_graph, "sum_new_user":sum_new_user, "sum_act_user":sum_act_user
                }

    #游戏渠道数据
    def get_app_channel_data(self, app_id=0, start_date="", end_date="", channels=None):

        must_con = [
            {"term": {"app_id": app_id}},
            {"term": {"stats_type": 1}},
            {"range": {
                "time": {
                    "gte": start_date,
                    "lte": end_date
                }}
            }
        ]

        if channels is not None:
            must_con.append({"terms": {"channel": channels}})

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
                "channel": {
                    "terms": {
                        "field": "channel",
                        "size": 0,
                        "order": {"sum_new_user":"desc"}
                    },
                    "aggs":{
                        "sum_pay": {
                            "sum": {
                                "field": "pay_money"
                            }
                        },
                        "sum_new_user": {
                            "sum": {
                                "field": "new_user"
                            }
                        },
                        "sum_new_device": {
                            "sum": {
                                "field": "new_device"
                            }
                        },
                        "sum_new_role": {
                            "sum": {
                                "field": "new_role"
                            }
                        },
                        "sum_act_user": {
                            "sum": {
                                "field": "act_user"
                            }
                        },
                        "sum_act_device": {
                            "sum": {
                                "field": "act_device"
                            }
                        },
                        "sum_pay_count": {
                            "sum": {
                                "field": "pay_count"
                            }
                        },
                        "sum_keep_next": {
                            "sum": {
                                "field": "keep_next"
                            }
                        },
                        "sum_keep_third": {
                            "sum": {
                                "field": "keep_third"
                            }
                        },
                        "sum_keep_seven": {
                            "sum": {
                                "field": "keep_seven"
                            }
                        },
                        "sum_keep_fourteen": {
                            "sum": {
                                "field": "keep_fourteen"
                            }
                        },
                        "sum_keep_thirty": {
                            "sum": {
                                "field": "keep_thirty"
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        sum_act_user = []
        sum_new_user = []
        sum_user_channel = []

        sum_act_others = 0
        sum_new_others = 0
        for source in result['aggregations']['channel']['buckets']:
            #大于30个渠道的时候后续算其它总和
            if len(sum_act_user) <= 30:
                sum_user_channel.append(str(source['key']))
                sum_act_user.append(int(source['sum_act_user']['value']))
                sum_new_user.append(int(source['sum_new_user']['value']))
            else:
                sum_act_others += int(source['sum_act_user']['value'])
                sum_new_others += int(source['sum_new_user']['value'])

        if len(sum_act_user) > 30:
            sum_user_channel.append('others')
            sum_act_user.append(sum_act_others)
            sum_new_user.append(sum_new_others)

        return {"result":result['aggregations']['channel']['buckets'], "sum_user_channel":sum_user_channel, "sum_act_user":sum_act_user,"sum_new_user":sum_new_user}

    def get_app_channel_data_limited(self, app_id=0, start_date="", end_date="", keep="keep_next"):

        body = {
            "size": 0,
            "query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"app_id": app_id}},
                                {"term": {"stats_type": 1}},
                                {"range": {
                                    "time": {
                                        "gte": start_date,
                                        "lte": end_date
                                    }}
                                }
                            ]
                        }
                    }
                }
            },
            "aggs": {
                "channel": {
                    "terms": {
                        "field": "channel",
                        "size": 0,
                        "order": {"sum_new_user":"desc"}
                    },
                    "aggs":{
                        "sum_new_user": {
                            "sum": {
                                "field": "new_user"
                            }
                        },
                        "sum_keep": {
                            "sum": {
                                "field": keep
                            }
                        }
                    }
                }
            }
        }

        result = self.es.search(index=options.es_index, doc_type="rh_analyze_kpi", body=body)

        return result['aggregations']['channel']['buckets']