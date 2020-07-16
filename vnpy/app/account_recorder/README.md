账号信息 sync=> mongodb
    
    1. 监听 EVENT_ACCOUNT事件，数据 => mongodb Account.account_info 和 daily_info
    2. 监听 EVENT_ORDER  事件，数据 => mongodb Account.today_orders, 若激活copy_history_orders, 数据=>history_orders
    3. 监听 EVENT_TRADE  事件，数据 => mongodb Account.today_trades, 若激活copy_history_trades，数据=>history_trades
    4. 监听 EVENT_POSITION事件,数据 => mongodb Account.today_position
    5. 监听股票接口的 EVENT_HISTORY_TRADE 事件， 数据 => history_orders
    6. 监听股票接口的 EVENT_HISTORY_ORDER 事件， 数据 => history_trades
    7. 监听股票接口的 EVENT_FUNDS_FLOW 事件， 数据 => funds_flow
    8. 监听 EVENT_STRATEGY_POS事件，数据 =》 mongodb Account.today_strategy_pos
    9. 监听 EVENT_STRATEGY_SNAPSHOT事件， 数据=》 mongodb Account.strategy_snapshot

配置文件 ar_setting.json
    
    {
      "mongo_db":
      {
        "host": "192.168.0.207",
        "port": 27017
      },
      "accounts":
      {
        "stock":
        { "history_orders": {"begin_day": "2019-07-19"},
          "history_trades": {"begin_day": "2019-07-19"}
        },
        "ctp":
        {
          "copy_history_trades": true,
          "copy_history_orders": true
        }
      }
    }

# 创建mongodb 索引，提高性能

db.today_orders.createIndex({'account_id':1,'vt_symbol':1,'sys_orderid':1,'order_date':1,'holder_id':1},{'name':'accountid_vtsymbol_sysorderid_order_date_holder_id','unique':true})
db.history_orders.createIndex({'account_id':1,'vt_symbol':1,'sys_orderid':1,'order_date':1,'holder_id':1},{'name':'history_accountid_vtsymbol_sysorderid_order_date_holder_id'})
db.today_trades.createIndex({'account_id':1,'vt_symbol':1,'vt_tradeid':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtSymbol_vt_tradeid_trade_date_holder_id','unique':true})
db.history_trades.createIndex({'account_id':1,'vt_symbol':1,'vt_tradeid':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtSymbol_vt_tradeid_trade_date_holder_id'})
db.today_positions.createIndex({'account_id':1,'vt_symbol':1,'direction':1,'trade_date':1,'holder_id':1},{'name':'accountid_vtsymbol_direction_trade_date_holder_id'})
db.today_strategy_pos.createIndex({'account_id':1,'strategy_group':1,'strategy_name':1,'date':1},{'name':'accountid_strategy_group_strategy_name_date'})
db.strategy_snapshot.createIndex({'account_id':1,'strategy_group':1,'strategy':1,'guid':1},{'name':'accountid_strategy_name_guid'})
