采用restful方式，访问另一台windows机器上的1430端口。
服务运行:
    
    from vnpy.api.easytrader import server
    server.run(port=1430)

    资金账号和密码，通过http request的方式请求，无需在服务器中指定；
    国金证券的'全能行证券交易终端'安装在服务器缺省的目录。
    
