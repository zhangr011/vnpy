股票类相关数据接口


基础数据
    
    
    获取/更新股票的基本资料
    参考/下载: http://baostock.com/baostock/index.php/%E8%AF%81%E5%88%B8%E5%9F%BA%E6%9C%AC%E8%B5%84%E6%96%99
    python stock_base.py
    保存二进制对象dict{vt_symbol: dict} => stock_base.pkb2
    

除权除息
    
    更新股票除权除息信息 (2006年)
    参考/下载: http://baostock.com/baostock/index.php/%E9%99%A4%E6%9D%83%E9%99%A4%E6%81%AF%E4%BF%A1%E6%81%AF
    python stock_dividend.py     
    保存csv文件=> stock_dividend.csv
    
复权因子
    
    获取/更新股票复权因子(2006年开始)
    参考/下载: http://baostock.com/baostock/index.php/%E5%A4%8D%E6%9D%83%E5%9B%A0%E5%AD%90%E4%BF%A1%E6%81%AF
    python adjust_factor.py
    保存二进制对象 dict {vt_symbol, []} => stock_adjust_factor.pkb2


5分钟K线数据
    
    

