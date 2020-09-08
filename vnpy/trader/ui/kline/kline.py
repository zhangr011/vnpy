# flake8: noqa
"""
Python K线模块,包含十字光标和鼠标键盘交互
Support By 量投科技(http://www.quantdo.com.cn/)

修改by：华富资产，李来佳
20180515 change log:
1.修改命名规则，将图形划分为 主图(main),副图1（volume)，副图2：（sub），能够设置开关显示/关闭 两个副图。区分indicator/signal
2.修改鼠标滚动得操作，去除focus，增加双击鼠标事件
3.增加重定位接口，供上层界面调用，实现多周期窗口的时间轴同步。

"""

# Qt相关和十字光标
import sys
import os
import traceback
import copy
import numpy as np
import pandas as pd
import pyqtgraph as pg

from functools import partial
from datetime import datetime
from collections import deque, OrderedDict
from qtpy import QtGui, QtCore, QtWidgets

# 其他
from vnpy.trader.ui.kline.crosshair import Crosshair
from vnpy.trader.constant import Direction, Offset


########################################################################
# 键盘鼠标功能
########################################################################
class KeyWraper(QtWidgets.QWidget):
    """键盘鼠标功能支持的窗体元类"""

    # 初始化

    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)

        # 定时器（for 鼠标双击）
        self.timer = QtCore.QTimer()
        self.timer.setInterval(300)
        self.timer.setSingleShot(True)
        self.timer.timeout.connect(self.timeout)
        self.click_count = 0  # 鼠标点击次数
        self.pos = None  # 鼠标点击的位置

        # 激活鼠标跟踪功能
        self.setMouseTracking(True)

    def timeout(self):
        """鼠标双击定时检查"""
        if self.click_count == 1 and self.pos is not None:
            self.onLClick(self.pos)

        self.click_count = 0
        self.pos = None

    def keyPressEvent(self, event):
        """
        重载方法keyPressEvent(self,event),即按键按下事件方法
        :param event:
        :return:
        """
        if event.key() == QtCore.Qt.Key_Up:
            self.onUp()
        elif event.key() == QtCore.Qt.Key_Down:
            self.onDown()
        elif event.key() == QtCore.Qt.Key_Left:
            self.onLeft()
        elif event.key() == QtCore.Qt.Key_Right:
            self.onRight()
        elif event.key() == QtCore.Qt.Key_PageUp:
            self.onPre()
        elif event.key() == QtCore.Qt.Key_PageDown:
            self.onNxt()
        event.accept()

    def mousePressEvent(self, event):
        """
        重载方法mousePressEvent(self,event),即鼠标点击事件方法
        :param event:
        :return:
        """
        if event.button() == QtCore.Qt.RightButton:
            self.onRClick(event.pos())
        elif event.button() == QtCore.Qt.LeftButton:
            self.click_count += 1
            if not self.timer.isActive():
                self.timer.start()

            if self.click_count > 1:
                self.onDoubleClick(event.pos())
            else:
                self.pos = event.pos()

        event.accept()

    def mouseRelease(self, event):
        """
        重载方法mouseReleaseEvent(self,event),即鼠标点击事件方法
        :param event:
        :return:
        """
        if event.button() == QtCore.Qt.RightButton:
            self.onRRelease(event.pos())
        elif event.button() == QtCore.Qt.LeftButton:
            self.onLRelease(event.pos())
        self.releaseMouse()

    def wheelEvent(self, event):
        """
        重载方法wheelEvent(self,event),即滚轮事件方法
        :param event:
        :return:
        """
        try:
            pos = event.angleDelta()
            if pos.y() > 0:
                self.onUp()
            else:
                self.onDown()
            event.accept()
        except Exception as ex:
            print(u'wheelEvent exception:{},{}'.format(str(ex), traceback.format_exc()))

    def paintEvent(self, event):
        """
        重载方法paintEvent(self,event),即拖动事件方法
        :param event:
        :return:
        """
        self.onPaint()
        event.accept()

    # PgDown键

    def onNxt(self):
        pass

    # PgUp键

    def onPre(self):
        pass

    # 向上键和滚轮向上

    def onUp(self):
        pass

    # 向下键和滚轮向下

    def onDown(self):
        pass

    # 向左键

    def onLeft(self):
        pass

    # 向右键

    def onRight(self):
        pass

    # 鼠标左单击

    def onLClick(self, pos):
        print('single left click')

    # 鼠标右单击

    def onRClick(self, pos):
        pass

    def onDoubleClick(self, pos):
        print('double click')

    # 鼠标左释放

    def onLRelease(self, pos):
        pass

    # 鼠标右释放

    def onRRelease(self, pos):
        pass

    # 画图

    def onPaint(self):
        pass


# 选择缩放功能支持
class CustomViewBox(pg.ViewBox):

    def __init__(self, *args, **kwds):
        pg.ViewBox.__init__(self, *args, **kwds)
        # 拖动放大模式
        # self.setMouseMode(self.RectMode)

    # 右键自适应

    def mouseClickEvent(self, ev):
        if ev.button() == QtCore.Qt.RightButton:
            self.autoRange()


class MyStringAxis(pg.AxisItem):
    """
    时间序列横坐标支持
    changelog: by  李来佳
    增加时间与x轴的双向映射
    """

    # 初始化

    def __init__(self, xdict, *args, **kwargs):
        pg.AxisItem.__init__(self, *args, **kwargs)
        self.minVal = 0
        self.maxVal = 0
        # 序列 <= > 时间
        self.xdict = OrderedDict()
        self.xdict.update(xdict)
        # 时间 <=> 序列
        self.tdict = OrderedDict([(v, k) for k, v in xdict.items()])
        self.x_values = np.asarray(xdict.keys())
        self.x_strings = list(xdict.values())
        self.setPen(color=(255, 255, 255, 255), width=0.8)
        self.setStyle(tickFont=QtGui.QFont("Roman times", 10, QtGui.QFont.Bold), autoExpandTextSpace=True)

    def update_xdict(self, xdict):
        """
        更新坐标映射表
        :param xdict:
        :return:
        """
        # 更新 x轴-时间映射
        self.xdict.update(xdict)
        # 更新 时间-x轴映射
        tdict = dict([(v, k) for k, v in xdict.items()])
        self.tdict.update(tdict)

        # 重新生成x轴队列和时间字符串显示队列
        self.x_values = np.asarray(self.xdict.keys())
        self.x_strings = list(self.xdict.values())

    def get_x_by_time(self, t_value):
        """
        通过 时间，找到匹配或最接近x轴
        :param t_value: datetime 类型时间
        :return:
        """
        last_time = None
        for t in self.x_strings:
            if last_time is None:
                last_time = t
                continue
            if t > t_value:
                break
            last_time = t

        x = self.tdict.get(last_time, 0)
        return x

    def tickStrings(self, values, scale, spacing):
        """
        将原始横坐标转换为时间字符串,第一个坐标包含日期
        :param values:
        :param scale:
        :param spacing:
        :return:
        """
        strings = []
        for v in values:
            vs = v * scale
            if vs in self.x_values:
                vstr = self.x_strings[np.abs(self.x_values - vs).argmin()]
                vstr = vstr.strftime('%Y-%m-%d %H:%M:%S')
            else:
                vstr = ""
            strings.append(vstr)
        return strings


class CandlestickItem(pg.GraphicsObject):
    """K线图形对象"""

    # 初始化

    def __init__(self, data):
        """初始化"""
        pg.GraphicsObject.__init__(self)
        # 数据格式: [ (time, open, close, low, high),...]
        self.data = data
        # 只重画部分图形，大大提高界面更新速度
        self.rect = None
        self.picture = None
        self.setFlag(self.ItemUsesExtendedStyleOption)
        # 画笔和画刷
        w = 0.4
        self.offset = 0
        self.low = 0
        self.high = 1
        self.picture = QtGui.QPicture()
        self.pictures = []
        self.bPen = pg.mkPen(color=(0, 240, 240, 255), width=w * 2)  # 阴线画笔
        self.bBrush = pg.mkBrush((0, 240, 240, 255))  # 阴线主体
        self.rPen = pg.mkPen(color=(255, 60, 60, 255), width=w * 2)  # 阳线画笔
        self.rBrush = pg.mkBrush((255, 60, 60, 255))  # 阳线主体
        self.rBrush.setStyle(QtCore.Qt.NoBrush)
        # 刷新K线
        self.generatePicture(self.data)

    # 画K线

    def generatePicture(self, data=None, redraw=False):
        """重新生成图形对象"""
        # 重画或者只更新最后一个K线
        if redraw:
            self.pictures = []
        elif self.pictures:
            self.pictures.pop()
        w = 0.4
        bPen = self.bPen
        bBrush = self.bBrush
        rPen = self.rPen
        rBrush = self.rBrush
        low, high = (data[0]['low'], data[0]['high']) if len(data) > 0 else (0, 1)
        for (t, open0, close0, low0, high0) in data:
            # t 并不是时间，是序列
            if t >= len(self.pictures):
                # 每一个K线创建一个picture
                picture = QtGui.QPicture()
                p = QtGui.QPainter(picture)
                low, high = (min(low, low0), max(high, high0))

                # 下跌蓝色（实心）, 上涨红色（空心）
                pen, brush, pmin, pmax = (bPen, bBrush, close0, open0) \
                    if open0 > close0 else (rPen, rBrush, open0, close0)
                p.setPen(pen)
                p.setBrush(brush)

                # 画K线方块和上下影线
                if open0 == close0:
                    p.drawLine(QtCore.QPointF(t - w, open0), QtCore.QPointF(t + w, close0))
                else:
                    p.drawRect(QtCore.QRectF(t - w, open0, w * 2, close0 - open0))
                if pmin > low0:
                    p.drawLine(QtCore.QPointF(t, low0), QtCore.QPointF(t, pmin))
                if high0 > pmax:
                    p.drawLine(QtCore.QPointF(t, pmax), QtCore.QPointF(t, high0))

                p.end()
                # 添加到队列中
                self.pictures.append(picture)

        # 更新所有K线的最高/最低
        self.low, self.high = low, high

    # 手动重画

    def update(self):
        if not self.scene() is None:
            self.scene().update()

    # 自动重画

    def paint(self, painter, opt, w):
        # 获取显示区域
        rect = opt.exposedRect
        # 获取显示区域/数据的滑动最小值/最大值，即需要显示的数据最小值/最大值。
        xmin, xmax = (max(0, int(rect.left())), min(int(len(self.pictures)), int(rect.right())))

        # 区域发生变化，或者没有最新图片（缓存），重画
        if not self.rect == (rect.left(), rect.right()) or self.picture is None:
            # 更新显示区域
            self.rect = (rect.left(), rect.right())
            # 重画，并缓存为最新图片
            self.picture = self.createPic(xmin, xmax)
            self.picture.play(painter)

        # 存在缓存，直接显示出来
        elif self.picture:
            self.picture.play(painter)

    # 缓存图片

    def createPic(self, xmin, xmax):
        picture = QtGui.QPicture()
        p = QtGui.QPainter(picture)
        # 全部数据，[xmin~xmax]的k线，重画一次
        [pic.play(p) for pic in self.pictures[xmin:xmax]]
        p.end()
        return picture

    # 定义显示边界，x轴：0~K线数据长度；Y轴，最低值~最高值-最低值

    def boundingRect(self):
        return QtCore.QRectF(0, self.low, len(self.pictures), (self.high - self.low))


########################################################################
class KLineWidget(KeyWraper):
    """用于显示价格走势图"""

    # 是否完成了历史数据的读取
    initCompleted = False
    clsId = 0

    def __init__(self, parent=None, display_vol=False, display_sub=False, **kargs):
        """Constructor"""
        self.parent = parent
        super(KLineWidget, self).__init__(parent)

        # 当前序号
        self.index = None  # 下标
        self.countK = 60  # 显示的Ｋ线数量范围

        KLineWidget.clsId += 1
        self.windowId = str(KLineWidget.clsId)

        self.title = u'KLineWidget'

        # # 保存K线数据的列表和Numpy Array对象
        self.datas = []  # 'datetime','open','close','low','high','volume','openInterest
        self.listBar = []  # 蜡烛图使用的Bar list :'time_int','open','close','low','high'
        self.listVol = []  # 成交量（副图使用）的 volume list

        # 交易事务有关的线段
        self.list_trans = []  # 交易事务( {'start_time','end_time','tns_type','start_price','end_price','start_x','end_x','completed'}

        # 交易记录相关的箭头
        self.list_trade_arrow = []  # 交易图标 list
        self.x_t_trade_map = OrderedDict()  # x 轴 与交易信号的映射
        self.t_trade_dict = OrderedDict()  # t 时间的交易记录

        # 标记相关
        self.list_markup = []
        self.x_t_markup_map = OrderedDict()  # x轴与标记的映射
        self.t_markup_dict = OrderedDict()  # t 时间的标记

        # 缠论相关的线段
        self.list_bi = []

        # 所有K线上指标
        self.main_color_pool = deque(['red', 'green', 'yellow', 'white'])
        self.main_indicator_data = {}  # 主图指标数据（字典，key是指标，value是list）
        self.main_indicator_colors = {}  # 主图指标颜色（字典，key是指标，value是list
        self.main_indicator_plots = {}  # 主图指标的所有画布（字典，key是指标，value是plot)

        self.display_vol = display_vol
        self.display_sub = display_sub

        # 所副图上信号图
        self.sub_color_pool = deque(['red', 'green', 'yellow', 'white'])
        self.sub_indicator_data = {}
        self.sub_indicator_colors = {}
        self.sub_indicator_plots = {}

        # 初始化完成
        self.initCompleted = False

        # 调用函数
        self.initUi()

        # 通知上层时间切换点的回调函数
        self.relocate_notify_func = None

    #  初始化相关
    def initUi(self):
        """
        初始化界面
        leyout 如下：
        ------------------————————
        \     主图(K线/主图指标/交易信号  \
        \                                 \
        -----------------------------------
        \     副图1（成交量)              \
        -----------------------------------
        \     副图2（持仓量/副图指标)     \
        -----------------------------------

        """
        self.setWindowTitle(u'K线工具')
        # 主图
        self.pw = pg.PlotWidget()
        # 界面布局
        self.lay_KL = pg.GraphicsLayout(border=(100, 100, 100))
        # self.lay_KL.setContentsMargins(10, 10, 10, 10)
        self.lay_KL.setContentsMargins(5, 5, 5, 5)
        self.lay_KL.setSpacing(0)
        self.lay_KL.setBorder(color=(100, 100, 100, 250), width=0.4)
        self.lay_KL.setZValue(0)
        self.KLtitle = self.lay_KL.addLabel(u'')
        self.pw.setCentralItem(self.lay_KL)
        # 设置横坐标
        xdict = {}
        self.axisTime = MyStringAxis(xdict, orientation='bottom')
        # 初始化子图
        self.init_plot_main()
        self.init_plot_volume()
        self.init_plot_sub()
        # 注册十字光标
        self.crosshair = Crosshair(self.pw, self)
        # 设置界面
        self.vb = QtWidgets.QVBoxLayout()
        self.vb.addWidget(self.pw)
        self.setLayout(self.vb)
        # 初始化完成
        self.initCompleted = True

    def create_plot_item(self, name):
        """生成PlotItem对象"""
        vb = CustomViewBox()
        plotItem = pg.PlotItem(viewBox=vb, name=name, axisItems={'bottom': self.axisTime})
        plotItem.setMenuEnabled(False)
        plotItem.setClipToView(True)
        plotItem.hideAxis('left')
        plotItem.showAxis('right')
        plotItem.setDownsampling(mode='peak')
        plotItem.setRange(xRange=(0, 1), yRange=(0, 1))
        plotItem.getAxis('right').setWidth(60)
        plotItem.getAxis('right').setStyle(tickFont=QtGui.QFont("Roman times", 10, QtGui.QFont.Bold))
        plotItem.getAxis('right').setPen(color=(255, 255, 255, 255), width=0.8)
        plotItem.showGrid(True, True)
        plotItem.hideButtons()
        return plotItem

    def init_plot_main(self):
        """
        初始化主图
        1、添加 K线（蜡烛图）
        :return:
        """
        # 创建K线PlotItem
        self.pi_main = self.create_plot_item('_'.join([self.windowId, 'Plot_Main']))

        # 创建蜡烛图
        self.ci_candle = CandlestickItem(self.listBar)

        # 添加蜡烛图到主图
        self.pi_main.addItem(self.ci_candle)
        self.pi_main.setMinimumHeight(200)
        self.pi_main.setXLink('_'.join([self.windowId, 'Plot_Sub']))
        self.pi_main.hideAxis('bottom')

        # 添加主图到window layout
        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pi_main)

    def init_plot_volume(self):
        """
        初始化成交量副图
        :return:
        """

        # 创建plot item
        self.pi_volume = self.create_plot_item('_'.join([self.windowId, 'Plot_Volume']))

        if self.display_vol:
            # 以蜡烛图（柱状图）的形式创建成交量图形对象
            self.ci_volume = CandlestickItem(self.listVol)

            # 副图1，添加成交量子图
            self.pi_volume.addItem(self.ci_volume)
            self.pi_volume.setMaximumHeight(150)
            self.pi_volume.setXLink('_'.join([self.windowId, 'Plot_Sub']))
            self.pi_volume.hideAxis('bottom')
        else:
            self.pi_volume.setMaximumHeight(1)
            self.pi_volume.setXLink('_'.join([self.windowId, 'Plot_Sub']))
            self.pi_volume.hideAxis('bottom')

        # 添加副图1到window layout
        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pi_volume)

    def init_plot_sub(self):
        """
        初始化副图（只有一个图层）
        :return:
        """
        self.pi_sub = self.create_plot_item('_'.join([self.windowId, 'Plot_Sub']))

        if self.display_sub:
            # 副图的plot对象
            self.curve_sub = self.pi_sub.plot()
        else:
            self.pi_sub.setMaximumHeight(1)
            self.pi_sub.setXLink('_'.join([self.windowId, 'Plot_Sub']))
            self.pi_sub.hideAxis('bottom')

        # 添加副图到窗体layer中
        self.lay_KL.nextRow()
        self.lay_KL.addItem(self.pi_sub)

    #  画图相关

    def plot_volume(self, redraw=False, xmin=0, xmax=-1):
        """重画成交量子图"""
        if self.initCompleted:
            self.ci_volume.generatePicture(self.listVol[xmin:xmax], redraw)  # 画成交量子图

    def plot_kline(self, redraw=False, xmin=0, xmax=-1):
        """重画K线子图"""
        if self.initCompleted:
            self.ci_candle.generatePicture(self.listBar[xmin:xmax], redraw)  # 画K线

            for indicator in list(self.main_indicator_data.keys()):
                if indicator in self.main_indicator_plots:
                    self.main_indicator_plots[indicator].setData(self.main_indicator_data[indicator],
                                                                 pen=self.main_indicator_colors[indicator][0],
                                                                 name=indicator)

    def plot_sub(self, xmin=0, xmax=-1):
        """重画持仓量子图"""
        if self.initCompleted:
            for indicator in list(self.sub_indicator_data.keys()):
                # 调用该信号/指标画布(plotDataItem.setData())，更新数据，更新画笔颜色，更新名称
                if indicator in self.sub_indicator_plots:
                    self.sub_indicator_plots[indicator].setData(self.sub_indicator_data[indicator],
                                                                pen=self.sub_indicator_colors[indicator][0],
                                                                name=indicator)

    def add_indicator(self, indicator, is_main=True):
        """
        新增指标信号图
        :param indicator: 指标/信号的名称，如ma10，
        :param is_main: 是否为主图
        :return:
        """
        if is_main:
            if indicator in self.main_indicator_plots:
                self.pi_main.removeItem(self.main_indicator_plots[indicator])  # 存在该指标/信号，先移除原有画布

            self.main_indicator_plots[indicator] = self.pi_main.plot()  # 为该指标/信号，创建新的主图画布，登记字典
            self.main_indicator_colors[indicator] = self.main_color_pool[0]  # 登记该指标/信号使用的颜色
            self.main_color_pool.append(self.main_color_pool.popleft())  # 调整剩余颜色
            if indicator not in self.main_indicator_data:
                self.main_indicator_data[indicator] = []
        else:
            if indicator in self.sub_indicator_plots:
                self.pi_sub.removeItem(self.sub_indicator_plots[indicator])  # 若存在该指标/信号，先移除原有的附图画布
            self.sub_indicator_plots[indicator] = self.pi_sub.plot()  # 为该指标/信号，创建新的主图画布，登记字典
            self.sub_indicator_colors[indicator] = self.sub_color_pool[0]  # 登记该指标/信号使用的颜色
            self.sub_color_pool.append(self.sub_color_pool.popleft())  # 调整剩余颜色
            if indicator not in self.sub_indicator_data:
                self.sub_indicator_data[indicator] = []

    def plot_indicator(self, datas, is_main=True, clear=False):
        """
        刷新指标/信号图( 新数据）
        :param datas: 所有数据
        :param is_main: 是否为主图
        :param clear: 是否要清除旧数据
        :return:
        """
        if clear:
            self.clear_indicator(is_main)  # 清除主图/副图

        if is_main:
            for indicator in datas:
                self.add_indicator(indicator, is_main)  # 逐一添加主图信号/指标
                self.main_indicator_data[indicator] = datas[indicator]  # 更新组件数据字典
                # 调用该信号/指标画布(plotDataItem.setData())，更新数据，更新画笔颜色，更新名称
                self.main_indicator_plots[indicator].setData(datas[indicator],
                                                             pen=self.main_indicator_colors[indicator][0],
                                                             name=indicator)
        else:
            for indicator in datas:
                self.add_indicator(indicator, is_main)  # 逐一增加子图指标/信号
                self.sub_indicator_data[indicator] = datas[indicator]  # 更新组件数据字典
                # 调用该信号/指标画布(plotDataItem.setData())，更新数据，更新画笔颜色，更新名称
                self.sub_indicator_plots[indicator].setData(datas[indicator],
                                                            pen=self.sub_indicator_colors[indicator][0], name=indicator)

    def update_all(self):
        """
        手动更新所有K线图形，K线播放模式下需要
        """
        datas = self.datas

        if self.display_vol:
            self.ci_volume.pictrue = None
            self.ci_volume.update()

        self.ci_candle.pictrue = None
        self.ci_candle.update()

        def update(view, low, high):
            """
            更新视图
            :param view: viewbox
            :param low:
            :param high:
            :return:
            """
            vRange = view.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            xmax = min(xmax, len(datas))
            if len(datas) > 0 and xmax > xmin:
                ymin = min(datas[xmin:xmax][low])
                ymax = max(datas[xmin:xmax][high])
                view.setRange(yRange=(ymin, ymax))
            else:
                view.setRange(yRange=(0, 1))

        update(self.pi_main.getViewBox(), 'low', 'high')
        update(self.pi_volume.getViewBox(), 'volume', 'volume')

    def plot_all(self, redraw=True, xMin=0, xMax=-1):
        """
        重画所有界面
        redraw ：False=重画最后一根K线; True=重画所有
        xMin,xMax : 数据范围
        """

        xMax = len(self.datas) if xMax < 0 else xMax
        self.countK = xMax - xMin
        self.index = int((xMax + xMin) / 2)  # 设置当前索引所在位置为数据的中心点
        self.pi_sub.setLimits(xMin=xMin, xMax=xMax)
        self.pi_main.setLimits(xMin=xMin, xMax=xMax)
        self.plot_kline(redraw, xMin, xMax)  # K线图

        if self.display_vol:
            self.pi_volume.setLimits(xMin=xMin, xMax=xMax)
            self.plot_volume(redraw, xMin, xMax)  # K线副图，成交量

        self.plot_sub(0, len(self.datas))  # K线副图，持仓量
        self.refresh()

    def refresh(self):
        """
        刷新三个子图的显示范围
        """
        # 计算界面上显示数量的最小x/最大x
        minutes = int(self.countK / 2)
        xmin = max(0, self.index - minutes)
        xmax = xmin + 2 * minutes

        # 更新主图/副图/成交量的 x范围
        self.pi_sub.setRange(xRange=(xmin, xmax))
        self.pi_main.setRange(xRange=(xmin, xmax))
        self.pi_volume.setRange(xRange=(xmin, xmax))

    #  快捷键相关

    def onNxt(self):
        """跳转到下一个开平仓点"""
        try:
            if len(self.x_t_trade_map) > 0 and self.index is not None:
                datalen = len(self.datas)
                self.index += 1
                while self.index < datalen and self.index in self.x_t_trade_map:
                    self.index += 1
                self.refresh()
                x = self.index
                y = self.datas[x]['close']
                self.crosshair.signal.emit((x, y))
        except Exception as ex:
            print(u'{} onDown() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onPre(self):
        """跳转到上一个开平仓点"""
        try:
            if len(self.x_t_trade_map) > 0 and self.index:
                self.index -= 1
                while self.index > 0 and self.index in self.x_t_trade_map:
                    self.index -= 1
                self.refresh()
                x = self.index
                y = self.datas[x]['close']
                self.crosshair.signal.emit((x, y))
        except Exception as ex:
            print(u'{}.onDown() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onDown(self):
        """放大显示区间"""
        try:
            self.countK = min(len(self.datas), int(self.countK * 1.2) + 1)
            self.refresh()
            if len(self.datas) > 0:
                x = self.index - self.countK / 2 + 2 if int(
                    self.crosshair.xAxis) < self.index - self.countK / 2 + 2 else int(self.crosshair.xAxis)
                x = self.index + self.countK / 2 - 2 if x > self.index + self.countK / 2 - 2 else x
                x = int(x)
                y = self.datas[x][2]
                self.crosshair.signal.emit((x, y))
            print(u'onDown:countK:{},x:{},y:{},index:{}'.format(self.countK, x, y, self.index))
        except Exception as ex:
            print(u'{}.onDown() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onUp(self):
        """缩小显示区间"""
        try:
            # 减少界面显示K线数量
            self.countK = max(3, int(self.countK / 1.2) - 1)
            self.refresh()
            if len(self.datas) > 0:
                x = self.index - int(self.countK / 2) + 2 if int(self.crosshair.xAxis) < self.index - int(
                    self.countK / 2) + 2 else int(self.crosshair.xAxis)
                x = self.index + int(self.countK / 2) - 2 if x > self.index + (self.countK / 2) - 2 else x
                x = int(x)
                y = self.datas[x]['close']
                self.crosshair.signal.emit((x, y))
            print(u'onUp:countK:{},x:{},y:{},index:{}'.format(self.countK, x, y, self.index))
        except Exception as ex:
            print(u'{}.onDown() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onLeft(self):
        """向左移动"""
        try:
            if len(self.datas) > 0 and int(self.crosshair.xAxis) > 2:
                x = int(self.crosshair.xAxis) - 1
                y = self.datas[x]['close']
                if x <= self.index - self.countK / 2 + 2 and self.index > 1:
                    self.index -= 1
                    self.refresh()
                self.crosshair.signal.emit((x, y))

                print(u'onLeft:countK:{},x:{},y:{},index:{}'.format(self.countK, x, y, self.index))
        except Exception as ex:
            print(u'{}.onLeft() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onRight(self):
        """向右移动"""
        try:
            if len(self.datas) > 0 and int(self.crosshair.xAxis) < len(self.datas) - 1:
                x = int(self.crosshair.xAxis) + 1
                y = self.datas[x]['close']
                if x >= self.index + int(self.countK / 2) - 2:
                    self.index += 1
                    self.refresh()
                self.crosshair.signal.emit((x, y))
        except Exception as ex:
            print(u'{}.onLeft() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def onDoubleClick(self, pos):
        """
        鼠标双击事件
        :param pos:
        :return:
        """
        try:
            if len(self.datas) > 0 and int(self.crosshair.xAxis) >= 0:
                x = int(self.crosshair.xAxis)
                time_value = self.axisTime.xdict.get(x, None)
                self.index = x

                print(u'{} doubleclick: {},x:{},index:{}'.format(self.title, time_value, x, self.index))

                if self.relocate_notify_func is not None and time_value is not None:
                    self.relocate_notify_func(self.windowId, time_value, self.countK)
        except Exception as ex:
            print(u'{}.onDoubleClick() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    def relocate(self, window_id, t_value, count_k):
        """
        重定位到最靠近t_value的x坐标
        :param window_id:
        :param t_value:
        :param count_k
        :return:
        """
        if self.windowId == window_id or count_k < 2:
            return

        try:
            x_value = self.axisTime.get_x_by_time(t_value)
            self.countK = count_k

            if 0 < x_value <= len(self.datas):
                self.index = x_value
                x = self.index
                y = self.datas[x]['close']
                self.refresh()
                self.crosshair.signal.emit((x, y))
                print(u'{} reloacate to :{},{}'.format(self.title, x, y))
        except Exception as ex:
            print(u'{}.relocate() exception:{},trace:{}'.format(self.title, str(ex), traceback.format_exc()))

    # 界面回调相关

    def onPaint(self):
        """界面刷新回调"""
        view = self.pi_main.getViewBox()
        vRange = view.viewRange()
        xmin = max(0, int(vRange[0][0]))
        xmax = max(0, int(vRange[0][1]))
        self.index = int((xmin + xmax) / 2) + 1

    def resignData(self, datas):
        """更新数据，用于Y坐标自适应"""
        self.crosshair.datas = datas

        def viewXRangeChanged(low, high, self):
            vRange = self.viewRange()
            xmin = max(0, int(vRange[0][0]))
            xmax = max(0, int(vRange[0][1]))
            xmax = min(xmax, len(datas))
            if len(datas) > 0 and xmax > xmin:
                ymin = min(datas[xmin:xmax][low])
                ymax = max(datas[xmin:xmax][high])
                self.setRange(yRange=(ymin, ymax))
            else:
                self.setRange(yRange=(0, 1))

        view = self.pi_main.getViewBox()
        view.sigXRangeChanged.connect(partial(viewXRangeChanged, 'low', 'high'))

        if self.display_vol:
            view = self.pi_volume.getViewBox()
            view.sigXRangeChanged.connect(partial(viewXRangeChanged, 'volume', 'volume'))
        if self.display_sub:
            view = self.pi_sub.getViewBox()
            # view.sigXRangeChanged.connect(partial(viewXRangeChanged,'openInterest','openInterest'))
            view.setRange(yRange=(0, 100))

    # 数据相关

    def clearData(self):
        """清空数据"""
        # 清空数据，重新画图
        self.time_index = []
        self.listBar = []
        self.listVol = []

        self.list_trade_arrow = []
        self.x_t_trade_map = OrderedDict()
        self.t_trade_dict = OrderedDict()

        self.list_trans = []

        self.list_markup = []
        self.x_t_markup_map = OrderedDict()
        self.t_markup_dict = OrderedDict()

        # 清空主图指标
        self.main_indicator_data = {}
        # 清空副图指标
        self.sub_indicator_data = {}

        self.datas = None

    def clear_indicator(self, main=True):
        """清空指标图形"""
        # 清空信号图
        if main:
            for indicator in self.main_indicator_plots:
                self.pi_main.removeItem(self.main_indicator_plots[indicator])
            self.main_indicator_data = {}
            self.main_indicator_plots = {}
        else:
            for indicator in self.sub_indicator_plots:
                self.pi_sub.removeItem(self.sub_indicator_plots[indicator])
            self.sub_indicator_data = {}
            self.sub_indicator_plots = {}

    def onBar(self, bar, main_indicator_datas, sub_indicator_datas, nWindow=20, inited=False):
        """
        新增K线数据,K线播放模式

        :param bar: dict
        :param main_indicator_datas:
        :param sub_indicator_datas:
        :param nWindow:
        :return: nWindow : 最大数据窗口
        """
        bar_datetime = bar.get('datetime', '')
        try:
            bar_datetime = datetime.strptime(bar_datetime, '%Y-%m-%d %H:%M:%S')
        except:  # noqa
            bar_datetime = datetime.now()
        bar_open = bar.get('open', 0)
        bar_close = bar.get('close', 0)
        bar_high = bar.get('high', 0)
        bar_low = bar.get('low', 0)
        bar_volume = bar.get('volume', 0)
        bar_openInterest = bar.get('openInterest')
        if bar_openInterest == np.inf or bar_openInterest == -np.inf:
            bar_openInterest = np.random.randint(0, 3)

        # 是否需要更新K线
        newBar = False if self.datas and bar_datetime == self.datas[-1].datetime else True
        nrecords = len(self.datas) if newBar else len(self.datas) - 1

        recordVol = (nrecords, bar_volume, 0, 0, bar_volume) if bar_close < bar_open else (
            nrecords, 0, bar_volume, 0, bar_volume)

        if newBar and any(self.datas):
            # 主图数据增加一项
            self.datas.resize(nrecords + 1, refcheck=0)
            self.listBar.resize(nrecords + 1, refcheck=0)
            # 成交量指标，增加一项
            self.listVol.resize(nrecords + 1, refcheck=0)

            # 主图指标，增加一项
            for indicator in list(self.main_indicator_data.keys()):
                indicator_data = self.main_indicator_data.get(indicator, [])
                indicator_data.append(0)

            # 副图指标，增加一行
            for indicator in list(self.sub_indicator_data.keys()):
                indicator_data = self.sub_indicator_data.get(indicator, [])
                indicator_data.append(0)

        elif any(self.datas):

            # 主图指标，移除第一项
            for indicator in list(self.main_indicator_data.keys()):
                indicator_data = self.main_indicator_data.get(indicator, [])
                indicator_data.pop()

            # 副图指标，移除第一项
            for indicator in list(self.sub_indicator_data.keys()):
                indicator_data = self.sub_indicator_data.get(indicator, [])
                indicator_data.pop()

        if any(self.datas):
            self.datas[-1] = (bar_datetime, bar_open, bar_close, bar_low, bar_high, bar_volume, bar_openInterest)
            self.listBar[-1] = (nrecords, bar_open, bar_close, bar_low, bar_high)
            self.listVol[-1] = recordVol

            # 主图指标，更新最后记录
            for indicator in list(self.main_indicator_data.keys()):
                indicator_data = self.main_indicator_data.get(indicator, [])
                indicator_data[-1] = main_indicator_datas.get(indicator, 0)

            # 副图指标，更新最后记录
            for indicator in list(self.sub_indicator_data.keys()):
                indicator_data = self.sub_indicator_data.get(indicator, [])
                indicator_data[-1] = sub_indicator_datas.get(indicator, 0)

        else:
            self.datas = np.rec.array(
                [(datetime, bar_open, bar_close, bar_low, bar_high, bar_volume, bar_openInterest)],
                names=('datetime', 'open', 'close', 'low', 'high', 'volume', 'openInterest'))
            self.listBar = np.rec.array([(nrecords, bar_open, bar_close, bar_low, bar_high)],
                                        names=('time_int', 'open', 'close', 'low', 'high'))
            self.listVol = np.rec.array([recordVol], names=('time_int', 'open', 'close', 'low', 'high'))

            # 主图指标，添加数据
            for indicator in list(self.main_indicator_data.keys()):
                indicator_data = self.main_indicator_data.get(indicator, [])
                indicator_data.append(main_indicator_datas.get(indicator, 0))

            # 副图指标，添加数据
            for indicator in list(self.sub_indicator_data.keys()):
                indicator_data = self.sub_indicator_data.get(indicator, [])
                indicator_data.append(sub_indicator_datas.get(indicator, 0))

            self.resignData(self.datas)

        self.axisTime.update_xdict({nrecords: bar_datetime})

        if 'openInterest' in self.sub_indicator_data:
            self.sub_indicator_data['openInterest'].append(bar_openInterest)

        self.resignData(self.datas)
        nWindow0 = min(nrecords, nWindow)
        xMax = nrecords + 2
        xMin = max(0, nrecords - nWindow0)
        if inited:
            self.plot_all(False, xMin, xMax)
        if not newBar:
            self.update_all()
        self.index = 0
        self.crosshair.signal.emit((None, None))

    def add_signal(self, t_value, direction, offset, price, volume):
        """
        增加信号
        :param t_value:
        :param direction:
        :param offset:
        :param price:
        :param volume:
        :return:
        """
        # 找到信号时间最贴近的bar x轴
        x = self.axisTime.get_x_by_time(t_value)
        need_plot_arrow = False

        # 修正一下 信号时间，改为bar的时间
        if x not in self.x_t_trade_map:
            bar_time = self.axisTime.xdict.get(x, t_value)
        else:
            # 如果存在映射，就更新
            bar_time = self.x_t_trade_map[x]

        trade_node = self.t_trade_dict.get(bar_time, None)
        if trade_node is None:
            # 当前时间无交易信号
            self.t_trade_dict[bar_time] = {'x': x, 'signals': [
                {'direction': direction, 'offset': offset, 'price': price, 'volume': volume}]}
            self.x_t_trade_map[x] = bar_time
            need_plot_arrow = True
        else:
            # match_signals = [t for t in trade_node['signals'] if t['direction'] == direction and t['offset'] == offset]
            # if len(match_signals) == 0:
            need_plot_arrow = True
            trade_node['signals'].append({'direction': direction, 'offset': offset, 'price': price, 'volume': volume})
            self.x_t_trade_map[x] = bar_time

        # 需要显示图标
        if need_plot_arrow:
            arrow = None
            # 多信号
            if direction == Direction.LONG:
                if offset == Offset.OPEN:
                    # buy
                    arrow = pg.ArrowItem(pos=(x, price), angle=135, brush=None, pen={'color': 'y', 'width': 2},
                                         tipAngle=30, baseAngle=20, tailLen=10, tailWidth=2)
                    # d = {
                    #    "pos": (x, price),
                    #    "data": 1,
                    #    "size": 14,
                    #    "pen": pg.mkPen((255, 255, 255)),
                    #    "symbol": "t1",
                    #    "brush": pg.mkBrush((255, 255, 0))
                    # }
                    # arrow = pg.ScatterPlotItem()
                    # arrow.setData([d])
                else:
                    # cover
                    arrow = pg.ArrowItem(pos=(x, price), angle=0, brush='y', pen=None, headLen=20, headWidth=20,
                                         tailLen=10, tailWidth=2)
            # 空信号
            elif direction == Direction.SHORT:
                if offset == Offset.CLOSE:
                    # sell
                    arrow = pg.ArrowItem(pos=(x, price), angle=0, brush='g', pen=None, headLen=20, headWidth=20,
                                         tailLen=10, tailWidth=2)
                else:
                    # short
                    arrow = pg.ArrowItem(pos=(x, price), angle=-135, brush=None, pen={'color': 'g', 'width': 2},
                                         tipAngle=30, baseAngle=20, tailLen=10, tailWidth=2)
            if arrow:
                self.pi_main.addItem(arrow)
                self.list_trade_arrow.append(arrow)

    def add_trades(self, df_trades):
        """
        批量导入交易记录（vnpy回测中导出的trade.csv)
        :param df_trades:
        :return:
        """
        if df_trades is None or len(df_trades) == 0:
            print(u'dataframe is None or Empty', file=sys.stderr)
            return

        if 'datetime' in df_trades.columns:
            col_datetime = 'datetime'
        else:
            col_datetime = 'time'
        for idx in df_trades.index:
            # 时间
            trade_time = df_trades[col_datetime].loc[idx]
            if not isinstance(trade_time, datetime) and isinstance(trade_time, str):
                trade_time = datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%S')

            price = df_trades['price'].loc[idx]
            direction = df_trades['direction'].loc[idx]
            if direction.lower() in ['long', 'direction.long', '多']:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            offset = df_trades['offset'].loc[idx]
            if offset.lower() in ['open', 'offset.open', '开']:
                offset = Offset.OPEN
            else:
                offset = Offset.CLOSE

            volume = df_trades['volume'].loc[idx]

            # 添加开仓信号
            self.add_signal(t_value=trade_time, direction=direction, offset=offset, price=price,
                            volume=volume)

    def add_signals(self, df_trade_list):
        """
        批量导入交易记录（vnpy回测中导出的trade_list.csv)
        :param df_trade_list:
        :return:
        """
        if df_trade_list is None or len(df_trade_list) == 0:
            print(u'dataframe is None or Empty', file=sys.stderr)
            return

        for idx in df_trade_list.index:
            # 开仓时间
            open_time = df_trade_list['open_time'].loc[idx]
            if not isinstance(open_time, datetime) and isinstance(open_time, str):
                open_time = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S')

            open_price = df_trade_list['open_price'].loc[idx]
            direction = df_trade_list['direction'].loc[idx]
            if direction.lower() == 'long':
                open_direction = Direction.LONG
                close_direction = Direction.SHORT
            else:
                open_direction = Direction.SHORT
                close_direction = Direction.LONG

            close_time = df_trade_list['close_time'].loc[idx]
            if not isinstance(close_time, datetime) and isinstance(close_time, str):
                close_time = datetime.strptime(close_time, '%Y-%m-%d %H:%M:%S')

            close_price = df_trade_list['close_price'].loc[idx]
            volume = df_trade_list['volume'].loc[idx]

            # 添加开仓信号
            self.add_signal(t_value=open_time, direction=open_direction, offset=Offset.OPEN, price=open_price,
                            volume=volume)

            # 添加平仓信号
            self.add_signal(t_value=close_time, direction=close_direction, offset=Offset.CLOSE, price=close_price,
                            volume=volume)

    def add_trans(self, tns_dict):
        """
        添加事务画线
        {'start_time','end_time','tns_type','start_price','end_price','start_x','end_x','completed'}
        :return:
        """
        if len(self.datas) == 0:
            print(u'No datas exist', file=sys.stderr)
            return
        tns = copy.copy(tns_dict)

        completed = tns.get('completed', False)
        end_price = tns.get('end_price', 0)
        if not completed:
            end_x = len(self.datas) - 1
            end_price = self.datas[end_x]['close']
            tns['end_x'] = end_x
            tns['end_price'] = end_price
            tns['completed'] = False
        else:
            tns['end_x'] = self.axisTime.get_x_by_time(tns['end_time'])

        tns['start_x'] = self.axisTime.get_x_by_time(tns['start_time'])
        # 将上一个线段设置为True
        if len(self.list_trans) > 0:
            self.list_trans[-1]['completed'] = True
        pos = np.array([[tns['start_x'], tns['start_price']], [tns['end_x'], tns['end_price']]])
        tns_type = tns.get('tns_type', None)
        if tns_type == Direction.LONG:
            pen = pg.mkPen({'color': 'r', 'width': 1})
        elif tns_type == Direction.SHORT:
            pen = pg.mkPen({'color': 'g', 'width': 1})
        else:
            pen = 'default'
        tns_line = pg.GraphItem(pos=pos, adj=np.array([[0, 1]]), pen=pen)
        self.pi_main.addItem(tns_line)
        self.list_trans.append(tns)

    def add_trans_df(self, df_trans):
        """
         批量增加,多空的切换
        :param df_trans:
        :return:
        """
        if df_trans is None or len(df_trans) == 0:
            print(u'dataframe is None or Empty', file=sys.stderr)
            return

        for idx in df_trans.index:
            if idx == 0:
                continue
            # 事务开始时间
            start_time = df_trans['datetime'].loc[idx - 1]
            if not isinstance(start_time, datetime) and isinstance(start_time, str):
                start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

            end_time = df_trans['datetime'].loc[idx]
            if not isinstance(end_time, datetime) and isinstance(end_time, str):
                end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

            tns_type = df_trans['direction'].loc[idx - 1]
            tns_type = Direction.LONG if tns_type.lower() == 'long' else Direction.SHORT

            start_price = df_trans['price'].loc[idx - 1]
            end_price = df_trans['price'].loc[idx]
            start_x = self.axisTime.get_x_by_time(start_time)
            end_x = self.axisTime.get_x_by_time(end_time)

            self.add_trans({'start_time': start_time, 'end_time': end_time,
                            'start_price': start_price, 'end_price': end_price,
                            'start_x': start_x, 'end_x': end_x,
                            'tns_type': tns_type, 'completed': True})

    def add_markup(self, t_value, price, txt):
        """
        添加标记
        :param t_value: 时间-》坐标x
        :param price: 坐标y
        :param txt: 文字
        :return:
        """
        # 找到信号时间最贴近的bar x轴
        x = self.axisTime.get_x_by_time(t_value)

        # 修正一下 标记时间，改为bar的时间
        if x not in self.x_t_markup_map:
            bar_time = self.axisTime.xdict.get(x, t_value)
        else:
            # 如果存在映射，就更新
            bar_time = self.x_t_markup_map[x]

        markup_node = self.t_markup_dict.get(bar_time, None)
        if markup_node is None:
            # 当前时间无标记
            markup_node = {'x': x, 'markup': [txt]}
            self.t_markup_dict[bar_time] = markup_node
            self.x_t_markup_map[x] = bar_time
        else:
            if '.' in txt:
                txt_list = txt.split('.')
            else:
                txt_list = [txt]

            for t in txt_list:
                if t in markup_node['markup']:
                    continue
                markup_node['markup'].append(t)

        if 'textitem' in markup_node:
            markup_node['textitem'].setText(';'.join(markup_node.get('markup', [])))
        else:
            textitem = pg.TextItem(markup_node['markup'][0])
            textitem.setPos(x, price)
            markup_node['textitem'] = textitem
            self.list_markup.append(textitem)
            self.pi_main.addItem(textitem)

    def add_markups(self, df_markup, include_list=[], exclude_list=[]):
        """
        批量增加标记
        :param df_markup: Dataframe(datetime, price, markup)
        :param include_list: 如果len(include_list)>0，只显示里面的内容
        :param exclude_list: 如果exclude_list里面存在，不显示
        :return:
        """
        if df_markup is None or len(df_markup) == 0:
            print(u'df_markup is None or Empty', file=sys.stderr)
            return

        for idx in df_markup.index:
            # 标记时间
            t_value = df_markup['datetime'].loc[idx]
            if not isinstance(t_value, datetime) and isinstance(t_value, str):
                t_value = datetime.strptime(t_value, '%Y-%m-%d %H:%M:%S')

            price = df_markup['price'].loc[idx]
            markup_text = df_markup['markup'].loc[idx]
            if '.' in markup_text:
                markup_texts = markup_text.split('.')
            else:
                markup_texts = [markup_text]

            for txt in markup_texts:
                if len(include_list) > 0 and markup_text not in include_list:
                    continue

                if len(exclude_list) > 0 and markup_text in exclude_list:
                    continue

                self.add_markup(t_value=t_value, price=price, txt=markup_text)


    def add_bi(self, df_bi, color='b', style= None):
        """
        添加缠论_笔（段）_画线
        # direction,(1/-1)，start, end, high, low
        # 笔： color = 'y', style: QtCore.Qt.DashLine
        # 段： color = 'b',
        :return:
        """
        if len(self.datas) == 0 or len(df_bi) == 0:
            print(u'No datas exist', file=sys.stderr)
            return

        for index, row in df_bi.iterrows():

            start_time = row['start']
            if not isinstance(start_time, datetime) and isinstance(start_time, str):
                start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

            end_time = row['end']
            if not isinstance(end_time, datetime) and isinstance(end_time, str):
                end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

            start_x = self.axisTime.get_x_by_time(start_time)
            end_x = self.axisTime.get_x_by_time(end_time)

            if int(row['direction']) == 1:
                pos = np.array([[start_x, row['low']], [end_x, row['high']]])
            elif int(row['direction']) == -1:
                pos = np.array([[start_x, row['high']], [end_x, row['low']]])
            else:
                continue

            if style:
                pen = pg.mkPen({'color': color, 'width': 1, 'style': QtCore.Qt.DashLine})
            else:
                pen = pg.mkPen({'color': color, 'width': 1})

            bi = pg.GraphItem(pos=pos, adj=np.array([[0, 1]]), pen=pen)
            self.pi_main.addItem(bi)


    def add_zs(self, df_zs, color='y'):
        """
        添加缠论中枢_画线
        # direction,(1/-1)，start, end, high, low
        # 笔中枢： color ='y'
        # 段中枢： color = 'b'
        :return:
        """
        if len(self.datas) == 0 or len(df_zs) == 0:
            print(u'No datas exist', file=sys.stderr)
            return

        for index,row in df_zs.iterrows():

            start_time = row['start']
            if not isinstance(start_time, datetime) and isinstance(start_time, str):
                start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')

            end_time = row['end']
            if not isinstance(end_time, datetime) and isinstance(end_time, str):
                end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

            start_x = self.axisTime.get_x_by_time(start_time)
            end_x = self.axisTime.get_x_by_time(end_time)

            pos_top = np.array([[start_x, row['high']], [end_x, row['high']]])
            pos_buttom = np.array([[start_x, row['low']], [end_x, row['low']]])
            pos_left = np.array([[start_x, row['high']], [start_x, row['low']]])
            pos_right = np.array([[end_x, row['high']], [end_x, row['low']]])

            pen = pg.mkPen({'color': color, 'width': 1})
            for pos in [pos_top, pos_buttom, pos_left, pos_right]:
                line = pg.GraphItem(pos=pos, adj=np.array([[0, 1]]), pen=pen)
                self.pi_main.addItem(line)


    def loadData(self, df_datas, main_indicators=[], sub_indicators=[]):
        """
        载入pandas.DataFrame数据
        :param df_datas:DataFrame数据格式，cols : datetime, open, close, low, high, ，，indicator，indicator2,indicator,,,
        :param main_indicators: 主图的indicator list
        :param sub_indicators:  副图的indicator list
        :return:
        """
        # 设置中心点时间
        self.index = 0
        # 绑定数据，更新横坐标映射，更新Y轴自适应函数，更新十字光标映射
        if 'open_interest' not in df_datas.columns:
            df_datas['open_interest'] = 0
        df_datas['time_int'] = np.array(range(len(df_datas.index)))
        self.datas = df_datas[['open', 'close', 'low', 'high', 'volume', 'open_interest']].to_records()
        self.axisTime.xdict = {}
        xdict = dict(enumerate(df_datas.index.tolist()))
        self.axisTime.update_xdict(xdict)
        self.resignData(self.datas)
        # 更新画图用到的数据
        self.listBar = df_datas[['time_int', 'open', 'close', 'low', 'high']].to_records(False)

        # 成交量颜色和涨跌同步，K线方向由涨跌决定
        datas0 = pd.DataFrame()
        datas0['open'] = df_datas.apply(lambda x: 0 if x['close'] >= x['open'] else x['volume'], axis=1)
        datas0['close'] = df_datas.apply(lambda x: 0 if x['close'] < x['open'] else x['volume'], axis=1)
        datas0['low'] = 0
        datas0['high'] = df_datas['volume']
        datas0['time_int'] = np.array(range(len(df_datas.index)))
        self.listVol = datas0[['time_int', 'open', 'close', 'low', 'high']].to_records(False)

        for indicator in main_indicators:
            list_indicator = list(df_datas[indicator])
            self.main_indicator_data[indicator] = list_indicator
        for indicator in sub_indicators:
            list_indicator = list(df_datas[indicator])
            self.sub_indicator_data[indicator] = list_indicator

        # 调用画图函数
        self.plot_all(redraw=True, xMin=0, xMax=len(self.datas))
        self.crosshair.signal.emit((None, None))
        print('finished load Data')


class GridKline(QtWidgets.QWidget):
    """多kline同时展示，时间联动"""

    def __init__(self, parent=None, kline_settings={}, title='', relocate=True):
        self.parent = parent
        super(GridKline, self).__init__(parent)
        # widget的标题
        if title:
            self.setWindowTitle(title)

        self.canvas_1 = None
        self.canvas_2 = None
        self.canvas_3 = None
        self.canvas_4 = None
        self.canvas_5 = None
        self.canvas_6 = None
        self.canvas_7 = None
        self.canvas_8 = None

        # 每一个K线的设置
        self.kline_settings = kline_settings
        # K线名称
        self.kline_names = list(self.kline_settings.keys())
        # K线名称: K线图表
        self.kline_dict = {}
        #
        self.grid_layout = QtWidgets.QGridLayout()
        self.setLayout(self.grid_layout)

        self.relocate = relocate
        self.init_ui()

    def init_ui(self):
        """初始化界面"""

        id = 1

        for kline_name, kline_setting in self.kline_settings.items():
            canvas = getattr(self, f'canvas_{id}')
            if id > 8:
                print(f'最多支持8个K线同时展现', file=sys.stderr)
                continue

            # 创建K线图表
            canvas = KLineWidget(display_vol=True, display_sub=True)
            canvas.show()
            # K线标题
            canvas.KLtitle.setText(f'{kline_name}', size='9pt')
            canvas.title = f'{kline_name}'
            # 主图指标
            main_indicators = kline_setting.get('main_indicators', [])
            for main_indicator in main_indicators:
                canvas.add_indicator(indicator=main_indicator, is_main=True)

            # 副图指标
            sub_indicators = kline_setting.get('sub_indicators', [])
            for sub_indicator in sub_indicators:
                canvas.add_indicator(indicator=sub_indicator, is_main=False)

            self.kline_dict[kline_name] = canvas

            if self.relocate:
                # 注册重定向事件
                canvas.relocate_notify_func = self.onRelocate

            id += 1

        # 将所有Kline放到画板
        kline_names = list(self.kline_names)
        widgets = len(kline_names)
        row = 0
        for i in range(0, widgets):
            if len(kline_names) == 0:
                break
            for column in [1, 2]:
                if len(kline_names) == 0:
                    break
                kline_name = kline_names.pop(0)
                kline_layout = QtWidgets.QVBoxLayout()
                kline_layout.addWidget(self.kline_dict[kline_name])
                self.grid_layout.addLayout(kline_layout, row, column)
                if len(kline_names) == 0:
                    break
            row += 1

        self.show()

        self.load_multi_kline()

    # ----------------------------------------------------------------------
    def load_multi_kline(self):
        """加载多周期窗口"""

        try:
            for kline_name, kline_setting in self.kline_settings.items():
                canvas = self.kline_dict.get(kline_name, None)
                if canvas is None:
                    continue

                # 加载K线
                if 'data_frame' in kline_setting:
                    df = kline_setting['data_frame']
                else:
                    data_file = kline_setting.get('data_file', None)
                    if not data_file:
                        continue
                    df = pd.read_csv(data_file)
                    df = df.set_index(pd.DatetimeIndex(df['datetime']))
                canvas.loadData(df,
                                main_indicators=kline_setting.get('main_indicators', []),
                                sub_indicators=kline_setting.get('sub_indicators', [])
                                )

                # 加载开、平仓的交易信号（一般是回测系统产生的）
                trade_list_file = kline_setting.get('trade_list_file', None)
                if trade_list_file and os.path.exists(trade_list_file):
                    print(f'loading {trade_list_file}')
                    df_trade_list = pd.read_csv(trade_list_file)
                    self.kline_dict[kline_name].add_signals(df_trade_list)

                # 记载交易信号（实盘产生的）
                trade_file = kline_setting.get('trade_file', None)
                if trade_file and os.path.exists(trade_file):
                    print(f'loading {trade_file}')
                    df_trade = pd.read_csv(trade_file)
                    self.kline_dict[kline_name].add_trades(df_trade)

                # 加载tns( 回测、实盘产生的）
                tns_file = kline_setting.get('tns_file', None)
                if tns_file and os.path.exists(tns_file):
                    print(f'loading {tns_file}')
                    df_tns = pd.read_csv(tns_file)
                    self.kline_dict[kline_name].add_trans_df(df_tns)

                # 加载policy 逻辑记录( 回测、实盘产生的）
                dist_file = kline_setting.get('dist_file', None)
                if dist_file and os.path.exists(dist_file):
                    print(f'loading {dist_file}')
                    df_markup = pd.read_csv(dist_file)
                    df_markup = df_markup[['datetime', 'price', 'operation']]
                    df_markup.rename(columns={'operation': 'markup'}, inplace=True)
                    self.kline_dict[kline_name].add_markups(df_markup=df_markup,
                                                       include_list=kline_setting.get('dist_include_list', []),
                                                       exclude_list=['buy', 'short', 'sell', 'cover'])

                # 笔
                bi_file = kline_setting.get('bi_file', None)
                if bi_file and os.path.exists(bi_file):
                    print(f'loading {bi_file}')
                    df_bi = pd.read_csv(bi_file)
                    self.kline_dict[kline_name].add_bi(df_bi, color='y', style= QtCore.Qt.DashLine)

                # 段
                duan_file = kline_setting.get('duan_file', None)
                if duan_file and os.path.exists(duan_file):
                    print(f'loading {duan_file}')
                    df_duan = pd.read_csv(duan_file)
                    self.kline_dict[kline_name].add_bi(df_duan, color='b')

                # 笔中枢
                bi_zs_file = kline_setting.get('bi_zs_file', None)
                if bi_zs_file and os.path.exists(bi_zs_file):
                    print(f'loading {bi_zs_file}')
                    df_bi_zs = pd.read_csv(bi_zs_file)
                    self.kline_dict[kline_name].add_zs(df_bi_zs, color='y')

                # 段中枢
                duan_zs_file = kline_setting.get('duan_zs_file', None)
                if duan_zs_file and os.path.exists(duan_zs_file):
                    print(f'loading {duan_zs_file}')
                    df_duan_zs = pd.read_csv(duan_zs_file)
                    self.kline_dict[kline_name].add_zs(df_duan_zs, color='b')

        except Exception as ex:
            traceback.print_exc()
            QtWidgets.QMessageBox.warning(self, 'Exception', u'Load data Exception',
                                          QtWidgets.QMessageBox.Cancel,
                                          QtWidgets.QMessageBox.NoButton)

            return

    def onRelocate(self, window_id, t_value, count_k):
        """
        重定位所有周期的时间
        :param window_id:
        :param t_value:
        :return:
        """
        for kline_name in self.kline_names:
            try:
                canvas = self.kline_dict.get(kline_name, None)
                if canvas is not None:
                    canvas.relocate(window_id, t_value, count_k)
            except Exception as ex:
                print(f'onRelocate exception:{str(ex)}')
                traceback.print_exc()

class MultiKlineWindow(QtWidgets.QMainWindow):
    """多窗口显示K线
    包括：

    """

    # ----------------------------------------------------------------------
    def __init__(self, parent=None, kline_settings={}, title=''):
        """Constructor"""
        super(MultiKlineWindow, self).__init__(parent)

        # 每一个K线的设置
        self.kline_settings = kline_settings
        # K线名称
        self.kline_names = list(self.kline_settings.keys())
        # K线名称: K线图表
        self.kline_dict = {}

        self.init_ui()

       # self.load_multi_kline()
    # ----------------------------------------------------------------------
    def init_ui(self):
        """初始化界面"""
        self.setWindowTitle(u'多周期')
        self.maximumSize()
        self.mdi = QtWidgets.QMdiArea()
        self.setCentralWidget(self.mdi)

        # 创建菜单
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")
        file_menu.addAction("Cascade")
        file_menu.addAction("Tiled")
        file_menu.triggered[QtWidgets.QAction].connect(self.windowaction)

        for kline_name, kline_setting in self.kline_settings.items():

            sub_window = QtWidgets.QMdiSubWindow()
            # K线标题
            sub_window.setWindowTitle(kline_name)

            # 创建K线图表
            canvas = KLineWidget(display_vol=False, display_sub=True)
            sub_window.setWidget(canvas)
            canvas.show()
            self.mdi.addSubWindow(sub_window)

            # 主图指标
            main_indicators = kline_setting.get('main_indicators', [])
            for main_indicator in main_indicators:
                canvas.add_indicator(indicator=main_indicator, is_main=True)

            # 副图指标
            sub_indicators = kline_setting.get('sub_indicators', [])
            for sub_indicator in sub_indicators:
                canvas.add_indicator(indicator=sub_indicator, is_main=False)

            self.kline_dict[kline_name] = canvas

            # 加载K线
            if 'data_frame' in kline_setting:
                df = kline_setting['data_frame']
            else:
                data_file = kline_setting.get('data_file', None)
                if not data_file:
                    continue
                df = pd.read_csv(data_file)
                df = df.set_index(pd.DatetimeIndex(df['datetime']))

            canvas.loadData(df,
                            main_indicators=kline_setting.get('main_indicators', []),
                            sub_indicators=kline_setting.get('sub_indicators', [])
                            )

            # 加载开、平仓的交易信号（一般是回测系统产生的）
            trade_list_file = kline_setting.get('trade_list_file', None)
            if trade_list_file and os.path.exists(trade_list_file):
                print(f'loading {trade_list_file}')
                df_trade_list = pd.read_csv(trade_list_file)
                self.kline_dict[kline_name].add_signals(df_trade_list)

            # 记载交易信号（实盘产生的）
            trade_file = kline_setting.get('trade_file', None)
            if trade_file and os.path.exists(trade_file):
                print(f'loading {trade_file}')
                df_trade = pd.read_csv(trade_file)
                self.kline_dict[kline_name].add_trades(df_trade)

            # 加载tns( 回测、实盘产生的）
            tns_file = kline_setting.get('tns_file', None)
            if tns_file and os.path.exists(tns_file):
                print(f'loading {tns_file}')
                df_tns = pd.read_csv(tns_file)
                self.kline_dict[kline_name].add_trans_df(df_tns)

            # 加载policy 逻辑记录( 回测、实盘产生的）
            dist_file = kline_setting.get('dist_file', None)
            if dist_file and os.path.exists(dist_file):
                print(f'loading {dist_file}')
                df_markup = pd.read_csv(dist_file)
                df_markup = df_markup[['datetime', 'price', 'operation']]
                df_markup.rename(columns={'operation': 'markup'}, inplace=True)
                self.kline_dict[kline_name].add_markups(df_markup=df_markup,
                                                        include_list=kline_setting.get('dist_include_list', []),
                                                        exclude_list=['buy', 'short', 'sell', 'cover'])

            sub_window.show()

        self.mdi.cascadeSubWindows()

    def windowaction(self,q):
        if q.text() == "cascade":
            self.mdi.cascadeSubWindows()

        if q.text() == "Cascade":
            self.mdi.tileSubWindows()

    # ----------------------------------------------------------------------
    def load_multi_kline(self):
        """加载多周期窗口"""

        try:
            for kline_name, kline_setting in self.kline_settings.items():

                canvas = self.kline_dict.get(kline_name, None)
                if canvas is None:
                    continue

                # 加载K线
                if 'data_frame' in kline_setting:
                    df = kline_setting['data_frame']
                else:
                    data_file = kline_setting.get('data_file', None)
                    if not data_file:
                        continue
                    df = pd.read_csv(data_file)
                    df = df.set_index(pd.DatetimeIndex(df['datetime']))

                canvas.loadData(df,
                                main_indicators=kline_setting.get('main_indicators', []),
                                sub_indicators=kline_setting.get('sub_indicators', [])
                                )

                # 加载开、平仓的交易信号（一般是回测系统产生的）
                trade_list_file = kline_setting.get('trade_list_file', None)
                if trade_list_file and os.path.exists(trade_list_file):
                    print(f'loading {trade_list_file}')
                    df_trade_list = pd.read_csv(trade_list_file)
                    self.kline_dict[kline_name].add_signals(df_trade_list)

                # 记载交易信号（实盘产生的）
                trade_file = kline_setting.get('trade_file', None)
                if trade_file and os.path.exists(trade_file):
                    print(f'loading {trade_file}')
                    df_trade = pd.read_csv(trade_file)
                    self.kline_dict[kline_name].add_trades(df_trade)

                # 加载tns( 回测、实盘产生的）
                tns_file = kline_setting.get('tns_file', None)
                if tns_file and os.path.exists(tns_file):
                    print(f'loading {tns_file}')
                    df_tns = pd.read_csv(tns_file)
                    self.kline_dict[kline_name].add_trans_df(df_tns)

                # 加载policy 逻辑记录( 回测、实盘产生的）
                dist_file = kline_setting.get('dist_file', None)
                if dist_file and os.path.exists(dist_file):
                    print(f'loading {dist_file}')
                    df_markup = pd.read_csv(dist_file)
                    df_markup = df_markup[['datetime', 'price', 'operation']]
                    df_markup.rename(columns={'operation': 'markup'}, inplace=True)
                    self.kline_dict[kline_name].add_markups(df_markup=df_markup,
                                                            include_list=kline_setting.get('dist_include_list', []),
                                                            exclude_list=['buy', 'short', 'sell', 'cover'])

        except Exception as ex:
            traceback.print_exc()
            QtWidgets.QMessageBox.warning(self, 'Exception', u'Load data Exception',
                                          QtWidgets.QMessageBox.Cancel,
                                          QtWidgets.QMessageBox.NoButton)

            return


    def closeEvent(self, event):
        """关闭窗口时的事件"""
        sys.exit(0)


def display_multi_grid(kline_settings={}):
    """显示多图"""
    from vnpy.trader.ui import create_qapp
    qApp = create_qapp()

    w = GridKline(kline_settings=kline_settings)
    w.showMaximized()
    sys.exit(qApp.exec_())
