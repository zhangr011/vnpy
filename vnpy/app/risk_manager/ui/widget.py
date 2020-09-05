from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets
from ..engine import APP_NAME


class RiskManager(QtWidgets.QDialog):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__()

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.rm_engine = main_engine.get_engine(APP_NAME)

        self.init_ui()

    def init_ui(self):
        """"""
        self.setWindowTitle("交易风控")

        # Create widgets
        self.active_combo = QtWidgets.QComboBox()
        self.active_combo.addItems(["停止", "启动"])

        self.flow_limit_spin = RiskManagerSpinBox()
        self.flow_clear_spin = RiskManagerSpinBox()
        self.size_limit_spin = RiskManagerSpinBox()
        self.trade_limit_spin = RiskManagerSpinBox()
        self.active_limit_spin = RiskManagerSpinBox()
        self.cancel_limit_spin = RiskManagerSpinBox()
        self.percent_limit_spin = RiskManagerSpinBox()

        self.ratio_active_limit_spin = RiskManagerSpinBox()
        self.reject_limit_percent_spin = RiskManagerSpinBox()
        self.cancel_limit_percent_spin = RiskManagerSpinBox()

        self.trade_hold_active_limit_spin = RiskManagerSpinBox()
        self.trade_hold_percent_limit_spin = RiskManagerSpinBox()


        save_button = QtWidgets.QPushButton("保存")
        save_button.clicked.connect(self.save_setting)

        # Form layout
        form = QtWidgets.QFormLayout()
        form.addRow("风控运行状态", self.active_combo)
        form.addRow("委托流控上限（笔）", self.flow_limit_spin)
        form.addRow("委托流控清空（秒）", self.flow_clear_spin)
        form.addRow("单笔委托上限（数量）", self.size_limit_spin)
        form.addRow("总成交上限（笔）", self.trade_limit_spin)
        form.addRow("活动委托上限（笔）", self.active_limit_spin)
        form.addRow("合约撤单上限（笔）", self.cancel_limit_spin)
        form.addRow("资金仓位上限(%)", self.percent_limit_spin)
        form.addRow("激活废单/撤单(笔)", self.ratio_active_limit_spin)
        form.addRow("废单比上限(%)", self.reject_limit_percent_spin)
        form.addRow("撤单比上限(%)", self.cancel_limit_percent_spin)
        form.addRow("激活成交/持仓比阈值（笔）" ,self.trade_hold_active_limit_spin)
        form.addRow("成交/持仓比上限(%)", self.trade_hold_percent_limit_spin)

        form.addRow(save_button)

        self.setLayout(form)

        # Set Fix Size
        hint = self.sizeHint()
        self.setFixedSize(hint.width() * 1.2, hint.height())

    def save_setting(self):
        """"""
        active_text = self.active_combo.currentText()
        if active_text == "启动":
            active = True
        else:
            active = False

        setting = {
            "active": active,
            "order_flow_limit": self.flow_limit_spin.value(),
            "order_flow_clear": self.flow_clear_spin.value(),
            "order_size_limit": self.size_limit_spin.value(),
            "trade_limit": self.trade_limit_spin.value(),
            "active_order_limit": self.active_limit_spin.value(),
            "order_cancel_limit": self.cancel_limit_spin.value(),
            "percent_limit": self.percent_limit_spin.value(),
            "ratio_active_order_limit": self.ratio_active_limit_spin.value(),
            "cancel_ratio_percent_limit": self.cancel_limit_percent_spin.value(),
            "reject_ratio_percent_limit": self.reject_limit_percent_spin.value(),
            "trade_hold_active_limit": self.trade_hold_active_limit_spin.value(),
            "trade_hold_percent_limit": self.trade_hold_percent_limit_spin.value()
        }

        self.rm_engine.update_setting(setting)
        self.rm_engine.save_setting()

        self.close()

    def update_setting(self):
        """"""
        setting = self.rm_engine.get_setting()
        if setting["active"]:
            self.active_combo.setCurrentIndex(1)
        else:
            self.active_combo.setCurrentIndex(0)

        self.flow_limit_spin.setValue(setting["order_flow_limit"])
        self.flow_clear_spin.setValue(setting["order_flow_clear"])
        self.size_limit_spin.setValue(setting["order_size_limit"])
        self.trade_limit_spin.setValue(setting["trade_limit"])
        self.active_limit_spin.setValue(setting["active_order_limit"])
        self.cancel_limit_spin.setValue(setting["order_cancel_limit"])
        self.percent_limit_spin.setValue(setting.get('percent_limit', 100))
        self.ratio_active_limit_spin.setValue(setting.get('ratio_active_order_limit', 500))
        self.cancel_limit_percent_spin.setValue(setting.get('cancel_ratio_percent_limit', 90))
        self.reject_limit_percent_spin.setValue(setting.get('reject_ratio_percent_limit', 90))
        self.trade_hold_active_limit_spin.setValue(setting.get('trade_hold_active_limit', 1000))
        self.trade_hold_percent_limit_spin.setValue(setting.get("trade_hold_percent_limit", 300))

    def exec_(self):
        """"""
        self.update_setting()
        super().exec_()


class RiskManagerSpinBox(QtWidgets.QSpinBox):
    """"""

    def __init__(self, value: int = 0):
        """"""
        super().__init__()
        self.setMinimum(0)
        self.setMaximum(1000000)
        self.setValue(value)
