from pbgui_purefunc import load_ini_snapshot, save_ini_section

class MonitorConfig():
    def __init__(self):
        self.server = None
        self.servers = []
        self.logfiles = []
        self.mem_warning_server = 50.0
        self.mem_error_server = 25.0
        self.swap_warning_server = 150.0
        self.swap_error_server = 100.0
        self.disk_warning_server = 500.0
        self.disk_error_server = 250.0
        self.cpu_warning_server = 80.0
        self.cpu_error_server = 95.0
        self.mem_warning_v7 = 250.0
        self.mem_error_v7 = 500.0
        self.swap_warning_v7 = 250.0
        self.swap_error_v7 = 500.0
        self.cpu_warning_v7 = 10.0
        self.cpu_error_v7 = 15.0
        self.error_warning_v7 = 100.0
        self.error_error_v7 = 250.0
        self.traceback_warning_v7 = 100.0
        self.traceback_error_v7 = 250.0
        self.load_monitor_config()

    def load_monitor_config(self):
        snapshot = load_ini_snapshot()
        fields = (
            "mem_warning_server", "mem_error_server", "swap_warning_server", "swap_error_server",
            "cpu_warning_server", "cpu_error_server", "disk_warning_server", "disk_error_server",
            "mem_warning_v7", "mem_error_v7", "swap_warning_v7", "swap_error_v7",
            "cpu_warning_v7", "cpu_error_v7", "error_warning_v7", "error_error_v7",
            "traceback_warning_v7", "traceback_error_v7",
        )
        for field in fields:
            if snapshot.has_option("monitor", field):
                value = snapshot.get("monitor", field)
                if value:
                    setattr(self, field, float(value))

    def save_monitor_config(self):
        fields = (
            "mem_warning_server", "mem_error_server", "swap_warning_server", "swap_error_server",
            "cpu_warning_server", "cpu_error_server", "disk_warning_server", "disk_error_server",
            "mem_warning_v7", "mem_error_v7", "swap_warning_v7", "swap_error_v7",
            "cpu_warning_v7", "cpu_error_v7", "error_warning_v7", "error_error_v7",
            "traceback_warning_v7", "traceback_error_v7",
        )
        save_ini_section("monitor", {field: str(getattr(self, field)) for field in fields})
