from pbgui_purefunc import load_ini, save_ini

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
        # Server
        mem_warning_server = load_ini("monitor", "mem_warning_server")
        if mem_warning_server:
            self.mem_warning_server = float(mem_warning_server)
        mem_error_server = load_ini("monitor", "mem_error_server")
        if mem_error_server:
            self.mem_error_server = float(mem_error_server)
        swap_warning_server = load_ini("monitor", "swap_warning_server")
        if swap_warning_server:
            self.swap_warning_server = float(swap_warning_server)
        swap_error_server = load_ini("monitor", "swap_error_server")
        if swap_error_server:
            self.swap_error_server = float(swap_error_server)
        cpu_warning_server = load_ini("monitor", "cpu_warning_server")
        if cpu_warning_server:
            self.cpu_warning_server = float(cpu_warning_server)
        cpu_error_server = load_ini("monitor", "cpu_error_server")
        if cpu_error_server:
            self.cpu_error_server = float(cpu_error_server)
        disk_warning_server = load_ini("monitor", "disk_warning_server")
        if disk_warning_server:
            self.disk_warning_server = float(disk_warning_server)
        disk_error_server = load_ini("monitor", "disk_error_server")
        if disk_error_server:
            self.disk_error_server = float(disk_error_server)
        # V7
        mem_warning_v7 = load_ini("monitor", "mem_warning_v7")
        if mem_warning_v7:
            self.mem_warning_v7 = float(mem_warning_v7)
        mem_error_v7 = load_ini("monitor", "mem_error_v7")
        if mem_error_v7:
            self.mem_error_v7 = float(mem_error_v7)
        swap_warning_v7 = load_ini("monitor", "swap_warning_v7")
        if swap_warning_v7:
            self.swap_warning_v7 = float(swap_warning_v7)
        swap_error_v7 = load_ini("monitor", "swap_error_v7")
        if swap_error_v7:
            self.swap_error_v7 = float(swap_error_v7)
        cpu_warning_v7 = load_ini("monitor", "cpu_warning_v7")
        if cpu_warning_v7:
            self.cpu_warning_v7 = float(cpu_warning_v7)
        cpu_error_v7 = load_ini("monitor", "cpu_error_v7")
        if cpu_error_v7:
            self.cpu_error_v7 = float(cpu_error_v7)
        error_warning_v7 = load_ini("monitor", "error_warning_v7")
        if error_warning_v7:
            self.error_warning_v7 = float(error_warning_v7)
        error_error_v7 = load_ini("monitor", "error_error_v7")
        if error_error_v7:
            self.error_error_v7 = float(error_error_v7)
        traceback_warning_v7 = load_ini("monitor", "traceback_warning_v7")
        if traceback_warning_v7:
            self.traceback_warning_v7 = float(traceback_warning_v7)
        traceback_error_v7 = load_ini("monitor", "traceback_error_v7")
        if traceback_error_v7:
            self.traceback_error_v7 = float(traceback_error_v7)

    def save_monitor_config(self):
        # Server
        save_ini("monitor", "mem_warning_server", str(self.mem_warning_server))
        save_ini("monitor", "mem_error_server", str(self.mem_error_server))
        save_ini("monitor", "swap_warning_server", str(self.swap_warning_server))
        save_ini("monitor", "swap_error_server", str(self.swap_error_server))
        save_ini("monitor", "cpu_warning_server", str(self.cpu_warning_server))
        save_ini("monitor", "cpu_error_server", str(self.cpu_error_server))
        save_ini("monitor", "disk_warning_server", str(self.disk_warning_server))
        save_ini("monitor", "disk_error_server", str(self.disk_error_server))
        # V7
        save_ini("monitor", "mem_warning_v7", str(self.mem_warning_v7))
        save_ini("monitor", "mem_error_v7", str(self.mem_error_v7))
        save_ini("monitor", "swap_warning_v7", str(self.swap_warning_v7))
        save_ini("monitor", "swap_error_v7", str(self.swap_error_v7))
        save_ini("monitor", "cpu_warning_v7", str(self.cpu_warning_v7))
        save_ini("monitor", "cpu_error_v7", str(self.cpu_error_v7))
        save_ini("monitor", "error_warning_v7", str(self.error_warning_v7))
        save_ini("monitor", "error_error_v7", str(self.error_error_v7))
        save_ini("monitor", "traceback_warning_v7", str(self.traceback_warning_v7))
        save_ini("monitor", "traceback_error_v7", str(self.traceback_error_v7))
