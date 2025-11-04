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
        self.mem_warning_multi = 250.0
        self.mem_error_multi = 500.0
        self.swap_warning_multi = 250.0
        self.swap_error_multi = 500.0
        self.cpu_warning_multi = 5.0
        self.cpu_error_multi = 10.0
        self.error_warning_multi = 25.0
        self.error_error_multi = 50.0
        self.traceback_warning_multi = 25.0
        self.traceback_error_multi = 50.0
        self.mem_warning_single = 50.0
        self.mem_error_single = 100.0
        self.swap_warning_single = 50.0
        self.swap_error_single = 100.0
        self.cpu_warning_single = 5.0
        self.cpu_error_single = 10.0
        self.error_warning_single = 25.0
        self.error_error_single = 50.0
        self.traceback_warning_single = 25.0
        self.traceback_error_single = 50.0
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
        # Multi
        mem_warning_multi = load_ini("monitor", "mem_warning_multi")
        if mem_warning_multi:
            self.mem_warning_multi = float(mem_warning_multi)
        mem_error_multi = load_ini("monitor", "mem_error_multi")
        if mem_error_multi:
            self.mem_error_multi = float(mem_error_multi)
        swap_warning_multi = load_ini("monitor", "swap_warning_multi")
        if swap_warning_multi:
            self.swap_warning_multi = float(swap_warning_multi)
        swap_error_multi = load_ini("monitor", "swap_error_multi")
        if swap_error_multi:
            self.swap_error_multi = float(swap_error_multi)
        cpu_warning_multi = load_ini("monitor", "cpu_warning_multi")
        if cpu_warning_multi:
            self.cpu_warning_multi = float(cpu_warning_multi)
        cpu_error_multi = load_ini("monitor", "cpu_error_multi")
        if cpu_error_multi:
            self.cpu_error_multi = float(cpu_error_multi)
        error_warning_multi = load_ini("monitor", "error_warning_multi")
        if error_warning_multi:
            self.error_warning_multi = float(error_warning_multi)
        error_error_multi = load_ini("monitor", "error_error_multi")
        if error_error_multi:
            self.error_error_multi = float(error_error_multi)
        traceback_warning_multi = load_ini("monitor", "traceback_warning_multi")
        if traceback_warning_multi:
            self.traceback_warning_multi = float(traceback_warning_multi)
        traceback_error_multi = load_ini("monitor", "traceback_error_multi")
        if traceback_error_multi:
            self.traceback_error_multi = float(traceback_error_multi)
        # Single
        mem_warning_single = load_ini("monitor", "mem_warning_single")
        if mem_warning_single:
            self.mem_warning_single = float(mem_warning_single)
        mem_error_single = load_ini("monitor", "mem_error_single")
        if mem_error_single:
            self.mem_error_single = float(mem_error_single)
        swap_warning_single = load_ini("monitor", "swap_warning_single")
        if swap_warning_single:
            self.swap_warning_single = float(swap_warning_single)
        swap_error_single = load_ini("monitor", "swap_error_single")
        if swap_error_single:
            self.swap_error_single = float(swap_error_single)
        cpu_warning_single = load_ini("monitor", "cpu_warning_single")
        if cpu_warning_single:
            self.cpu_warning_single = float(cpu_warning_single)
        cpu_error_single = load_ini("monitor", "cpu_error_single")
        if cpu_error_single:
            self.cpu_error_single = float(cpu_error_single)
        error_warning_single = load_ini("monitor", "error_warning_single")
        if error_warning_single:
            self.error_warning_single = float(error_warning_single)
        error_error_single = load_ini("monitor", "error_error_single")
        if error_error_single:
            self.error_error_single = float(error_error_single)
        traceback_warning_single = load_ini("monitor", "traceback_warning_single")
        if traceback_warning_single:
            self.traceback_warning_single = float(traceback_warning_single)
        traceback_error_single = load_ini("monitor", "traceback_error_single")
        if traceback_error_single:
            self.traceback_error_single = float(traceback_error_single)

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
        # Multi
        save_ini("monitor", "mem_warning_multi", str(self.mem_warning_multi))
        save_ini("monitor", "mem_error_multi", str(self.mem_error_multi))
        save_ini("monitor", "swap_warning_multi", str(self.swap_warning_multi))
        save_ini("monitor", "swap_error_multi", str(self.swap_error_multi))
        save_ini("monitor", "cpu_warning_multi", str(self.cpu_warning_multi))
        save_ini("monitor", "cpu_error_multi", str(self.cpu_error_multi))
        save_ini("monitor", "error_warning_multi", str(self.error_warning_multi))
        save_ini("monitor", "error_error_multi", str(self.error_error_multi))
        save_ini("monitor", "traceback_warning_multi", str(self.traceback_warning_multi))
        save_ini("monitor", "traceback_error_multi", str(self.traceback_error_multi))
        # Single
        save_ini("monitor", "mem_warning_single", str(self.mem_warning_single))
        save_ini("monitor", "mem_error_single", str(self.mem_error_single))
        save_ini("monitor", "swap_warning_single", str(self.swap_warning_single))
        save_ini("monitor", "swap_error_single", str(self.swap_error_single))
        save_ini("monitor", "cpu_warning_single", str(self.cpu_warning_single))
        save_ini("monitor", "cpu_error_single", str(self.cpu_error_single))
        save_ini("monitor", "error_warning_single", str(self.error_warning_single))
        save_ini("monitor", "error_error_single", str(self.error_error_single))
        save_ini("monitor", "traceback_warning_single", str(self.traceback_warning_single))
        save_ini("monitor", "traceback_error_single", str(self.traceback_error_single))

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
