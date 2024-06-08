from pathlib import Path, PurePath
from time import sleep
import json
from io import TextIOWrapper
from datetime import datetime
from shutil import copy

class InstanceStatus():
    """Stocks information about one passivbot configuration."""
    def __init__(self):
        self.name = None
        self.version = None
        self.multi = None
        self.enabled_on = None
        self.running = None

class InstancesStatus():
    def __init__(self, status_file: str):
        self.instances = []
        self.index = 0
        self.pbname = None
        self.activate_ts = 0
#        pbgdir = Path.cwd()
#        self.status_file = f'{pbgdir}/data/cmd/status.json'
        self.status_file = status_file
        self.status_ts = 0
        self.load()

    def __iter__(self):
        return iter(self.instances)

    def __next__(self):
        if self.index > len(self.instances):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.name, self.instances))

    def add(self, istatus : InstanceStatus):
        for index, instance in enumerate(self.instances):
            if instance.name == istatus.name:
                self.instances[index] = istatus
                return
        self.instances.append(istatus)

    def remove(self, istatus : InstanceStatus):
        for index, instance in enumerate(self.instances):
            if instance.name == istatus.name:
                self.instances.pop(index)
                return

    def is_running(self, name: str):
        if self.has_new_status():
            self.load()
        for instance in self.instances:
            if instance.name == name:
                return instance.running

    def find_name(self, name: str):
        for instance in self.instances:
            if instance.name == name:
                return instance
        return None

    def find_version(self, name: str):
        for instance in self.instances:
            if instance.name == name:
                return instance.version
        return 0

    def has_new_status(self):
        if Path(self.status_file).exists():
            status_ts = Path(self.status_file).stat().st_mtime
            if self.status_ts < status_ts:
                return True
        return False

    def update_status(self):
        if Path(self.status_file).exists():
            self.status_ts = Path(self.status_file).stat().st_mtime

    def load(self):
        file = Path(self.status_file)
        if file.exists():
            self.status_ts = file.stat().st_mtime
            with open(file, "r", encoding='utf-8') as f:
                instances = json.load(f)
                if "activate_ts" in instances:
                    self.activate_ts = instances["activate_ts"]
                    self.activate_pbname = instances["activate_pbname"]
                    for instance in instances["instances"]:
                        status = InstanceStatus()
                        status.name = instance
                        status.version = instances["instances"][instance]["version"]
                        status.multi = instances["instances"][instance]["multi"]
                        status.enabled_on = instances["instances"][instance]["enabled_on"]
                        status.running = instances["instances"][instance]["running"]
                        self.add(status)

    def save(self):
        instances = {}
        for instance in self.instances:
            instances[instance.name] = ({
                "enabled_on" : instance.enabled_on,
                "version": instance.version,
                "multi": instance.multi,
                "running": instance.running
            })
        status = {
            "activate_ts" : self.activate_ts,
            "activate_pbname" : self.pbname,
            "instances" : instances
        }
        file = Path(self.status_file)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(status, f, indent=4)


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
