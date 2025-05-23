"""
Status collects and stores multiple parameters related to running a PassivBot configuration.

The collected data are stored in a list of statuses (InsancesStatusList()), where each individual PassivBot instances deployed on PBGui has its own Status (InstanceStatus()).

Each status includes informations such as the name, the version, where it is supposed to run, whether it is a multi configuration, and whether it is running on the local server. 

This status list is then sent through PBRemote to the remote storage, enabling us to manage bots from the master server.
"""
from pathlib import Path
import json

class InstanceStatus():
    """Stores information about one passivbot configuration."""
    def __init__(self):
        self.name = None
        self.version = None
        self.multi = None
        self.enabled_on = None
        self.running = None

class InstancesStatus():
    """Stores every InstanceStatus into status.json, manages and loads them."""
    def __init__(self, status_file: str): 
        """status_file (str): Path to the status file."""
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

    def list(self): # Never referenced ?
        """Returns a list of names of all the passivbot instances in the status list."""
        return list(map(lambda c: c.name, self.instances))

    def add(self, istatus: InstanceStatus):
        """
        Adds a new instance status or updates an existing one in the status list.

        Args:
            istatus (InstanceStatus): The instance status to add or to update.
        """
        for index, instance in enumerate(self.instances):
            if instance.name == istatus.name:
                self.instances[index] = istatus
                return
        self.instances.append(istatus)

    def remove(self, istatus: InstanceStatus):
        """
        Removes an instance from the status list.

        Args:
            istatus (InstanceStatus): The instance status to remove.
        """
        for index, instance in enumerate(self.instances):
            if instance.name == istatus.name:
                self.instances.pop(index)
                return

    def is_running(self, name: str):
        # if self.has_new_status():
        #     self.load()
        for instance in self.instances:
            if instance.name == name:
                return instance.running

    def find_name(self, name: str):
        """
        Checks If an instance already has a status and return It.

        Returns:
            InstanceStatus: The instance with the specified name, or None if not found.
        """
        for instance in self.instances:
            if instance.name == name:
                return instance
        return None

    def find_version(self, name: str):
        """
        Finds the version of an instance by name in the status list.

        Args:
            name (str): The name of the instance.

        Returns:
            str: The version of the instance, or 0 if not found.
        """
        for instance in self.instances:
            if instance.name == name:
                return instance.version
        return 0

    def has_new_status(self):
        if Path(self.status_file).exists():
            status_ts = Path(self.status_file).stat().st_mtime
            if self.status_ts < status_ts:
                self.load()
                return True
        return False

    def update_status(self):
        """Updates the status timestamp from the status list."""
        if Path(self.status_file).exists():
            self.status_ts = Path(self.status_file).stat().st_mtime

    def load(self):
        """Loads the status information from the status list."""
        file = Path(self.status_file)
        if file.exists():
            self.status_ts = file.stat().st_mtime
            with open(file, "r", encoding='utf-8') as f:
                try:
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
                except json.JSONDecodeError as e:
                    print(f"Error loading status file: {e}")

    def save(self):
        """Saves the current status information to the status file."""
        instances = {}
        for instance in self.instances:
            instances[instance.name] = ({
                "enabled_on" : instance.enabled_on,
                "version": instance.version,
                "multi": instance.multi,
                "running": instance.running
            })
        status = {
            "activate_ts": self.activate_ts,
            "activate_pbname": self.pbname,
            "instances": instances
        }
        file = Path(self.status_file)
        with open(file, "w", encoding='utf-8') as f:
            json.dump(status, f, indent=4)


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
