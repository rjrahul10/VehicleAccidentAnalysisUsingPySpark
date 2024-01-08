import yaml


class Utilities:
    def __init__(self):
        self.config = None

    def read_config(self, yaml_file):
        with open(yaml_file, 'r') as cfg:
            self.config = yaml.safe_load(cfg)
        return self.config
