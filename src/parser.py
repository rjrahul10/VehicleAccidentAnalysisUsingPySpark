import argparse


class Parser:

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('--config', type=str, required=True,
                                 help='path of configuration YAML file')
        self.parser.add_argument('--appname', type=str, required=False,
                                 help = 'Appname for the spark Session')

    def getParser(self):
        return self.parser
