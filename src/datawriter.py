

class DataWriter:

    def __init__(self, spark, cfg):
        self.spark = spark
        self.cfg = cfg

    def write_to_csv(self, df, prblmNm):
        df.write.csv(self.cfg.get('OUTPUT')+prblmNm , header =True, mode="overwrite")