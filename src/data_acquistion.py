
class DataAcquisition:

    def __init__(self, spark):
        self.spark = spark

    def load_csv(self, filename):
        return self.spark.read.load(filename, format='csv', header=True, inferschema=True)
