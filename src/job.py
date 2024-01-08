from datetime import datetime
from pyspark.sql.functions import countDistinct, col, rank, broadcast
from pyspark.sql.window import Window

from data_acquistion import DataAcquisition
from datawriter import DataWriter


class Job:
    def __init__(self, spark, cfg, logger):
        self.damageDF = None
        self.Result = None
        self.unitDF = None
        self.win_prtn = None
        self.personDF = None
        self.spark = spark
        self.cfg = cfg
        self.logger = logger
        self.dataAcquisition = DataAcquisition(self.spark)
        self.datawriter = DataWriter(self.spark, self.cfg)

    def getAccidentByMale(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  1 started ")

        self.personDF = self.dataAcquisition.load_csv(self.cfg.get('DS_Person'))
        self.logger.info(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Dataframe created from the csv file " + self.cfg.get(
                'DS_Charges'))

        """Problem 1 """
        self.Result = self.personDF.filter("PRSN_GNDR_ID =='MALE' and  DEATH_CNT>2").groupBy("CRASH_ID").agg(
            countDistinct("CRASH_ID"))

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  1 completed ")

        self.datawriter.write_to_csv(self.Result, "1")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  1 ")

    def getTwoWheelerAccident(self):
        """Problem 2"""
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  2 started ")

        self.unitDF = self.dataAcquisition.load_csv(self.cfg.get('DS_Unit'))
        self.logger.info(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Dataframe created from the csv file " + self.cfg.get(
                'DS_Unit'))
        self.Result = self.unitDF.filter(col('VEH_BODY_STYL_ID').like('%MOTORCYCLE%')).agg(
            countDistinct("CRASH_ID").alias("Two_Wheeler_Acccident_Count"))

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  2 completed ")

        self.datawriter.write_to_csv(self.Result, "2")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  2 ")
        """Problem 3"""

    def getDeathByNoAirBag(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  3 started ")

        self.Result = ((self.personDF.join(self.unitDF, self.personDF['CRASH_ID'] == self.unitDF['CRASH_ID'])).filter(
            (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') &
            (col('PRSN_INJRY_SEV_ID') == 'KILLED') &
            (col('VEH_MAKE_ID') != 'NA')).groupBy('VEH_MAKE_ID').agg({"*": "count"})).withColumnRenamed("count(1)",
                                                                                                        "TOT_DTH").orderBy(
            "TOT_DTH", ascending=False).limit(5)

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  3 completed ")

        self.datawriter.write_to_csv(self.Result, "3")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  3 ")

        """Problem 4"""

    def getHitandRunCrash(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  4 started ")
        self.chargeDF = self.dataAcquisition.load_csv(self.cfg.get('DS_Charges'))
        self.no_lic_charge = self.chargeDF.filter(col("CHARGE").like("%LICENSE%"))

        self.Result = self.unitDF.join(broadcast(self.no_lic_charge),
                                       self.unitDF['CRASH_ID'] == self.no_lic_charge['CRASH_ID'],
                                       how='left_outer').filter((col("VEH_HNR_FL") == "Y")
                                                                & (self.no_lic_charge['CRASH_ID'].isNull())
                                                                ).agg(
            countDistinct(self.unitDF['CRASH_ID']).alias("HNR_CRASH"))

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  4 completed ")

        self.datawriter.write_to_csv(self.Result, "4")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  4 ")

        """" problem 5"""

    def getDriverState(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  5 started ")

        self.Result = self.personDF.filter((col("DRVR_LIC_STATE_ID").isin('NA', 'OTHER', 'Unknown') == False)
                                           & (col("PRSN_GNDR_ID") != "FEMALE")
                                           ).groupBy("DRVR_LIC_STATE_ID").agg(
            countDistinct("CRASH_ID").alias("Crash_Count")).orderBy("Crash_Count", ascending=False).limit(1)

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  5 completed ")

        self.datawriter.write_to_csv(self.Result, "5")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  5 ")

        """ Problem 6"""

    def getVehMakewithMostInjry(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  6 started ")

        self.Result = (self.unitDF.withColumn("TOT_INJRY_DTH_CNT",
                                              col("DEATH_CNT").cast("int") + col("TOT_INJRY_CNT").cast("int"))).groupBy(
            'VEH_MAKE_ID').sum('TOT_INJRY_DTH_CNT').withColumnRenamed("sum(TOT_INJRY_DTH_CNT)",
                                                                      "TOT_PRSN")
        self.win_prtn = Window.orderBy(col('TOT_PRSN').desc())
        self.Result = self.Result.withColumn("Rank", rank().over(self.win_prtn)).filter(col("Rank").between(3, 5))
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  6 completed ")

        self.datawriter.write_to_csv(self.Result, "6")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  6 ")
        """problem 7 """

    def getVehMakewithMostEthnc(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  7 started ")

        self.Result = ((self.personDF.join(self.unitDF, self.personDF['CRASH_ID'] == self.unitDF['CRASH_ID'])).groupBy(
            "VEH_MAKE_ID",
            "PRSN_ETHNICITY_ID").agg({"*": "count"})).withColumnRenamed("count(1)", "TOT_ETHNC_CNT")
        self.win_prtn = Window.partitionBy('VEH_MAKE_ID').orderBy(col('TOT_ETHNC_CNT').desc())
        self.Result = self.Result.withColumn("Rank", rank().over(self.win_prtn)).filter(col("Rank") == 1).select(
            "VEH_MAKE_ID", "PRSN_ETHNICITY_ID")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  7 completed ")

        self.datawriter.write_to_csv(self.Result, "7")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  7 ")
        """Problem 8"""

    def getZIPwithAlcCrash(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  8 started ")

        self.Result = (
            self.personDF.filter((col("PRSN_ALC_RSLT_ID") == "Positive") & (col("DRVR_ZIP").isNotNull())).groupBy(
                "DRVR_ZIP").agg({"*": "count"})).orderBy("count(1)", ascending=False).limit(5).select('DRVR_ZIP')

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  8 completed ")

        self.datawriter.write_to_csv(self.Result, "8")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem  8 ")
        """problem 9"""

    def getCrashwithNoDamage(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  9 started ")

        self.damageDF = self.dataAcquisition.load_csv(self.cfg.get('DS_Damages'))

        self.Result = self.unitDF.join(broadcast(self.damageDF), self.unitDF['CRASH_ID'] == self.damageDF['CRASH_ID'],
                                       how='left_outer').filter(
            (col('FIN_RESP_TYPE_ID') != "NA") & (self.damageDF["CRASH_ID"].isNull())
            & ((col('VEH_DMAG_SCL_1_ID').isin(['DAMAGED 4', 'DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST']))
               | (col('VEH_DMAG_SCL_1_ID').isin(['DAMAGED 4', 'DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST'])))
        ).agg(countDistinct(self.unitDF['CRASH_ID']).alias("NO_DAMAGE_CRASH"))

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  9 completed ")

        self.datawriter.write_to_csv(self.Result, "9")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem 9 ")

        """problem 9"""

    def getVehwithSpeedColor(self):
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  10 started ")

        self.Result = self.unitDF.filter(col('VEH_COLOR_ID') != 'NA').groupBy('VEH_COLOR_ID').agg(
            {"*": "count"}).withColumnRenamed("count(1)", "CRASH_CNT")
        self.win_prtn = Window.partitionBy('VEH_COLOR_ID').orderBy(col("CRASH_CNT").desc())
        self.color = self.Result.withColumn("rank", rank().over(self.win_prtn)).filter(col("rank") <= 10).select(
            'VEH_COLOR_ID').collect()
        self.colorList = [row[0] for row in self.color]

        self.Result = self.unitDF.filter(col('VEH_LIC_STATE_ID').isin('NA', '98') == False).groupBy('VEH_LIC_STATE_ID').agg(
            {"*": "count"}).withColumnRenamed("count(1)", "CRASH_CNT")
        self.win_prtn = Window.partitionBy('VEH_LIC_STATE_ID').orderBy(col('CRASH_CNT').desc())
        self.vehStateDF = self.Result.withColumn("rank", rank().over(self.win_prtn)).filter(col('rank') <= 25).select(
            'VEH_LIC_STATE_ID').collect()
        self.vehstatelist = [row[0] for row in self.vehStateDF]

        self.Result = self.unitDF.join(self.personDF, self.unitDF['CRASH_ID'] == self.personDF['CRASH_ID']
                                       ).join(broadcast(self.chargeDF),
                                              self.unitDF['CRASH_ID'] == self.chargeDF['CRASH_ID']).filter(
            (col('CHARGE').like('%SPEED%')) &
            (col('VEH_COLOR_ID').isin(self.colorList))
            & (col('VEH_LIC_STATE_ID').isin(self.vehstatelist))
            & (col('DRVR_LIC_TYPE_ID') != "UNLICENSED")).groupBy("VEH_MAKE_ID").agg(
            countDistinct(self.unitDF['CRASH_ID']).alias("Crash_Count")).orderBy("Crash_Count", ascending=False).limit(5)

        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Analysis of Problem  10 completed ")

        self.datawriter.write_to_csv(self.Result, "10")
        self.logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S :') + " Output has been loaded for Problem 10 ")




    def run(self):
        self.getAccidentByMale()
        self.getTwoWheelerAccident()
        self.getDeathByNoAirBag()
        self.getHitandRunCrash()
        self.getDriverState()
        self.getVehMakewithMostInjry()
        self.getVehMakewithMostEthnc()
        self.getZIPwithAlcCrash()
        self.getCrashwithNoDamage()
        self.getVehwithSpeedColor()
