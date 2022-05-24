class readData:

    def readcsv(self,spark,path,schema=None,header=False,inferSchema=True, sep=",",multiLine=True,escape="\'"):

        if (header==None) & (inferSchema==False):
            raise Exception('Provide manual schema or choose inferSchema as True')

        if (schema==None):
            readCSV = spark.read.csv(path=path,inferSchema=True,header=True,sep=sep,multiLine=True,escape="\'")


        else:
            readCSV = spark.read.csv(path=path,inferSchema=inferSchema,schema=schema,header=header,sep=sep,multiLine=multiLine,escape="\'")


        return readCSV