from pyspark.sql import functions as F 
from pyspark.sql import Row 

def toDaily(df):
    rows = []
    
    for row in df.collect():
        id = row['id']
        year = row['year']
        month = row['month']
        element = row['element']

        for day in range(1, 32):
            #creating column names based on day 
            valueDay = f"value{day}"
            mflagDay = f"mflag{day}"

            #checking of value exists before creating the row 
            if value in row.asDict() and mflag in row.asDict():
                newValue = row[valueDay]
                newMflag = row[mflagDay]

                newRow = Row(id=id, year=year, month=month, element=element, 
                        day=day, value=newValue, mflag=newMflag)
                #refers to the f-strings used earlier to rename and allocate values

            rows.append(newRow) #adding each day

    #atp, 'rows' has all days from all months cuz it converted all the rows in original df
    #combining all the rows into 
    df_final = spark.createDataFrame(rows) 

    return df_final
