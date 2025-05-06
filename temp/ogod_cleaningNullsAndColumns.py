from pyspark.sql.functions import col 

def removeNullAndFlags(df):
    
    #cleaning out rows with null in specified columns
    columns = ['ID', 'date', 'element', 'value']
    df_noNull = df.dropna(subset=columns)

    #removing all S and Q flag columns
    filtered = [c for c in df_noNull.columns if "Sflag" not in c and "Qflag" not in c]

    #selecting only the filtered columns  
    df_final = df_noNull.select([col(c) for c in filtered])

    return df_final

#import pandas as pd

#def removeNullAndSFlags(df):

#    #cleaning out rows with null in specified columns
#    columns = ['ID', 'date', 'element', 'value']
#    df_noNull = df.dropna(subset=columns)

#    #removing all Sflag columns
#    df_noSflags = df_noNull.loc[:, ~df_noNull.columns.str.contains("Sflag")]

#    df_noQflags = df_noSflags.loc[:, ~df_noSflags.columns.str.contains("Qflag")]

#    return df_noQflags
