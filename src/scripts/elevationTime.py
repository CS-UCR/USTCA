import time 
import sys
from ustca.src.scripts.elevation import gettingData, runAnovaNoPrints, runLinearNoPrints, runLinearResults, runAncovaNoPrints

avgTMaxByElevYear, avgTMinByElevYear, avgTMaxPD, avgTMinPD = gettingData()
numWorkers = [1, 2, 4]
originalSTDout = sys.stdout
with open('outputs/elevation/executionTimes.txt', 'w') as f: 
    sys.stdout = f

    print("1. DO THE ELEVATION BINS HAVE DIFFERENT AVERAGE TEMPERATURES?\n")
    for count in numWorkers:
        print(f"{count} workers: ")

        startTime = time.time()
        
        runAnovaNoPrints(avgTMaxPD, avgTMinPD)
        
        endTime = time.time()    
        duration = endTime - startTime
        print(f"== Execution time: {duration:.2f} seconds")


    print("\n\n\n 2. WHAT TRENDS DO EACH OF THE ELEVATION BINS HAVE?\n")
    for count in numWorkers:
        print(f"{count} workers: ")

        startTime = time.time()
        
        runLinearNoPrints(avgTMaxByElevYear, avgTMinByElevYear)
        
        end_time = time.time()
        duration = end_time - startTime
        print(f"== Execution time: {duration:.2f} seconds")


    print("\n\n\n 3. WHICH ELEVATION BIN CHANGED THE MOST?\n")
    for count in numWorkers:
        print(f"{count} workers: ")

        startTime = time.time()
        
        runAncovaNoPrints(avgTMaxPD, avgTMinPD)
        
        end_time = time.time()
        duration = end_time - startTime
        print(f"== Execution time: {duration:.2f} seconds")
    
sys.stdout = originalSTDout
print("All analysis results have written to output file.")

