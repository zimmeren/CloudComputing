import csv
import os

inDirectory = "data/raw/"
outDirectory = "data/clean/"
for root, dirs, files in os.walk(inDirectory):
    print("files to clean", files)
    for filename in files:
        inFile = inDirectory+filename
        outFile = outDirectory+"Clean_"+filename
        print("reading file: " + inFile + " writing file: " + outFile)
        with open(inFile) as csv_input_file:
            with open (outFile, 'w+') as csv_output_file:
                csv_reader = csv.reader(csv_input_file, delimiter=',')
                csv_writer = csv.writer(csv_output_file, delimiter=',')
                line_count = 0
                removed_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        csv_writer.writerow(["Year", "Month", "DayofMonth", "UniqueCarrier", "FlightNum", "Origin", "Dest", "CRSDepTime", "DepDelay", "ArrDelay"])
                    else:
                        cancelled = row[41]
                        diverted = row[43]
                        if float(cancelled) != 0.0 or float(diverted) != 0.0:
                            removed_count += 1
                        else:
                            csv_writer.writerow([row[0], row[2], row[3], row[6], row[10], row[11], row[17], row[23], row[25], row[36]])
                    line_count += 1
                print("Line Count: ", line_count)
                print("Removed Count: ", removed_count)