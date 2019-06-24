import csv
import os
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('G3Q2')
inDirectory = "../results/G3Q2/"
for root, dirs, files in os.walk(inDirectory):
    print("files to upload", files)
    for filename in files:
        inFile = inDirectory+filename
        print("uploading file: " + inFile)
        with open(inFile) as csv_input_file:
            csv_reader = csv.reader(csv_input_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                #0:  DFW->ABI->DFW
                #1:  24/1/2008:1010	
                #2:  47.0
                #3:  true
                #4:  6.0
                #5:  DFW 
                #6:  ABI
                #7:  24/1/2008
                #8:  1010
                #9:  MQ 3299
                #10: false
                #11: 41.0
                #12: ABI
                #13: DFW
                #14: 26/1/2008
                #15: 1423
                #16: MQ 3234
                #17: -1900149235
                entryId = row[17]
                flights = row[0]
                date = row[7]
                totalDelay = row[2].replace("\t","")
                print("uploading id: " + entryId + " flights: " + flights + " date: " + date + " totalDelay: " + totalDelay)
                table.put_item(
                    Item={
                        'id': int(entryId),
                        'flights': flights,
                        'date': date,
                        'delay': int(float(totalDelay)),
                        'leg1': {
                            'delay': int(float(row[4])),
                            'origin': row[5],
                            'dest': row[6],
                            'date': row[7],
                            'time': row[8],
                            'flight': row[9]
                        },
                        'leg2': {
                            'delay': int(float(row[11])),
                            'origin': row[12],
                            'dest': row[13],
                            'date': row[14],
                            'time': row[15],
                            'flight': row[16]
                        }
                    }
                )
                line_count += 1
            print("Line Count: ", line_count)