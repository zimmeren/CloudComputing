import csv
import os
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('G2Q2')
inDirectory = "../results/G2Q2/"
for root, dirs, files in os.walk(inDirectory):
    print("files to upload", files)
    for filename in files:
        inFile = inDirectory+filename
        print("uploading file: " + inFile)
        with open(inFile) as csv_input_file:
            csv_reader = csv.reader(csv_input_file, delimiter='\t')
            line_count = 0
            for row in csv_reader:
                origin = ""
                destination = ""
                pair = row[0].split()
                if len(pair) == 2:
                    origin = pair[0]
                    destination = pair[1]
                elif len(pair) == 3:
                    if (pair[1] == "(1)"):
                        origin = pair[0] + " " + pair[1]
                        destination = pair[2]
                    else:
                        origin = pair[0]
                        destination = pair[1] + " " + pair[2]
                elif len(pair) > 3:
                    origin = pair[0] + " " + pair[1]
                    destination = pair[2] + " " + pair[3]
                print("origin: " + origin + " destination: " + destination + " delay: " + row[1])
                table.put_item(
                    Item={
                        'origin': origin,
                        'destination': destination,
                        'delay': row[1]
                    }
                )
                line_count += 1
            print("Line Count: ", line_count)