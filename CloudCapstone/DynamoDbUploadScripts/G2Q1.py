import csv
import os
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('G2Q1')
inDirectory = "../results/G2Q1/"
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
                airline = ""
                pair = row[0].split()
                if len(pair) == 2:
                    origin = pair[0]
                    airline = pair[1]
                elif len(pair) == 3:
                    if (pair[1] == "(1)"):
                        origin = pair[0] + " " + pair[1]
                        airline = pair[2]
                    else:
                        origin = pair[0]
                        airline = pair[1] + " " + pair[2]
                elif len(pair) > 3:
                    origin = pair[0] + " " + pair[1]
                    airline = pair[2] + " " + pair[3]
                print("origin: " + origin + " airline: " + airline + " delay: " + row[1])
                table.put_item(
                    Item={
                        'origin': origin,
                        'airline': airline,
                        'delay': row[1]
                    }
                )
                line_count += 1
            print("Line Count: ", line_count)