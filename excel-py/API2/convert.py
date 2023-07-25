import json
import csv

def convert(file):
    # Load the JSON file
    with open(f'{file}', encoding=('utf-8')) as f:
        data = json.load(f)

    # Open (or create) a CSV file and write the data
    with open('output.csv', 'w', newline='', encoding=('utf-8')) as f:
        writer = csv.writer(f)

        # Get the keys from the first dictionary in the list and write them as the first row (header)
        header = data[0].keys()
        writer.writerow(header)

        # Write the data
        for row in data:
            writer.writerow(row.values())

if __name__ == '__main__':
    convert("filtered.json")