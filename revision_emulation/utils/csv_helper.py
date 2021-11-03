import csv
class CsvHelper:

    @staticmethod
    def save_to_csv(rows, filename):
        """Converts a list of elements into a CSV file"""

        with open(filename, 'w', encoding='UTF8', newline='') as file:
            writer = csv.writer(file)

            writer.writerows(rows)

    
    @staticmethod
    def read_csv(filepath):
        with open(filepath, encoding='UTF8') as file:
            csv_reader = csv.reader(file)
            rows = []
            for row in csv_reader:
                rows.append(row)
        return rows