import csv


def extract_unique_column_value(input_filename, output_filename, column):
    # Extract unique values from a column in a CSV file
    with open(input_filename, "r") as input_file, open(
        output_filename, "w"
    ) as output_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            output_file.write(row[column] + "\n")

    with open(output_filename, "r") as f:
        unique_values = set(f.readlines())

    with open(output_filename, "w") as f:
        f.writelines(unique_values)
