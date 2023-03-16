import csv
from scipy import io
from sys import argv, stderr

from typing import List

OUTPUT_EXTENSION = '.csv'

def convert_file(filepath: str) -> None:
    # Pick the array with the largest number of elements
    matlab = io.loadmat(filepath)
    array_to_convert = None
    for array in matlab.values():
        if array_to_convert is None or len(array_to_convert) < len(array):
            array_to_convert = array

    # Write values in a CSV format
    output_file = f'{filepath}{OUTPUT_EXTENSION}'
    with open(output_file, 'w') as converted_file:
        writer = csv.writer(converted_file)
        writer.writerows(array_to_convert)

def parse_args(args: List[str]) -> str:
    if len(args) == 0:
        print('ERROR: Please provide the path to the file to convert!', file=stderr)
        exit(1)
    return args[0]

def main(args: List[str]) -> None:
    filepath = parse_args(args)
    convert_file(filepath)

if __name__ == '__main__':
    main(argv[1:])