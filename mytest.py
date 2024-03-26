import sys


def process(input_file, output_file):
    # Your processing logic here
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    print(f"length of arguments = {len(sys.argv)}")
    if len(sys.argv) != 3:
        print("Usage: python myprogram.py <input_file> <output_file>")
        sys.exit(1)

    # Get input and output file paths from command-line arguments
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f" value of arguments(0) :{sys.argv[0]}")

    # Call the process method with the input and output file paths
    process(input_file, output_file)
