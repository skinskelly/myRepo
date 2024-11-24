import pyodbc
import pandas as pd


# Step 1: Load serial numbers from the text file
def load_serial_numbers(file_path):
    with open(file_path, 'r') as file:
        serial_numbers = [line.strip() for line in file.readlines()]
    return serial_numbers


# Step 2: Connect to the SQL Server database and execute the query
def fetch_query_results(conn_str, query):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    # Convert results to a list of tuples for easier processing
    query_results = [(row[0], row[1].strip()) for row in results]  # (code, numerical value)
    return query_results


# Step 3: Match and sort based on last three digits
def match_and_sort(serial_numbers, query_results):
    matched_results = []
    for serial in serial_numbers:
        last_three_digits = serial[-3:]

        # Find the matching SQL row
        match = next((qr for qr in query_results if qr[1].endswith(last_three_digits)), None)
        if match:
            matched_results.append((serial, match[1], match[0]))  # (serial number, numerical value, code)

    return matched_results


# Step 4: Save the results to a CSV file
def save_to_csv(matched_results, output_file):
    df = pd.DataFrame(matched_results, columns=['TextFile Number', 'Serial', 'MAC'])
    df.to_csv(output_file, index=False)

# Connection string function to get the details for connecting the database
def connection_string():
    driver = '{ODBC Driver 17 for SQL Server}'
    server = 'dbcontent6848.footfallcam.com,41433'
    uid = 'checker'
    pwd = 'checker888'

    return f'DRIVER={driver};SERVER={server};UID={uid};PWD={pwd}'


# Main function to execute the steps
def main():
    # Change this serial whenever running the SQL
    counterSerial = '10000000d3155e3e'

    # File paths and connection details
    text_file_path = 'serial_numbers.txt'
    output_csv = 'matched_results.csv'
    conn_str = connection_string()
    query = f'SELECT chipserial, serial FROM eslTags WHERE CounterSerial = \'{counterSerial}\''

    # Step-by-step execution
    serial_numbers = load_serial_numbers(text_file_path)
    query_results = fetch_query_results(conn_str, query)
    matched_results = match_and_sort(serial_numbers, query_results)
    save_to_csv(matched_results, output_csv)

    print(f"Matched results have been saved to {output_csv}")


# Run the script
if __name__ == '__main__':
    main()
