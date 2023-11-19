import pandas as pd
from datetime import datetime, time
import random

# Define the format of your input datetime string
input_format = "%a, %d %b %Y %I:%M:%S %p"
output_date_format = "%d-%b-%y"  # Format for "Date of Duty"

# Define the input and output file paths
input_csv_file = '{insert your input filePath here}'
output_csv_file = '{insert your output filePath here}'

def read_csv(file_path):
    # Read the CSV file into a DataFrame
    return pd.read_csv(file_path)

def generate_random_time_readable(start_time, end_time):
    # Generate a random time within the specified range in hours and minutes
    minutes_between = (end_time.hour * 60 + end_time.minute) - (start_time.hour * 60 + start_time.minute)
    random_minutes = random.randint(0, minutes_between)
    random_hours = random_minutes // 60
    remaining_minutes = random_minutes % 60
    random_time = time(start_time.hour + random_hours, start_time.minute + remaining_minutes)
    return random_time

def is_within_shift_range(parsed_time, start_time, end_time):
    # Check if the parsed time is within the specified shift range
    return start_time <= parsed_time.replace(second=0, microsecond=0) <= end_time

def process_shift(df, shift_name, start_time, end_time):
    # Process a specific shift and update the DataFrame
    correct_range = 0
    incorrect_range = 0

    for i in range(len(df)):
        timestamp = df.at[i, 'Timestamp']
        shift = df.at[i, 'Shift']
        
        try:
            parsed_date = datetime.strptime(timestamp, input_format)
            parsed_time = parsed_date.time()

            if shift == shift_name:
                if is_within_shift_range(parsed_time, start_time, end_time):
                    correct_range += 1
                else:
                    incorrect_range += 1
                    random_time = generate_random_time_readable(start_time, end_time)
                    df.at[i, 'Timestamp'] = parsed_date.replace(hour=random_time.hour, minute=random_time.minute).strftime(input_format)
        except ValueError:
            print(f"Failed: {timestamp}")

    return correct_range, incorrect_range

def replace_selectAmbGen_with_ambChecklist(df):
    # Loop through the DataFrame and replace "Please Select The Ambulance Gen" with values from the "Ambulance Checklist" column
    for i in range(len(df)):
        if df.at[i, 'Ambulance Checklist'] != df.at[i, 'Please Select The Ambulance Gen']:
            df.at[i, 'Please Select The Ambulance Gen'] = df.at[i, 'Ambulance Checklist']
            
def update_date_of_duty(df):
    # Update the "Date of Duty" column to match the date from the "Timestamp" column
    for i in range(len(df)):
        timestamp = df.at[i, 'Timestamp']
        try:
            parsed_date = datetime.strptime(timestamp, input_format)
            date_of_duty = parsed_date.strftime(output_date_format)
            df.at[i, 'Date of Duty'] = date_of_duty
        except ValueError:
            print(f"Failed to update Date of Duty for row {i}: {timestamp}")

def main():
    # Read the CSV file
    df = read_csv(input_csv_file)

    # Define shift information
    day_shift_start = time(8, 0)
    day_shift_end = time(10, 0)
    night_shift_start = time(20, 0)
    night_shift_end = time(22, 0)
    
    # Update the "Date of Duty" column to match the date from the "Timestamp" column
    update_date_of_duty(df)

    # Process day shift
    day_correct, day_incorrect = process_shift(df, 'Day', day_shift_start, day_shift_end)

    # Process night shift
    night_correct, night_incorrect = process_shift(df, 'Night', night_shift_start, night_shift_end)

    # Replace "Please Select The Ambulance Gen" with values from the "Ambulance Checklist" column
    replace_selectAmbGen_with_ambChecklist(df)
    

    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_csv_file, index=False)
    
    unique_callsigns = df['Ambulance Callsign'].unique()
    print(unique_callsigns)

    # Print results
    print(f"Day Shift: Correct Timing Count - {day_correct}, Incorrect Timing Count - {day_incorrect}")
    print(f"Night Shift: Correct Timing Count - {night_correct}, Incorrect Timing Count - {night_incorrect}")

if __name__ == "__main__":
    main()
