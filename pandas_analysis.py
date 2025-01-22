import pandas as pd
from datetime import datetime
from time import perf_counter_ns

# get most placed color and pixel coord
def colorpixel():
    while True:
        try:
            # get start and end hours
            start_hour = input("Choose a start time using a YYYY-MM-DD HH format: ")
            end_hour = input("Choose an end time using a YYYY-MM-DD HH format: ")

            start = datetime.strptime(start_hour, "%Y-%m-%d %H")
            end = datetime.strptime(end_hour, "%Y-%m-%d %H")

            # record start time
            start_time = perf_counter_ns() 

            df = pd.read_csv('2022_place_canvas_history.csv', 
                                usecols = [0, 2, 3], 
                                names = ['timestamp', 'color', 'pixel'], 
                                skiprows = 1)

            # change timestamp col to datetime format
            df['timestamp'] = pd.to_datetime(df['timestamp'], 
                                             errors = 'coerce', 
                                             utc = True)
            df['timestamp'] = df['timestamp'].dt.tz_convert(None)

            # filter rows based on start and end times
            newdf = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

            # record end time
            end_time = perf_counter_ns()

            # get execution time
            execution_time = (end_time - start_time) / 1000000

            return newdf["pixel"].mode(), newdf["color"].mode(), execution_time

        except ValueError:
            print("Invalid Format")

def main():
    color, pixel, executiontime = colorpixel()
    print(color, pixel, executiontime)

if __name__ == "__main__":
    main()