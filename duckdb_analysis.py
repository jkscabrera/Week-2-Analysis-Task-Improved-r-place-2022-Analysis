import duckdb
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

            # load csv into duckdb
            connection = duckdb.connect()
            connection.execute("""CREATE OR REPLACE TABLE canvas_data AS 
                               SELECT timestamp, color, pixel
                               FROM read_csv_auto('2022_place_canvas_history.csv', 
                                    columns = {'timestamp': 'VARCHAR', 
                                               'userid': 'VARCHAR',
                                               'color': 'VARCHAR', 
                                               'pixel': 'VARCHAR'},
                                    delim = ',', 
                                    quote = '"', 
                                    escape = '"', 
                                    skip = 1, 
                                    ignore_errors = true);""")


            # filter timestamps
            query = f"""SELECT timestamp, pixel, color
                        FROM canvas_data
                        WHERE CAST(timestamp AS TIMESTAMP) 
                        BETWEEN TIMESTAMP '{start.strftime('%Y-%m-%d %H:%M:%S')}' 
                        AND TIMESTAMP '{end.strftime('%Y-%m-%d %H:%M:%S')}';"""
            newdf = connection.execute(query).df()

            # most common pixel and color
            pixel = newdf['pixel'].mode()[0] if not newdf.empty else None
            color = newdf['color'].mode()[0] if not newdf.empty else None

            # record end time
            end_time = perf_counter_ns()

            # get execution time
            execution_time = (end_time - start_time) / 1000000

            print("DONE")

            return pixel, color, execution_time

        except ValueError:
            print("Invalid Format")

def main():
    color, pixel, executiontime = colorpixel()
    print(color, pixel, executiontime)

if __name__ == "__main__":
    main()
