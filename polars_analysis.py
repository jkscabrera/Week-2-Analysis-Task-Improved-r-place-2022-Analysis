import polars as pl
from datetime import datetime
from time import perf_counter_ns

# get most placed color and pixel coord
def colorpixel(parquetfile):
    while True:
        try:
            # get start and end hours
            start_hour = input("Choose a start time using a YYYY-MM-DD HH format: ")
            end_hour = input("Choose an end time using a YYYY-MM-DD HH format: ")

            start = datetime.strptime(start_hour, "%Y-%m-%d %H")
            end = datetime.strptime(end_hour, "%Y-%m-%d %H")

            # record start time
            start_time = perf_counter_ns()

            # read csv
            df = pl.scan_parquet(parquetfile)

            # filter rows
            newdf = df.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
          
            # calculate modes for color and pixel
            pixel = newdf.select(pl.col("coordinate").mode())
            color = newdf.select(pl.col("pixel_color").mode())

            # get output
            pixel_result = pixel.collect()
            color_result = color.collect()

            # record end time
            end_time = perf_counter_ns()

            # get execution time
            execution_time = (end_time - start_time) / 1000000

            print("DONE")

            return pixel_result, color_result, execution_time

        except ValueError:
            print("Invalid Format")

def main():
    parquet_file = "final.parquet"
    pixel, color, executiontime = colorpixel(parquet_file)
    print(pixel, color, executiontime)

if __name__ == "__main__":
    main()
