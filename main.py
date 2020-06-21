import time
import pandas as pd
import datetime
from pandas import to_numeric, to_datetime
import multiprocessing as mp

def missed_dates(fixed_df):
    max_datetime = fixed_df['lpep_pickup_datetime'].max()
    min_datetime = fixed_df['lpep_pickup_datetime'].min()
    list_missed_dates = []
    group_by_day = fixed_df.lpep_pickup_datetime.dt.date.unique()
    while min_datetime < max_datetime:
        if min_datetime not in group_by_day:
            list_missed_dates.append(min_datetime)
        min_datetime += datetime.timedelta(days=1)
    df = pd.DataFrame(list_missed_dates, columns=["Missed_dates"])
    return df

def count_trips_by_day(fixed_df):
    day_count_trip = fixed_df.set_index("lpep_pickup_datetime").groupby(pd.Grouper(freq='D')).size()
    return day_count_trip

def avg_length_trip(fixed_df):
    avg_trip_person = fixed_df.groupby([fixed_df.lpep_pickup_datetime.dt.month, 'Passenger_count', ])['time_in_trip'].mean()
    return avg_trip_person

def count(rows):
    start_time = min(rows['START_DATE'])
    end_time = max(rows['Lpep_dropoff_datetime'])
    curr = start_time
    adict = {}
    count_trip = 0
    while curr <= end_time:
        interval_active = rows[(rows['lpep_pickup_datetime'] <= curr) & (rows['Lpep_dropoff_datetime'] > curr)]
        adict[curr] = len(interval_active)
        if len(interval_active) > count_trip:
            count_trip = len(interval_active)
        curr = curr + pd.Timedelta(minutes=10)
    new_df = pd.DataFrame(adict, index=['COUNT']).T
    result_start = new_df['COUNT'].idxmax()
    result_end = result_start + datetime.timedelta(minutes=10)
    RESULT = (count_trip, str(result_start) + " " + str(result_end))
    return RESULT

def count_untreatable_rows(fixed_df):
    untreatable_rows = 0
    untreatable_rows += len(fixed_df.loc[fixed_df['Trip_distance'].isnull()])
    untreatable_rows += len(fixed_df.loc[fixed_df['Lpep_dropoff_datetime'].isnull()])
    untreatable_rows += len(fixed_df.loc[fixed_df['Total_amount'].isnull()])
    untreatable_rows += len(fixed_df.loc[fixed_df['time_in_trip'] < 0])
    untreatable_rows += len(fixed_df.loc[fixed_df['lpep_pickup_datetime'].isnull()])
    return untreatable_rows

def reader(filename):
    fixed_df = pd.read_csv(filename,
                           names=['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime', 'Store_and_fwd_flag',
                                  'RateCodeID',
                                  'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude',
                                  'Passenger_count',
                                  'Trip_distance', 'Fare_amount', 'Extra', 'MTA_tax', 'Tip_amount', 'Tolls_amount',
                                  'Ehail_fee', 'Total_amount', 'Payment_type', 'Trip_type'],
                           dtype={"VendorID": str, "lpep_pickup_datetime": str, "Lpep_dropoff_datetime": str,
                                  "Store_and_fwd_flag": str,
                                  "RateCodeID": str, "Pickup_longitude": str, "Pickup_latitude": str,
                                  "Dropoff_longitude": str,
                                  "Dropoff_latitude": str, "Passenger_count": str, "Trip_distance": str,
                                  "Fare_amount": str,
                                  'Extra': str, "MTA_tax": str, "Tip_amount": str, "Tolls_amount": str,
                                  "Ehail_fee": str,
                                  "Total_amount": str, "Payment_type": str, "Trip_type": str}, na_values=['na'],
                           skip_blank_lines=True)
    fixed_df['Total_amount'] = to_numeric(fixed_df['Total_amount'], errors='coerce')
    fixed_df['Trip_distance'] = to_numeric(fixed_df['Trip_distance'], errors='coerce')
    fixed_df['Lpep_dropoff_datetime'] = to_datetime(fixed_df['Lpep_dropoff_datetime'], errors='coerce')
    fixed_df['lpep_pickup_datetime'] = to_datetime(fixed_df['lpep_pickup_datetime'], errors='coerce')
    fixed_df['time_in_trip'] = fixed_df['Lpep_dropoff_datetime'] - fixed_df['lpep_pickup_datetime']
    fixed_df['time_in_trip'] = fixed_df['time_in_trip'].dt.total_seconds()
    return fixed_df

def reader_2(filename):
    rows = pd.read_csv(filename, usecols=['VendorID', 'lpep_pickup_datetime',
                                            'Lpep_dropoff_datetime']).reset_index()
    rows['lpep_pickup_datetime'] = pd.to_datetime(rows['level_1'])
    rows['Lpep_dropoff_datetime'] = pd.to_datetime(rows['VendorID'])
    rows['VendorID'] = rows['level_0']
    rows = rows[['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime']]
    rows['START_DATE'] = rows['lpep_pickup_datetime'].dt.normalize()
    return rows

def average(fixed_df):
    return fixed_df['Total_amount'].mean()

def longest_trip(fixed_df):
    return (fixed_df['time_in_trip'].max(), fixed_df['Trip_distance'][fixed_df['time_in_trip'].idxmax()])

def main():
    pool = mp.Pool(mp.cpu_count())
    start = time.time()
    list_of_filename = ['data.csv', 'data2.csv', 'data3.csv', 'data4.csv']
    fixed_df = pool.map(reader, list_of_filename)
    # task 1_2
    untreatable_rows = sum(list(map(count_untreatable_rows, fixed_df)))
    rows = pool.map(reader_2, list_of_filename)
    counts = max(pool.map(count, rows))[1]
    average_sum = sum(list(map(average, fixed_df))) / len(list(map(average, fixed_df)))
    lng_trip = max(pool.map(longest_trip, fixed_df))[1]
    df = pd.DataFrame([[average_sum, lng_trip, counts, untreatable_rows]],
                          columns=["Average total sum", "Distance", "Busiest period", "Untreatable_rows"])
    df.to_csv('general-stats.csv', index=False)

    #task 1_3
    missed_dates_df = pd.concat(map(missed_dates, fixed_df))
    missed_dates_df.to_csv('missing-dates.csv', index=False)

    #task 1_4
    count_trips_by_day_df = pd.concat(map(count_trips_by_day, fixed_df))
    count_trips_by_day_df.to_csv('usage-stats.csv', sep=",", header=False)

    #task 1_5
    avg_length_trip_df = pd.concat(map(avg_length_trip, fixed_df))
    avg_length_trip_df.to_csv('trip-stats.csv', sep=",", header=False)
    print(time.time() - start)

if __name__ == "__main__":
    main()