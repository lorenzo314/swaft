import pathlib
import pandas as pd
import json
import os
import os.path
from tqdm.auto import tqdm
from datetime import datetime
import datetime as dtm
from urllib import request
import re
import requests
import pytz
from pytz import timezone

from airflow.decorators import task

# Necessary to do pip install --upgrade google-cloud-storage
# See https://stackoverflow.com/questions/50840511/google-cloud-import-storage-cannot-import-storage
from google.cloud import storage


@task()
def initialize_date_and_directory_path():
    start_date = dtm.datetime.now() - dtm.timedelta(days=3)
    end_date = start_date
    sat_number = '16'
    measuring_device = 'mpsh'
    raw_directory_path = \
        "/home/lorenzo/spaceable/airflow_goes_scraping/data_scraping/GOES-16/mpsh"
    silver_directory_path = \
        "/home/lorenzo/spaceable/airflow_goes_scraping/data/data_aggregating/GOES-16/mpsh"

    return {
        "start_date": start_date,
        "end_date": end_date,
        "raw_directory_path": raw_directory_path,
        "silver_directory_path": silver_directory_path,
        "sat_number": sat_number,
        "measuring_device": measuring_device
    }


@task()
def get_dates_in_time_interval(passed_arguments_dict: dict):
    def serialize_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    """
    Get the dates over the selected time interval.
    For manual mode, frequency is either month or day.
    For automatic mode, frequency is day.
    For automatic mode, the right time bound is equal to the left time bound.

    Parameters
    ----------
    passed_arguments_dict : a dictionary containing
    1 start_date : datetime.datetime
        the left bound for generating scraping time interval.
    2 end_date : datetime.datetime
        Right bound for generating scraping time interval.
    3 monthly : bool
        If True, frequency is month (manual mode).
        If False, frequency is day.
        If False and end_date is None,
        the right time bound is equal to the left time bound

    Returns
    -------
    A dictionary containing the 3 keys in input plus
    dates_in_time_interval : DatetimeIndex
    The range of equally spaced time points between start_date and end_date.
    """

    start_date = passed_arguments_dict["start_date"]
    end_date = passed_arguments_dict["end_date"]
    monthly = passed_arguments_dict["monthly"]

    if monthly is True:
        dates_in_time_interval = pd.date_range(
            start_date, end_date, freq='MS'
        )
    else:
        if end_date is None:
            end_date = start_date

        dates_in_time_interval = pd.date_range(
            start_date, end_date, freq='d'
        )

    # Transform the DatetimeIndex type in a type that can be serialized
    dates_in_time_interval = list(dates_in_time_interval)
    tmp_list = [i.to_pydatetime() for i in dates_in_time_interval]
    dates_in_time_interval = \
        [json.dumps(i, default=serialize_datetime) for i in tmp_list]

    passed_arguments_dict["dates_in_time_interval"] = dates_in_time_interval

    return passed_arguments_dict


@task
def get_measuring_devices(passed_arguments_dict: dict):
    measuring_devices = ['mag', 'swepam', 'epam', 'sis']

    passed_arguments_dict["measuring_devices"] = measuring_devices

    return passed_arguments_dict


@task
def get_bucket_name(passed_arguments_dict: dict):
    bucket_name = "v3mhxhemaey1ohkyil8v"

    passed_arguments_dict["bucket_name"] = bucket_name

    return passed_arguments_dict


@task()
def download_data(passed_arguments_dict: dict):
    scraping_path = passed_arguments_dict["raw_directory_path"]
    sat_number = passed_arguments_dict["sat_number"]
    start_date = passed_arguments_dict["start_date"]
    end_date = passed_arguments_dict["end_date"]
    measuring_device = passed_arguments_dict["measuring_device"]

    """
    Download GOES data from :
    https://satdat.ngdc.noaa.gov/sem/goes/data/avg/
    for GOES-05 to GOES-12 (measuring device : eps) and GOES-13 to GOES-15 (measuring device : epead)
    https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/
    for GOES-16 and GOES-17 (measuring devices : mpsh and spgs)
    https://ftp.swpc.noaa.gov/pub/warehouse/ for SWPC-NOAA GOES processed data
    
    Parameters
    ----------
    scraping_path : str
        Path where the data will be downloaded
    sat_number : int
        Number of the GOES spacecraft to scrape. Scrape SWPC-NOAA GOES processed data if None.
    start_date : datetime
        Starting date
    end_date : datetime
        Ending date
    measuring_device : str
        Measuring device of GOES spacecraft

    Returns
    -------
    None.
    """
    sat_name = 'GOES-' + str(sat_number).zfill(2)
    if sat_name in ['GOES-01', 'GOES-02', 'GOES-03', 'GOES-04', 'GOES-05', 'GOES-06', 'GOES-07', 'GOES-08', 'GOES-09', 'GOES-10', 'GOES-11',
                    'GOES-12', 'GOES-13', 'GOES-14', 'GOES-15']:
        print('Scraping ' + sat_name + ' data (' + measuring_device + ') from ' + str(start_date.year) + '-' + str(start_date.month).zfill(2) +
              '-' + str(start_date.day).zfill(2) + ' to ' + str(end_date.year) + '-' + str(end_date.month).zfill(2) + '-' +
              str(end_date.day).zfill(2) + '...')
        for date in tqdm(pd.date_range(start_date, end_date, freq='M')):
            strmonth = str(date.month).zfill(2)
            stryear = str(date.year)
            strday = str(calendar.monthrange(date.year, date.month)[1])
            token = 'https://satdat.ngdc.noaa.gov/sem/goes/data/avg'
            short_sat_name = 'g' + sat_name[-2:]

            file_name = short_sat_name + '_' + measuring_device + '_5m_' + stryear + strmonth + '01_' + stryear + strmonth + strday + '.csv'
            token = token + '/' + stryear + '/' + strmonth + '/goes' + str(sat_name[-2:]) + '/csv'

            url = token + '/' + file_name
            check_url = is_url(url)
            if check_url:
                request.urlretrieve(url, scraping_path / file_name)
            else:
                print(url + " doesn't exist")
        print('Scraping finished for ' + sat_name + ' ' + measuring_device)

    elif sat_name in ['GOES-16', 'GOES-17']:
        print('Scraping ' + sat_name + ' data (' + measuring_device + ') from ' + str(start_date.year) + '-' + str(start_date.month).zfill(2) +
              '-' + str(start_date.day).zfill(2) + ' to ' + str(end_date.year) + '-' + str(end_date.month).zfill(2) + '-' +
              str(end_date.day).zfill(2) + '...')
        for date in tqdm(pd.date_range(start_date, end_date, freq='D')):
            strday = str(date.day).zfill(2)
            strmonth = str(date.month).zfill(2)
            stryear = str(date.year)
            source = 'https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/'
            short_sat_name = 'g' + sat_name[-2:]

            version = get_version(date, measuring_device)
            file_name = 'sci_' + measuring_device + '-l2-avg5m_' + short_sat_name + '_d' + stryear + strmonth + strday + '_' + version + '.nc'
            source = source + 'goes' + str(sat_name[-2:]) + '/l2/data/' + measuring_device + '-l2-avg5m/' + stryear + '/' + strmonth

            url = source + '/' + file_name
            check_url = is_url(url)
            if check_url:
                request.urlretrieve(url, scraping_path / file_name)
            else:
                print(url + " doesn't exist")
        print('Scraping finished for ' + sat_name + ' ' + measuring_device)
    else:
        print('Scraping SWPC-NOAA GOES processed data from ' + str(start_date.year) + '-' + str(start_date.month).zfill(2) +
              '-' + str(start_date.day).zfill(2) + ' to ' + str(end_date.year) + '-' + str(end_date.month).zfill(2) + '-' +
              str(end_date.day).zfill(2) + '...')
        list_years = range(start_date.year, end_date.year+1, 1)
        token = 'ftp.swpc.noaa.gov'
        ftp = FTP(token)
        ftp.login()
        ftp.cwd('pub/warehouse/')
        for year in tqdm(list_years):
            ftp.cwd(str(year))
            file_name = str(year) + '_DPD.txt'
            with open(str(scraping_path) + '/' + file_name, 'wb') as file:
                ftp.retrbinary(f"RETR {file_name}", file.write)
            ftp.cwd('..')
        print('Scraping finished for SWPC-NOAA GOES data.')

    return passed_arguments_dict


def get_version(date, measuring_device):
    """
    Get the version of the data of GOES according to the date and the measuring device (GOES-16 and GOES-17 only)

    Parameters
    ----------
    date : dtm.datetime
        date of the data
    measuring_device : str
        measuring device of GOES spacecraft
    Returns
    -------
    None.
    """

    # Added by me
    # Without it, it gives
    # T_ype Errorr can't compare offset-naive and offset-aware datetimes
    # See https://www.pythonclear.com/errors/typeerror-cant-compare-offset-naive-and-offset-aware-datetimes/?utm_content=cmp-true
    eastern = timezone('US/Eastern')

    if measuring_device == 'mpsh':
        if date < eastern.localize(dtm.datetime(year=2019, month=3, day=21)):
            return 'v1-0-0'
        elif eastern.localize(dtm.datetime(year=2019, month=3, day=21))\
                <= date < eastern.localize(dtm.datetime(year=2019, month=5, day=3)):
            return 'v1-0-1'
        elif eastern.localize(dtm.datetime(year=2019, month=5, day=3))\
                <= date < eastern.localize(dtm.datetime(year=2020, month=12, day=1)):
            return 'v1-0-2'
        elif eastern.localize(dtm.datetime(year=2020, month=12, day=1))\
                <= date < eastern.localize(dtm.datetime(year=2022, month=4, day=27)):
            return 'v1-0-3'
        else:
            return 'v2-0-0'
    elif measuring_device == 'sgps':
        if date < dtm.datetime(year=2021, month=8, day=11):
            return 'v1-0-1'
        else:
            return 'v2-0-0'


def is_url(url):
    """
    Check if the passed url exists or not

    INPUT:
    param: url (str)
    url to test

    OUTPUT: True if the url exists, False if not
    """
    r = requests.get(url)
    if r.status_code == 429:
        print('Retry URL checking (429)')
        time.sleep(5)
        return is_url(url)
    elif r.status_code == 404:
        return False
    else:
        return True


@task()
def save_passed_arguments_locally(passed_arguments_dict: dict):
    date_time = datetime.now()
    str_date_time = date_time.strftime("%d%m%YT%H%M%S")
    str_date_time = f"{str_date_time}.txt"
    output_file = os.path.join(
        passed_arguments_dict["raw_directory_path"],
        str_date_time
    )

    with open(output_file, "w") as file:
        if passed_arguments_dict["start_date"] is not None:
            file.write(f'start_date {passed_arguments_dict["start_date"].strftime("%m/%d/%Y")}\n')
        else:
            file.write(f'start_date {str(None)}\n')

        if passed_arguments_dict["end_date"] is not None:
            file.write(
                f'end_date {passed_arguments_dict["end_date"].strftime("%m/%d/%Y")}\n'
            )
        else:
            file.write(f'end_date {str(None)}\n')

        if passed_arguments_dict["source"] is not None:
            file.write(f'source {passed_arguments_dict["source"]}\n')
        else:
            file.write(f'source {str(None)}\n')

        if passed_arguments_dict["raw_directory_path"] is not None:
            file.write(f'directory_path {passed_arguments_dict["raw_directory_path"]}\n')
        else:
            file.write(f'directory_path {str(None)}\n')

        if passed_arguments_dict["dates_in_time_interval"] is not None:
            dates_in_time_interval = passed_arguments_dict["dates_in_time_interval"]
            assert type(dates_in_time_interval) == list
            for i in dates_in_time_interval:
                assert type(i) == str
                file.write(f'dates_in_time_interval {i}\n')
        else:
            file.write(f'dates_in_time_interval {str(None)}\n')

        if passed_arguments_dict["measuring_devices"] is not None:
            measuring_devices = passed_arguments_dict["measuring_devices"]
            assert type(measuring_devices) == list
            for i in measuring_devices:
                file.write(f'measuring device {i}\n')
        else:
            file.write(f'measuring devices {str(None)}\n')

        if passed_arguments_dict["list_url"] is not None:
            list_url = passed_arguments_dict["list_url"]
            assert type(list_url) == list
            for i in list_url:
                file.write(f'url {i}\n')
        else:
            file.write(f'list url {str(None)}\n')

        if passed_arguments_dict["output_files"] is not None:
            output_files = passed_arguments_dict["output_files"]
            assert type(output_files) == list
            for i in output_files:
                file.write(f'output file {i}\n')
        else:
            file.write(f'output_files {str(None)}\n')

    return passed_arguments_dict


@task()
def upload_raw(passed_arguments_dict: dict):
    bucket_name = passed_arguments_dict["bucket_name"]
    output_files = passed_arguments_dict["output_files"]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for i in output_files:
        destination_blob_name = i
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0
        source_file_name = i
        blob.upload_from_filename(
            source_file_name,
            if_generation_match=generation_match_precondition
        )
        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )

    return passed_arguments_dict
