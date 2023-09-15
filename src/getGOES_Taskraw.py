import pendulum

from airflow.decorators import dag

import goes_utils_raw as gur


@dag(
    schedule="35 8 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
    tags=["goes_raw"]
)
def getGOES_taskraw():
    # Get the start and en dates and the directory path
    # Retrieves the current day as start date and One as end date since
    # this is done in automatic mode
    # The directory path is a local one
    passed_arguments_dict = gur.initialize_date_and_directory_path()

    # Get the date range to query
    # In automatic mode it is just the current date
    # passed_arguments_dict = \
    #   gur.get_dates_in_time_interval(passed_arguments_dict)

    # Get the measuring devices
    # passed_arguments_dict = \
    #    gur.get_measuring_devices(passed_arguments_dict)

    # Get the list of URLs
    # passed_arguments_dict = gur.define_url_format(passed_arguments_dict)

    # Download the data
    passed_arguments_dict = gur.download_data(passed_arguments_dict)

    # Get the bucket name
    passed_arguments_dict = gur.get_bucket_name(passed_arguments_dict)

    # Upload on cloud
    # passed_arguments_dict = gur.upload_raw(passed_arguments_dict)

    # Save parameters on local file
    # passed_arguments_dict = gur.save_passed_arguments_locally(passed_arguments_dict)


getGOES_taskraw()
