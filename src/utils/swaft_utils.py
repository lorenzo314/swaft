# ONLY AUTOMATIC DOWNLOAD IMPLEMENTED FOR NOW
# NO ARGUMENTS PASSED TO THE MAIN FUNCTION
# TODO: Implement manual download
# TODO: Settle the args issue in DAGS
# TODO: Clarify if it is possible to use DAG params with TaskFlow

import os.path

from airflow.decorators import task


@task()
def initialize_directory_path():
    """
    Developed to test the DAG in local environment
    TODO: it will be necessary to settle the args issue in DAGS

    The code is from the check_passes_arguments function for the
    automatic download case
    """

    base_directory_path = "/home/lorenzo/spaceable/swaft/data"
    raw_directory_path = os.path.join(base_directory_path, "raw_data")
    silver_directory_path = os.path.join(base_directory_path, "silver_data")

    # A DECORATED FUNCTION SHOULD RETURN A DICTIONARY,
    # OTHERWISE IT GIVES ERRORS
    return {
        "base_directory_path": base_directory_path,
        "raw_directory_path": raw_directory_path,
        "silver_directory_path": silver_directory_path
    }


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
