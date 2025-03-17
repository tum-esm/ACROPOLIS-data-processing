import requests
import os
from datetime import datetime, timezone

from .os_functions import zip_file, hash_file


def upload_file_to_icos_cp(
    config: dict,
    root_directory: str,
    file_name: str,
    site_id: str,
    sampling_height: float,
    data_level: int,
) -> bool:
    """Two step upload process to ICOS Carbon Portal. Register metadata package and upload file.

    Args:
        root_directory (str): root directory of the file
        file_name (str): file name
        site_id (str): site id (must be available in the ICOS CP)
        sampling_height (str): variable sampling height

    Returns:
        Bool: True if successful
    """

    # zip the file and calculate hashsum
    try:
        zipped_filename = zip_file(os.path.join(root_directory, file_name))
        hash_sum = hash_file(zipped_filename)
    except AssertionError:
        print(f"File {file_name} not found")
        return False

    # STEP 1: register metadata package
    datetime_format = '%Y-%m-%dT%H:%M:%S.000Z'
    creation_date = datetime.now(timezone.utc).strftime(
        datetime_format)  # date of the data creation
    if data_level == 2:
        data_object_specification = config["icos_cities_portal"][
            "l2_object_specification"]
    elif data_level == 1:
        data_object_specification = config["icos_cities_portal"][
            "l1_object_specification"]

    payload = {
        'submitterId': config["icos_cities_portal"]["submitter_id"],
        'hashSum': hash_sum,
        'fileName': file_name + '.zip',
        'specificInfo': {
            'station':
            config["icos_cities_portal"]["station_base_url"] + site_id,
            'samplingHeight': sampling_height,
            'production': {
                "creator": config["icos_cities_portal"]["creator_s"],
                "contributors": config["icos_cities_portal"]["contributor_s"],
                "comment": config["icos_cities_portal"]["comment"],
                "hostOrganization": config["icos_cities_portal"]["host_org_s"],
                "creationDate": creation_date
            }
        },
        'objectSpecification': data_object_specification,
        'references': {
            'keywords': config["icos_cities_portal"]["keywords"],
            'duplicateFilenameAllowed': False,
            'autodeprecateSameFilenameObjects': True,
            'partialUpload': False
        },
        'autodeprecateSameFilenameObjects': True,
        'duplicateFilenameAllowed': False,
        'partialUpload': False,
    }

    #Perform the POST request and save cookies to a file
    with requests.Session() as session:
        # authenticate
        session.post("https://cpauth.icos-cp.eu/password/login",
                     data={
                         "mail":
                         config["icos_cities_portal"]["portal_user"],
                         "password":
                         config["icos_cities_portal"]["portal_password"]
                     })

        r = session.post(
            url='https://citymeta.icos-cp.eu/upload', json=payload
        )  # Automatically sets the Content-Type to application/json)

        if r.status_code == 200:
            print(f"Register metadata package for site {site_id}: Successful")
            print(f'Response URL : {r.text}')
            print('Start uploading file')

            filepath = os.path.join(root_directory, file_name + '.zip')
            with open(filepath, "rb") as file:
                rd = session.put(r.text, data=file)
            if rd.status_code == 200:
                print(f"File upload successful")
            else:
                print(f"File upload failed with status code {rd.status_code}")
                print(f"Response: {rd.text}")
                return False
        else:
            print(
                f"Register metadata package: Failed with status code {r.status_code}"
            )
            print(f"Response: {r.text}")
            return False

    return True
