import os
import hashlib
import zipfile


def ensure_data_dir(dir_path: str) -> None:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


# calculate hashsum of a file
def hash_file(file_path: str) -> str:
    """Generates hashsum of a file
    Args:
        fname (str): _description_
    Returns:
        str: sha256 hashsum of the file
    """
    assert os.path.isfile(file_path)
    with open(file_path, "rb") as f:
        bytes = f.read()  # read entire file as bytes
        return hashlib.sha256(bytes).hexdigest()


def zip_file(file_path: str) -> str:
    """Generates zipped version of a file
    Args:
        file_path (str): path to the file to be zipped
    Returns:
        str: path to the zipped file
    """
    assert os.path.isfile(file_path)
    zip_file_name = file_path + '.zip'
    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Get the base name of the file (no directory structure)
        file_name_in_zip = os.path.basename(file_path)
        # Add the file to the zip, with no folder structure
        zipf.write(file_path, arcname=file_name_in_zip)
    return zip_file_name
