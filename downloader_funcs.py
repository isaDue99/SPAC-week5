# downloader_funcs.py
# will contain functions to be delegated to threads in main.py

import os
import requests as rq
import pandas as pd


### global values are set when init() is run

def init(settings: dict) -> None:
    """
    Intializes chosen settings in the global scope of file downloader thread module. Important to run this function first!
    """
    global INPUT_FILE, REPORT_FILE, DL_FOLDER, TEMP_DL_FOLDER, LINK_COLS, NAMING_COL, FILETYPE, IS_BIN, DOWNLOAD_ALL, TIMEOUT

    try:
        INPUT_FILE = settings["input_file"]
        REPORT_FILE = settings["report_file"]
        DL_FOLDER = settings["downloads_folder"]
        TEMP_DL_FOLDER = settings["temporary_downloads_folder"]

        LINK_COLS = settings["link_columns"]
        NAMING_COL = settings["naming_column"]

        FILETYPE = settings["download_filetype"]
        IS_BIN = settings["download_is_binary_file"]

        DOWNLOAD_ALL = settings["do_download_all"]

        TIMEOUT = settings["connection_timeout"]
    except KeyError:
        print("All fields in settings must be set; refer to examples in main.py for syntax. Exiting...")
        exit(1)

    ### create the folders we need
    # don't need to do INPUT_FILE since it logically must exist at a path already
    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(DL_FOLDER), exist_ok=True)
    os.makedirs(os.path.dirname(TEMP_DL_FOLDER), exist_ok=True)



### some primary functions main thread will use

def test_settings() -> None:
    """
    Tests whether accessing inputted files will result in errors. Useful for course-correcting before you start processing huge inputs.
    """

    _test_file(INPUT_FILE, "r")
    _test_file(REPORT_FILE, "w")


def _test_file(path, mode) -> None:
    try:
        with open(path, mode) as f:
            pass
    except FileNotFoundError:
        print(f"File \"{path}\" not found. Exiting...")
        exit(1)
    except PermissionError:
        print(f"No permission to access file \"{path}\". Exiting...")
        exit(1)
    except IsADirectoryError:
        print(f"Expected a file, but file \"{path}\" is a directory. Exiting...")
        exit(1)


def load_input() -> pd.DataFrame:
    """
    Reads the excel file at input_file path, returns a pandas.DataFrame containing first the naming column (naming_column), followed by link-containing columns (link_columns)
    """

    cols = [NAMING_COL] + LINK_COLS
    df = pd.read_excel(INPUT_FILE, usecols=cols, engine_kwargs={"read_only":True})
    return df


def write_report(reports: list[pd.Series]) -> None:
    """
    Writes a collection of reports (list of pandas.Series objects) to an excel file at path report_file. Will overwrite previous reports at this path
    """

    # turn our reports into a dataframe for nice printing
    df = pd.DataFrame()
    for item in reports:
        df = pd.concat([df, item.to_frame().T], ignore_index=True)
        # sort the rows of the report by the names of downloaded (or attempted) files
        df = df.sort_values(by=['name'])

    # write it
    with pd.ExcelWriter(REPORT_FILE, mode='w') as writer:  
        df.to_excel(writer)


def thread_job(input_row: pd.DataFrame, reports: list[pd.Series]) -> None:
    """
    The routine for one file-downloading thread. 
    
    Given a row of data from the input file, the thread will attempt to download just one file from the contained list of links.
    The first link to successfully return the kind of file we ask for (download_filetype) will be saved in the chosen downloads folder, and the thread will add a summary to the reports list.
    If no links produce the kind of file we're looking for, then no file will be added to downloads folder, but a summary is still added to the reports list.
    """

    name, links = _unpack_input(input_row)

    # check whether we should try to download from links on every row of input, and whether a file with (name) already exists in download folder
    if DOWNLOAD_ALL == False and _file_exists(name):
        return

    # make http request(s) for the files
    exceptions = []
    response = _try_links(links, exceptions)
    if response is not None:
        # if successful: open pdf download folder and save pdf there
        _save_file(response, name)
        print(f"Downloaded \"{name}\"")
        # add success/failure to report collection
        _add_to_report(reports, name, success=True, response=response, exceptions=exceptions)
    else:
        _add_to_report(reports, name, success=False, exceptions=exceptions)



### functions helping each thread do its job

def _unpack_input(input, name_col_i=0) -> tuple[str, list[str]]:
    """
    Unpacks an input row from dataset into a name (to be given to a future downloaded file) and a list of links, which are returned. "nan" values are removed.
    
    name_col_i indicates the index of the column that contains the name, the rest of the input_row is assumed to be links.
    name_col_i defaults to being the first column, since this is the how load_input() constructs a row of data. 
    """

    name = input[name_col_i]
    # all other columns are assumed to contain links
    links = list(input[:name_col_i] + input[name_col_i+1:])
    # remove nan values
    links = [x for x in links if str(x) != "nan"]
    return name, links


def _file_exists(name: str) -> bool:
    """
    Checks download folder for whether a file named (name).FILETYPE already exists, returns True or False
    """
    if os.path.isfile(DL_FOLDER + name + "." + FILETYPE):
        return True
    return False


def _try_links(links: list[str], exceptions: list[Exception]) -> rq.Response | None:
    """
    Tries to download a file from given list of links (invalid URLs are skipped). 
    A connection will time out after TIMEOUT seconds.
    First link to produce a valid file of type FILETYPE returns its http response, which contains the content of the desired file. 
    If no links produce a valid file, then returns None.
    If any exceptions are encountered, they will be added to exceptions list.
    """

    res = None
    for url in links:
        try:
            r = rq.get(url, timeout=TIMEOUT)
        except Exception as e:
            # fail silently, but save exception for adding to the report
            exceptions.append(e)
        else:
            if r.status_code == 200 and _is_correct_filetype(r):
                res = r
    return res


def _is_correct_filetype(response: rq.Response) -> bool:
    """
    Checks if http response contains a valid file of type FILETYPE, returns True if it does and False if not.
    Very hacky solution, so may be incorrect for more obscure values of FILETYPE.
    """

    if FILETYPE in response.headers["content-type"]:
        return True
    return False


def _save_file(response: rq.Response, name: str) -> None:
    """
    Save the file from http response as a file in DL_FOLDER/name.FILETYPE. Overwrites already present files with the same path.
    Whether the contents of the file are written to file as raw bytes or text depends on IS_BIN setting.
    """

    # idea: switch to iterating thru response content/text stream-style in case we get huge files back?
    # tried it. kinda slow compared to downloading the content immediately. not worth it for files that can be contained in memory

    # save file to temp folder first
    save_path = TEMP_DL_FOLDER + name + "." + FILETYPE
    if IS_BIN:
        with open(save_path, "wb") as f:
            f.write(response.content)
    else:
        with open(save_path, "w", encoding=response.encoding) as f:
            f.write(response.text)
    # then move it to actual downloads folder
    final_path = DL_FOLDER + name + "." + FILETYPE
    os.replace(save_path, final_path)


def _add_to_report(reports: list[pd.Series], 
                   name: str, 
                   success: bool, 
                   response: rq.Response | None = None, 
                   exceptions: list[Exception] | None = None) -> None:
    """
    Add result of download attempt, indicated by success bool, to list containing reports. 
    Name is the identifier for each row of items, and will probably correspond to values in NAMING_COL.
    If http response is given, then the url of the response will be added to report.
    If exceptions list is given, then any exceptions encountered will be added to report.
    Actual report file should be written after threads are done by the main thread, using write_report().
    """

    # NOTE to self: accessing shared variables/memory is danger territory for threads
    content = {
        "name": name, 
        "success?": success,
        "from url": "",
        "exceptions encountered": "",
    }
    if response is not None:
        content["from url"] = response.url
    if exceptions:
        content["exceptions encountered"] = " ; AND ; ".join([str(e) for e in exceptions])

    s = pd.Series(content)
    reports.append(s) # appending is thread-safe https://docs.python.org/3/faq/library.html#what-kinds-of-global-value-mutation-are-thread-safe
