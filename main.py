# main.py
# Exercise for multithreaded download of PDF-files
# this will be the main controller for the threads

import concurrent.futures as cf
import downloader_funcs as dl


############################################# EDIT THESE:

### examples of "settings profiles"

settings_example = dict(
    # (Windows) paths can have the following syntax:
    #   file, absolute: "C:/folder1/folder2/example.txt"
    #   file, relative: "example.txt" (saved to the folder that this script is placed in) 
    #                       OR "./subfolder_of_current_folder/example.txt"
    #                       OR "../sibling_folder_to_current_folder/example.txt"
    #   folder:         "(...absolute or relative start...)/just_the_folder/"
    input_file = "C:/path/to/GRI_2017_2020.xlsx",
    report_file = "./reports/Metadata2024.xlsx",
    downloads_folder = "./PDFs/",
    temporary_downloads_folder = "./PDFs_temp/",

    # e.g. ["A", "B", "C"] = will try to download from the link in column with header 'A' first, then try subsequent columns
    link_columns = ["Pdf_URL", "Report Html Address"],

    # Column to use for naming downloaded files, 
    # e.g. if value is "A" and cell (A, n)'s value in input file is "example", then the nth file will be named "example.(filetype)"
    naming_column = "BRnum",

    # should match download_is_binary_file
    download_filetype = "pdf",

    # e.g. PDF files are binary, if False then downloaded files will be treated as text files
    download_is_binary_file = True,

    # Whether to download all files from rows in input file, overwriting already-downloaded files in download folder
    # If False, then download just the files from input that don't exist in the folder already
    do_download_all = True,

    # Amount of seconds to wait before giving up on trying to connect to a link
    connection_timeout = 10,
)

# for testing: list of MIME types https://www.iana.org/assignments/media-types/media-types.xhtml
# test of other binary filetype
settings_jpeg = dict(
    input_file = "./test_input_files/jpeg.xlsx",
    report_file = "./reports/test_jpeg.xlsx",
    downloads_folder = "./jpeg/",
    temporary_downloads_folder = "./jpeg_temp/",

    link_columns = ["url"],
    naming_column = "name",

    download_filetype = "jpeg",
    download_is_binary_file = True,
    do_download_all = True,
    connection_timeout = 10,
)

# test of non-binary files (text)
settings_html = dict(
    input_file = "./test_input_files/html.xlsx",
    report_file = "./reports/test_html.xlsx",
    downloads_folder = "./html/",
    temporary_downloads_folder = "./html_temp/",

    link_columns = ["url"],
    naming_column = "name",

    download_filetype = "html",
    download_is_binary_file = False, # NOTE
    do_download_all = True,
    connection_timeout = 10,
)



### settings pertaining to main.py:

# NOTE: change this value to use other "settings profiles"
SETTINGS = settings_jpeg 

# Integer number (e.g. 10) of threads that can be active and download files at a time, 
# set to None to use the concurrent.futures module's default choice
CONNECTIONS_LIMIT = None      

#############################################



def main(connections_limit: int | None = None):
    # initialize input values for module functions
    dl.init(SETTINGS)

    # test inputted file paths for validity before we get started
    dl.test_settings()

    # read data
    data = dl.load_input()

    # pass each row of data to threads
    reports = []
    with cf.ThreadPoolExecutor(max_workers=connections_limit) as executor:
        fs = []
        for row in data.itertuples(index=False, name="datarow"):
            fs.append(executor.submit(dl.thread_job, row, reports))

    # wait for threads to complete their jobs so we don't write the report too soon
    cf.wait(fs)

    # write gathered reports from threads to file
    dl.write_report(reports)

    return


if __name__ == "__main__":

    main(connections_limit=CONNECTIONS_LIMIT)