# SPAC-week5
Weekly assignment in Python. Multithreaded script that can download files listed in an excel-file. 2 example excel-files included.

To run this script with one of the example excel-files (default "test_input_files/jpeg.xlsx"), simply set up a virtual environment with requirements.txt and run "python main.py". Downloaded files can be found in the newly-created "jpeg"-folder, and a report of the process can be found in "reports/test_jpeg.xlsx". The "jpeg_temp"-folder should be empty if the program completed successfully.

The script can be configured to use other excel-files for input, and download other types of files, by adjusting the settings in main.py. 

Note: the input excel-file can have multiple columns containing links, which should be specified with the link_columns setting. The script will try each link in the list of columns until a file of the correct filetype is received, and save it to the folder specified by the downloads_folder setting (technically it is saved to the temporary downloads folder first, and moved once fully written). 
If none of the links in the list succeed, then no files are downloaded. No matter the outcome, a report of how the script did is generated once all possible files are downloaded.
