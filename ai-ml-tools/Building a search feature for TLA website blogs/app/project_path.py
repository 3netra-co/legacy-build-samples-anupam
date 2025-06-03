import os
import pathlib

# Get the current file path
current_file_path = pathlib.Path(__file__)

PROJECT_PATH = current_file_path
current_file_name = current_file_path.name
path = PROJECT_PATH
number = 0
while path.name != 'app' and number <= 4:
    path = path.parent
    current_file_name = PROJECT_PATH.name
    print(current_file_name,path)
    number += 1
PROJECT_PATH = path

