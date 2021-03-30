"""
Project # 1 
Name: Chris Jackson
Time to completion: 4ish hours
Comments:

Sources: 
Used some previous knowledge with the json module as well as the json module documentation (specifically
    json.loads() and json.dumps())

Used https://stackoverflow.com/questions/3678869/pythonic-way-to-combine-two-lists-in-an-alternating-fashion in my
    write_xml_string(data) function to combine two lists in alternating order.
"""

import json
import xml.etree

def read_csv_string(input_):
    """
    Takes a string which is the contents of a CSV file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_string functions.
    """
    # Remove excess whitespace
    input_ = input_.strip()

    # Split by line and save the first line (the column headers)
    split_data = input_.split("\n")
    col_headers = split_data[0].strip().split(",")

    # For each line (not including the first line because it is the headers), strip the
    #   data and split it by commas into a list. Add this list to an overall list holding the data.
    rows = []
    for i in range(1, len(split_data)):
        curr_row = split_data[i].strip().split(",")
        rows.append(curr_row)
    

    # Convert the list of column headers and row information into a dictionary and return the dictionary.
    data = {}
    data["columns"] = col_headers
    data["rows"] = rows

    return data


def write_csv_string(data):
    """
    Takes a data object (created by one of the read_*_string functions).
    Returns a string in the CSV format.
    """
    # Get a list of column headers and rows
    col_headers = data["columns"]
    rows = data["rows"]

    # Combine elements in the col_headers list into a string
    column_header_final = ""
    for item in col_headers:
        column_header_final = column_header_final + item + ','
    column_header_final = column_header_final[:-1] + "\n"
    
    # Combine each element in each row into a string
    row_data = ""
    for item in rows:
        seperator = ","
        row_data = row_data + seperator.join(item) + "\n"

    return column_header_final + row_data


def read_json_string(input_):
    """
    Similar to read_csv_string, except works for JSON files.
    """
    # Load the input_ string as a json object
    json_ = json.loads(input_)
    col_headers = []
    rows = []

    # Iterate through the json object and save the data of each row into an overall list
    for item in json_:
        col_headers = list(item.keys())
        curr_row = list(item.values())
        rows.append(curr_row)

    # Create and return dictionary
    data = {}
    data["columns"] = col_headers
    data["rows"] = rows

    return data


def write_json_string(data):
    """
    Writes JSON strings. Similar to write_csv_string.
    """
    col_headers = data["columns"]
    rows = data["rows"]

    data = []

    # Create a dictionary with col_headers and current_row (each row in rows)
    for current_row in rows:
        test = dict(zip(col_headers, current_row))
        data.append(test)

    # return the list of dictionaries as a string
    return json.dumps(data)


def read_xml_string(input_):
    """
    You should know the drill by now...
    """
    # remove <data> and </data> tags
    input_ = input_.strip()
    input_ = input_[6:-7]

    # Split by < (start of new tag) and remove the first item (will be a <record> tag)
    input_ = input_.split("<")
    input_.pop(0)

    col_headers = []
    rows = []
    row_data = []

    # check each item in input_ (a list)
    for item in input_:
        # If the starting item is / it is an end tag and should not be counted/used
        if item[0] != "/":
            # Split the item by > (this will give a list of the tag name followed by the data)
            header_and_data = item.split(">")
            
            # Replace escape characters with appropriate characters and append the information to the over all row_data list
            if header_and_data[0] != "record":
                header_and_data[1] = header_and_data[1].replace("&gt;", ">")
                header_and_data[1] = header_and_data[1].replace("&lt;", "<")
                header_and_data[1] = header_and_data[1].replace("&apos;", "'")
                header_and_data[1] = header_and_data[1].replace("&quot;", '"')
                header_and_data[1] = header_and_data[1].replace("&amp;", "&")
                row_data.append(header_and_data[1])

            # Append the header to the list of column_headers
            if header_and_data[0] != "record" and header_and_data[0] not in col_headers:
                col_headers.append(header_and_data[0])
        
        elif item == "/record>":
            rows.append(row_data)
            row_data = []

    data = {}
    data["columns"] = col_headers
    data["rows"] = rows

    return data


def write_xml_string(data):
    """
    Writes XML strings. Similar to write_csv_string or write_json_string.
    """
    final_data = ["<data>"]
    col_headers = data["columns"]
    rows = data["rows"]

    # Combine the list of column headers and rows in alternating fasion (i.e.: [col1, row_data_1, col2, row_data_2])
    all_results = []
    for current_row in rows:
        result = [None]*(len(col_headers)+len(current_row))
        result[::2] = col_headers
        result[1::2] = current_row

        all_results.append(result)

    for result in all_results:
        # Add starting <record> tag and then replace characters with escape characters and fill in column header tag and data
        final_data.append("<record>")
        for i in range(0, len(all_results[0]), 2 ):
            tag_name = result[i]
            tag_data = result[i+1]
            tag_data = tag_data.replace("&", "&amp;")
            tag_data = tag_data.replace("<", "&lt;")
            tag_data = tag_data.replace(">", "&gt;")
            tag_data = tag_data.replace("'", "&apos;")
            tag_data = tag_data.replace('"', "&quot;")

            first = f"<{tag_name}>"
            second = f"{tag_data}"
            third = f"</{tag_name}>"
            combined = first+second+third

            final_data.append( combined )
        final_data.append("</record>")
    final_data.append("</data>")
    
    # concatenate final_data (list) into a string and return it
    return "".join(final_data)
