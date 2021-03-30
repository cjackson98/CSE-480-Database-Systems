"""
Name: Chris Jackson
Time To Completion: 3-4 hours
Comments:

Sources: 
    Used https://docs.python.org/3/howto/sorting.html to help
        with sorting (specifically how to sort a list of tuples by an index in each tuple)
    
    Used https://www.geeksforgeeks.org/python-key-index-in-dictionary/ to remind
        myself how to get an index of a dictionary given a key

    Used https://docs.python.org/3.3/library/functions.html#zip to remind
        myself how to convert a list into a dictionary
        
    Used the lecture notes video "Tokenizing SQL statements" to tokenize the queries
"""

import string

_ALL_DATABASES = {} # Dictionary that stores all the data


"""
FUNCTIONS USED TO TOKENIZE QUERY
ADAPTED FROM Week03 LECTURE NOTE VIDEO TITLED: "Tokenizing SQL statements"
"""
def collect_characters(query, allowed_characters):
    letters = []
    
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query, string.ascii_letters + "_" + string.digits)
    
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"    
    query = query[1:]
    end_quote_index = query.find("'")
    if query[end_quote_index+1] == "'":
        end_quote_index = query.find("'", end_quote_index+2)
    text = query[0:end_quote_index]
    text = text.replace("''", "'")
    tokens.append(text)
    query = query[end_quote_index + 1:]
    
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits + "-")
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []

    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in (string.digits + "-"):
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter")

    return tokens
"""
FUNCTIONS USED TO TOKENIZE QUERY
ADAPTED FROM Week03 LECTURE NOTE VIDEO TITLED: "Tokenizing SQL statements"
"""


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.name = filename
        self.database = Database()

    def execute(self, query):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokenized_query = tokenize(query)

        # Remove "," from query
        if "," in tokenized_query:
            tokenized_query.remove(",")

        if " ".join(tokenized_query[0:2]) == "CREATE TABLE":
            # Remove "," from query
            if "," in tokenized_query:
                tokenized_query.remove(",")

            # Remove "CREATE TABLE"
            tokenized_query = tokenized_query[2:]

            # Get (and then remove) name from tokenized_query
            name = tokenized_query[0]
            tokenized_query = tokenized_query[2:-2]

            # Combine the information into a dictionary
            schema = dict(zip(tokenized_query[::2], tokenized_query[1::2]))
            
            # Add a table and update tokenized_query
            self.database.add_table(name, schema)
            tokenized_query = []

        elif " ".join(tokenized_query[0:2]) == "INSERT INTO":
            # Remove any "," as they won't be used
            if "," in tokenized_query:
                tokenized_query.remove(",")

            # Remove "INSERT INTO"
            tokenized_query = tokenized_query[2:]

            # Get table name
            name = tokenized_query.pop(0)

            # Remove the table name, parenthesis and the semicolon
            tokenized_query = tokenized_query[2:-2]

            # add a row and update tokenized_query
            self.database.tables[name].add_row(tokenized_query)
            tokenized_query = []
        
        else:
            # Remove any "," as they won't be used
            if "," in tokenized_query:
                tokenized_query.remove(",")
            
            # Remove "SELECT"
            tokenized_query.pop(0)

            # Get name(s) of columns (or if * is used set this to None and it will be updated later)
            col_names = []
            if tokenized_query[0] == "*":
                tokenized_query.pop(0)
                col_names = None
            else:
                col_names.append(tokenized_query.pop(0))

                while True:
                    if tokenized_query[0] != "FROM":
                        col_names.append(tokenized_query.pop(0))
                    else:
                        break
            
            # Remove "FROM"
            tokenized_query.pop(0)

            # Get table name
            table_name = tokenized_query.pop(0)
            if col_names == None:
                col_names = list(self.database.tables[table_name].schema.keys())

            # Remove "ORDER BY" and ";"
            tokenized_query = tokenized_query[2:-1]

            # Get name of columns to order by
            order_by = []
            for x in tokenized_query:
                order_by.append(x)
            tokenized_query = []
            
            # Get index of columns to sort by
            order_by_index = []
            for x in order_by:
                order_by_index.append( col_names.index(x) )

            # Get index of the column names
            col_headers = list(self.database.tables[table_name].schema.keys())

            col_names_index = []
            for x in col_names:
                col_names_index.append( col_headers.index(x) )

            # Fill a list called "all_data" with the requested data
            all_data = []
            for row in self.database.tables[table_name].rows:
                all_data.append( row.get_data(col_names_index) )

            # Reverse the order of the list (if multiple "ORDER BY" arguments are passed,
            #   this will sort by the least important first. For example if given
            #   "ORDER BY num1, num2" it will sort by num2 before sorting by num1)
            order_by_index.reverse()

            # Sort the data and return it
            for index in order_by_index:
                all_data = sorted(all_data, key=lambda all_data: all_data[index])

            return all_data

        _ALL_DATABASES[self.name] = self.database
        return tokenized_query
    
    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


class Database(object):
    """
    Holds 0 or more Table objects.
    """
    def __init__(self):
        self.tables = {} # dictionary in the form name: table
    
    
    def add_table(self, name, schema):
        """
        Creates a new Table object and adds it to self.tables
        """
        newTable = Table(name, schema)
        self.tables[name] = newTable


class Table(object):
    """
    Takes in a name (str) and a schema (dict) and
    holds 0 or more Row objects.
    """
    def __init__(self, name, schema):
        self.name = name        # Name of table
        self.rows = []          # List of Row objects
        self.schema = schema    # Column names/types
    
    def add_row(self, data):
        """
        Creates a new Row object and adds it to self.rows
        """
        newRow = Row(data)
        self.rows.append(newRow)


class Row(object):
    """
    Holds the data for each row
    """
    def __init__(self, data):
        self.data = tuple(data)


    def get_data(self, indexes):
        """
        Takes in a list of indexes and retrieves the requested data
        at each index before returning it as a tuple
        """
        requested_data = []
        for index in indexes:
            requested_data.append(self.data[index])
        
        return tuple(requested_data)


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)
