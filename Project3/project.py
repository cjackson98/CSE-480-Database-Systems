"""
Name: Chris Jackson
Time To Completion: 12(ish?) hours
Comments: Adapted from the Project 2 solution provided on Mimir along with
    my own Project 2 solution

Sources: Project 2 solution provided on Mimir
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}



def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)

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
    while True:
        if query[end_quote_index+1] == "'":
            end_quote_index = query.find("'", end_quote_index+2)
        else:
            break

    text = query[:end_quote_index]
    text = text.replace("''", "'")
    tokens.append(text)
    query = query[end_quote_index+1:]
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

        if query[0] in "(),;*.<>=!":
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
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def get_output_columns(tokens):
            output_columns = []
            while True:
                if not tokens:
                    break
                col = tokens.pop(0)

                if len(tokens) >= 1:
                    next_char = tokens[0]
                
                if next_char == ".":
                    tokens.pop(0)
                    col = tokens.pop(0)
                    output_columns.append(col)
                    continue
                elif next_char == "FROM":
                    output_columns.append(col)
                    tokens.pop(0)
                    break
                elif col == "FROM":
                    break
                elif next_char == ",":
                    tokens.pop(0)
                    output_columns.append(col)
            return output_columns, tokens

        def where(tokens):
            pop_and_check(tokens, "WHERE")
            condition = []
            while True:
                if not tokens:
                    break

                curr_char = tokens[0]
                if len(tokens) <= 1:
                    next_char = None
                else:
                    next_char = tokens[1]

                if next_char == ".":
                    tokens.pop(0)
                    tokens.pop(0)
                    condition.append(tokens.pop(0))
                    continue
                elif next_char == "ORDER":
                    condition.append(tokens.pop(0))
                elif curr_char == "ORDER":
                    break
                else:
                    condition.append(tokens.pop(0))
            return tokens, condition

        def join(tokens):

            distinct = False
            if("DISTINCT" in tokens):
                indx = tokens.find("DISTINCT")
                tokens = tokens[:indx] + tokens[indx+1:]
                distinct = True

            output_columns = []
            while True:
                if not tokens:
                    break

                col = tokens.pop(0)
                
                if col == "LEFT":
                    break
                elif col == ",":
                    col = tokens.pop(0)
                elif col == "FROM":
                    break
                
                if not tokens:
                    break

                next_token = tokens.pop(0)

                if next_token == "LEFT":
                    break
                elif next_token == ".":
                    col = col + next_token + tokens.pop(0)
                elif next_token == ",":
                    tokens.pop(0)

                output_columns.append(col)

            output_columns_dict = dict()
            for x in output_columns:
                if "." in x:
                    index = x.find(".")
                    table = x[:index]
                    col = x[index+1:]
                    if table in list(output_columns_dict.keys()):
                        output_columns_dict[table].append(col)
                    else:
                        output_columns_dict[table] = [col]

            left_table = tokens.pop(0)

            pop_and_check(tokens, "LEFT")
            pop_and_check(tokens, "OUTER")
            pop_and_check(tokens, "JOIN")

            right_table = tokens.pop(0)

            pop_and_check(tokens, "ON")

            left_on_condition = tokens.pop(0)
            if tokens[0] == ".":
                left_on_condition = left_on_condition + tokens.pop(0) + tokens.pop(0)

            operator = tokens.pop(0)

            right_on_condition = tokens.pop(0)
            if tokens[0] == ".":
                right_on_condition = right_on_condition + tokens.pop(0) + tokens.pop(0)

            on_condition = [left_on_condition, operator, right_on_condition]

            joined_rows = self.database.join(output_columns_dict, left_table, right_table, on_condition)

            condition = True

            order_by_columns = dict()
            if len(tokens) > 0:
                if tokens[0] == "WHERE":
                    tokens, condition = where(tokens)   

                    lvalue = condition[0]
                    rvalue = condition[-1]
                    condition = condition[1:-1]
                    condition = " ".join(condition)
                    condition = [lvalue, condition, rvalue]
            
            if len(tokens) > 0:
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")

                while True:
                    table_name = tokens.pop(0)
                    tokens.pop(0)
                    col = tokens.pop(0)

                    if table_name in list(order_by_columns.keys()):
                        order_by_columns[table_name].append(col)
                    else:
                        order_by_columns[table_name] = [col]
                    
                    if len(tokens) > 0:
                        tokens.pop(0)
                    else:
                        break
            
            updated_output_columns = []
            for value in output_columns_dict.values():
                updated_output_columns.extend(value)

            updated_order_by_columns = []
            for value in order_by_columns.values():
                updated_order_by_columns.extend(value)

            # Distinct
            final_data = self.database.select(updated_output_columns, False, updated_order_by_columns, condition)
            
            if not distinct:
                return final_data
            else:
                distinct_data = []
                for x in final_data:
                    if x not in distinct_data:
                        distinct_data.append(x)
                return distinct_data


        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            
            insert_order = None
            if tokens[0] == "(":
                pop_and_check(tokens, "(")
                insert_order = []
                while True:
                    col = tokens.pop(0)
                    if col == ",":
                        continue
                    if col == ")":
                        break
                    insert_order.append(col)

            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")

            all_rows = []

            while True:
                row_contents = []
                while True:
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                
                all_rows.append(row_contents)
                
                if not tokens:
                    break
                
                next_char = tokens.pop(0)
                
                if next_char == ",":
                    pop_and_check(tokens, "(")
                    continue


            if insert_order is not None:
                normal_order = self.database.tables[table_name].column_names
                for row in range(len(all_rows)):
                    new_list = [None]*len(normal_order)
                    curr_row = all_rows[row]
                    
                    for x in range(len(insert_order)):
                        indx = normal_order.index(insert_order[x])
                        new_list[indx] = curr_row[x]

                    all_rows[row] = new_list
            
            for row in all_rows:
                self.database.insert_into(table_name, row)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")

            if "JOIN" in tokens:
                result = join(tokens)
                return result

            distinct = False
            if(tokens[0] == "DISTINCT"):
                distinct = True

            output_columns, tokens = get_output_columns(tokens)

            table_name = tokens.pop(0)
            if len(tokens) > 0:
                next_char = tokens[0]

            if len(tokens) > 0:
                next_char = tokens[0]

            lvalue = 1
            rvalue = 1
            condition = True
            if next_char == "WHERE":
                tokens, condition = where(tokens)   

                lvalue = condition[0]
                rvalue = condition[-1]
                condition = condition[1:-1]
                condition = " ".join(condition)
                condition = [lvalue, condition, rvalue]
                            
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            
            order_by_columns = []
            while True:
                if not tokens:
                    break

                col = tokens.pop(0)

                if len(tokens) >= 1:
                    next_char = tokens[0]
                else:
                    next_char = None

                if next_char is None:
                    order_by_columns.append(col)
                    break
                elif next_char == ".":
                    tokens.pop(0)
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if len(tokens) > 0:
                        tokens.pop(0)
                        continue
                    else:
                        break
                elif next_char == ",":
                    tokens.pop(0)
                    order_by_columns.append(col)
                    continue

            final_data = self.database.select(
                output_columns, table_name, order_by_columns, condition)
            
            if not distinct:
                return final_data
            else:
                distinct_data = []
                for x in final_data:
                    if x not in distinct_data:
                        distinct_data.append(x)
                return distinct_data

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            
            if not tokens:
                self.database.delete_all(table_name)
            else:
                tokens, condition = where(tokens)
                lvalue = condition[0]
                rvalue = condition[-1]
                condition = condition[1:-1]
                condition = " ".join(condition)
                condition = [lvalue, condition, rvalue]

                rows_to_delete = list(self.database.select(None, table_name, None, condition))
                self.database.delete_rows(table_name, rows_to_delete)

        def update(tokens):

            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            update_cols = []
            pop_and_check(tokens, "SET")

            while True:
                if not tokens:
                    break
                if tokens[0] == "WHERE":
                    break
                
                col = tokens.pop(0)
                operator = tokens.pop(0)
                value = tokens.pop(0)
                
                condition = []
                condition.append(col)
                condition.append(operator)
                condition.append(value)

                update_cols.append(condition)

                if not tokens:
                    break
                if tokens[0] == ",":
                    tokens.pop(0)
                else:
                    break

            condition = True
            if tokens:
                if tokens[0] == "WHERE":
                    tokens, condition = where(tokens)

            rows_to_update = self.database.select(None, table_name, None, condition)

            for update in update_cols:
                self.database.update(table_name, rows_to_update, update)

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass



class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.joined = None

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns, condition):
        if table_name is False:
            table = self.joined
        else:
            assert table_name in self.tables
            table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns, condition)

    def join(self, output_columns, left_table_name, right_table_name, condition):
        assert left_table_name in self.tables
        assert right_table_name in self.tables

        tables = list(output_columns.keys())
        for table_name in tables:
            assert table_name in self.tables

        left_table = self.tables[left_table_name]
        right_table = self.tables[right_table_name]

        joined_table = left_table.join(right_table, condition)

        self.joined = joined_table

        return self.joined.rows
        
    def delete_all(self, table_name):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete_all_rows()

    def delete_rows(self, table_name, rows_to_delete):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete_rows(rows_to_delete)

    def update(self, table_name, rows_to_update, update):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(rows_to_update, update)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns, condition):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            # Make a copy of self.rows and replace all None with "" for comparisons
            #     then sort the list and return the None values
            all_rows = self.rows
            for row in all_rows:
                for key in row:
                    if row[key] is None:
                        row[key] = ""
            sorted_all_rows = sorted(all_rows, key=itemgetter(*order_by_columns))
            for row in sorted_all_rows:
                for key in row:
                    if row[key] == "":
                        row[key] = None

            return sorted_all_rows

        def generate_tuples(rows, output_columns, condition):
            for row in rows:
                if type(condition) == list:
                    lvalue = row[condition[0]]
                    rvalue = condition[2]
                    
                    if rvalue is None:
                        if condition[1].lower() == "is":
                            if lvalue is rvalue:
                                yield tuple(row[col] for col in output_columns)
                        elif condition[1].lower() == "is not":
                            if lvalue is not rvalue:
                                yield tuple(row[col] for col in output_columns)
                    else:
                        if lvalue is not None:
                            if condition[1] == "=":
                                if lvalue == rvalue:
                                    yield tuple(row[col] for col in output_columns)
                            if condition[1] == "! =":
                                if lvalue != rvalue:
                                    yield tuple(row[col] for col in output_columns)
                            if condition[1] == "<":
                                if lvalue < rvalue:
                                    yield tuple(row[col] for col in output_columns)
                            if condition[1] == ">":
                                if lvalue > rvalue:
                                    yield tuple(row[col] for col in output_columns)
                else:
                    yield tuple(row[col] for col in output_columns)

        def generate_tuples_for_delete(rows, condition):
            if type(condition) == list:

                rvalue = condition[2]
                for row in rows:
                    lvalue = row[condition[0]]
                    if rvalue is None:
                        if condition[1].lower() == "is":
                            if lvalue is rvalue:
                                yield row
                        elif condition[1].lower() == "is not":
                            if lvalue is not rvalue:
                                yield row
                    else:
                        if lvalue is not None:
                            if condition[1] == "=":
                                if lvalue == rvalue:
                                    yield row
                            if condition[1] == "! =":
                                if lvalue != rvalue:
                                    yield row
                            if condition[1] == "<":
                                if lvalue < rvalue:
                                    yield row
                            if condition[1] == ">":
                                if lvalue > rvalue:
                                    yield row
            else:
                for row in self.rows:
                    yield row

        if output_columns:
            expanded_output_columns = expand_star_column(output_columns)
            check_columns_exist(expanded_output_columns)
            if order_by_columns:
                check_columns_exist(order_by_columns)
                sorted_rows = sort_rows(order_by_columns)
            else:
                sorted_rows = self.rows
            return generate_tuples(sorted_rows, expanded_output_columns, condition)
        else:
            return list(generate_tuples_for_delete(self.rows, condition))

    def delete_all_rows(self):
        self.rows = []

    def delete_rows(self, rows_to_delete):
        updated_rows = []
        for row in self.rows:
            if row not in rows_to_delete:
                updated_rows.append(row)
        self.rows = updated_rows

    def update(self, rows_to_update, update):
        all_rows = []
        for row in self.rows:
            if row in rows_to_update:
                update_row = row
                update_row[update[0]] = update[2]
                all_rows.append(update_row)
            else:
                all_rows.append(row)
            
        self.rows = all_rows

    def join(self, right_table, condition):

        def combine(x, y):
            for key, value in y.items():
                x[key] = value
            return x

        left_condition = condition[0]
        operation = condition[1]
        right_condition = condition[2]

        if "." in left_condition:
            index = left_condition.find(".")
            col = left_condition[index+1:]
        left_condition = col

        if "." in right_condition:
            index = right_condition.find(".")
            col = right_condition[index+1:]
        right_condition = col

        joined_rows = []

        for x in range(len(self.rows)):
            for y in right_table.rows:
                lvalue = self.rows[x][left_condition]
                rvalue = y[right_condition]
                
                if rvalue is None:
                    if operation.lower() == "is":
                        if lvalue is rvalue:
                            if y not in joined_rows:
                                joined_rows.append(combine(self.rows[x], y))
                    elif operation.lower() == "is not":
                        if lvalue is not rvalue:
                            if y not in joined_rows:
                                joined_rows.append(combine(self.rows[x], y))
                else:
                    if lvalue is not None:
                        if operation == "=":
                            if lvalue == rvalue:
                                if y not in joined_rows:
                                    joined_rows.append(combine(self.rows[x], y))
                            if operation == "! =":
                                if lvalue != rvalue:
                                    if y not in joined_rows:
                                        joined_rows.append(combine(self.rows[x], y))
                            if operation == "<":
                                if lvalue < rvalue:
                                    if y not in joined_rows:
                                        joined_rows.append(combine(self.rows[x], y))
                            if operation == ">":
                                if lvalue > rvalue:
                                    if y not in joined_rows:
                                        joined_rows.append(combine(self.rows[x], y))
        for x in self.rows:
            if x not in joined_rows:
                joined_rows.append(x)
        
        right_col_names = list(right_table.column_names)
        left_col_names = list(self.column_names)

        right_types = list(right_table.column_types)
        left_types = list(self.column_types)

        joined_col_names = right_col_names + left_col_names
        joined_types = right_types + left_types

        column_name_type_pairs = []
        for x in range(len(joined_types)):
            column_name_type_pairs.append((joined_col_names[x], joined_types[x]))

        joined_table = Table(None, column_name_type_pairs)
        joined_table.rows = joined_rows

        all_rows = joined_table.rows
        for x in range(len(all_rows)):
            for col in joined_col_names:
                if col not in joined_table.rows[x].keys():
                    joined_table.rows[x][col] = None
        return joined_table
