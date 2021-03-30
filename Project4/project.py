"""
Name: Chris Jackson
Time To Completion: 6ish hours
Comments:

Sources:
    Project 3 solution posted on Mimir. 
    
    Also asked a few questions on Piazza and received answers/help from TA Ikechukwu Uchendu.

    Briefly looked at https://www.programiz.com/python-programming/methods/built-in/hash to get the general idea of how to use hash()
        (as suggested by TA Ikechukwu Uchendu)
    
    HW 8 helped me understand locks a little better as well (not sure if it's necessary to include this as a source or not).
"""
import copy
import string
from operator import itemgetter
from collections import namedtuple
import itertools

_ALL_DATABASES = {}

# LOCKS FORMAT/LAYOUT:
#       hash(conn_1) : (lockType, test.db)
#           conn_1 : "s" : "test.db"
#           conn_2 : "s" : "test.db"
#       locks can be upgraded:
#           conn_1 : "r" : "test.db"
#           conn_2 : "s" : "test.db"
_LOCKS = {}

WhereClause = namedtuple("WhereClause", ["col_name", "operator", "constant"])
UpdateClause = namedtuple("UpdateClause", ["col_name", "constant"])
FromJoinClause = namedtuple("FromJoinClause", ["left_table_name",
                                               "right_table_name",
                                               "left_join_col_name",
                                               "right_join_col_name"])




####################
## Connection class
####################
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
        
        self.filename = filename
        self.open_transaction = None
        self.transaction_database = None
        self.transaction_number = None
        self.mode = "D"
        self.didWrite = False
        self.autocommit = True

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        # Get latest copy of database before executing statement
        self.database = _ALL_DATABASES[self.filename]

        def get_and_check_locks(currentKey, desired_lock, db):
            """
            Checks currently owned locks/acquires any needed locks

            currentKey: hash(self) - used to as a key in _LOCKS
            desired_lock: Lock that the connection is trying to get ('e'=exclusive, 'r'=reserved, 's'=shared)
            db: the database the connection is trying to get a lock on (i.e., "test.db")
            """

            # List of relationships between locks (i.e., shared locks block exclusive locks)
            blocks = dict()
            blocks['s'] = ['e']
            blocks['r'] = ['r', 'e']
            blocks['e'] = ['s', 'r', 'e']

            # Get a list of locks currently on the database (use to check for blocks. i.e. exclusive blocks all other)
            db_locks = []
            for key, value in _LOCKS.items():
                if value[1] == db and key != currentKey:
                    db_locks.append(value[0])

            # For each lock on the database, check if it blocks our desired lock. If so, raise  exception.
            for lock in db_locks:
                if desired_lock in blocks[lock]:
                    raise Exception("BLOCKED")

            # Update _LOCKS with the appropriate lock if needed
            if currentKey in _LOCKS.keys():
                lock = _LOCKS[currentKey]
                if desired_lock == 'r':
                    if lock[0] == 's':
                        _LOCKS[currentKey] = (desired_lock, db)
                elif desired_lock == 'e':
                    if lock[0] != 'e':
                        _LOCKS[currentKey] = (desired_lock, db)
            else:
                _LOCKS[currentKey] = (desired_lock, db)

        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            check_exists = False

            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")

            next_token = tokens.pop(0)
            if next_token == "IF":
                pop_and_check(tokens, "NOT")
                pop_and_check(tokens, "EXISTS")
                check_exists = True
                next_token = tokens.pop(0)

            table_name = next_token
            pop_and_check(tokens, "(")
            column_name_type_pairs = []


            if self.open_transaction:
                if check_exists:
                    if table_name in self.transaction_database.tables:
                        return
                if table_name in self.transaction_database.tables:
                    raise Exception("Table already exists")
            else:
                if check_exists:
                    if table_name in self.database.tables:
                        return
                if table_name in self.database.tables:
                    raise Exception("Table already exists")
            
            while True:
                column_name = tokens.pop(0)
                qual_col_name = QualifiedColumnName(column_name, table_name)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((qual_col_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            if self.open_transaction:
                self.transaction_database.create_new_table(table_name, column_name_type_pairs)
            else:
                self.database.create_new_table(table_name, column_name_type_pairs)

        def drop(tokens):
            check_exists = False

            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")

            next_token = tokens.pop(0)
            if next_token == "IF":
                pop_and_check(tokens, "EXISTS")
                check_exists = True
                next_token = tokens.pop(0)

            table_name = next_token

            if self.open_transaction:
                if check_exists:
                    if table_name in self.transaction_database.tables:
                        self.transaction_database.tables.pop(table_name)
                        return
                    else:
                        return
                if table_name in self.transaction_database.tables:
                    self.transaction_database.tables.pop(table_name)
                    return
                else:
                    raise Exception("Table does not exist")
            else:
                if check_exists:
                    if table_name in self.database.tables:
                        self.database.tables.pop(table_name)
                        return
                    else:
                        return
                if table_name in self.database.tables:
                    self.database.tables.pop(table_name)
                    return
                else:
                    raise Exception("Table does not exist")

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            # A reserved lock is needed to write (INSERT, UPDATE, and DELETE). 
            if self.open_transaction:
                self.didWrite = True
            get_and_check_locks(hash(self), 'r', self.filename)

            def get_comma_seperated_contents(tokens):
                contents = []
                pop_and_check(tokens, "(")
                while True:
                    item = tokens.pop(0)
                    contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        return contents
                    assert comma_or_close == ',', comma_or_close

            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            if tokens[0] == "(":
                col_names = get_comma_seperated_contents(tokens)
                qual_col_names = [QualifiedColumnName(col_name, table_name)
                                  for col_name in col_names]
            else:
                qual_col_names = None
            pop_and_check(tokens, "VALUES")
            while tokens:
                row_contents = get_comma_seperated_contents(tokens)
                if qual_col_names:
                    assert len(row_contents) == len(qual_col_names)
                if self.open_transaction:
                    self.transaction_database.insert_into(table_name,
                                            row_contents,
                                            qual_col_names=qual_col_names)
                else:
                    self.database.insert_into(table_name,
                                            row_contents,
                                            qual_col_names=qual_col_names)
                if tokens:
                    pop_and_check(tokens, ",")

            if self.autocommit:
                _LOCKS.pop(hash(self))

        def get_qualified_column_name(tokens):
            """
            Returns comsumes tokens to  generate tuples to create
            a QualifiedColumnName.
            """
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)

        def update(tokens):
            # A reserved lock is needed to write (INSERT, UPDATE, and DELETE).
            if self.open_transaction:
                self.didWrite = True
            get_and_check_locks(hash(self), 'r', self.filename)

            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            update_clauses = []
            while tokens:
                qual_name = get_qualified_column_name(tokens)
                if not qual_name.table_name:
                    qual_name.table_name = table_name
                pop_and_check(tokens, '=')
                constant = tokens.pop(0)
                update_clause = UpdateClause(qual_name, constant)
                update_clauses.append(update_clause)
                if tokens:
                    if tokens[0] == ',':
                        tokens.pop(0)
                        continue
                    elif tokens[0] == "WHERE":
                        break

            where_clause = get_where_clause(tokens, table_name)

            if self.open_transaction:
                self.transaction_database.update(table_name, update_clauses, where_clause)
            else:
                self.database.update(table_name, update_clauses, where_clause)

            if self.autocommit:
                _LOCKS.pop(hash(self))

        def delete(tokens):
            # A reserved lock is needed to write (INSERT, UPDATE, and DELETE).
            if self.open_transaction:
                self.didWrite = True
            get_and_check_locks(hash(self), 'r', self.filename)

            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            where_clause = get_where_clause(tokens, table_name)
            if self.open_transaction:
                self.transaction_database.delete(table_name, where_clause)
            else:
                self.database.delete(table_name, where_clause)
            
            if self.autocommit:
                _LOCKS.pop(hash(self))

        def get_where_clause(tokens, table_name):
            if not tokens or tokens[0] != "WHERE":
                return None
            tokens.pop(0)
            qual_col_name = get_qualified_column_name(tokens)
            if not qual_col_name.table_name:
                qual_col_name.table_name = table_name
            operators = {">", "<", "=", "!=", "IS"}
            found_operator = tokens.pop(0)
            assert found_operator in operators
            if tokens[0] == "NOT":
                tokens.pop(0)
                found_operator += " NOT"
            constant = tokens.pop(0)
            if constant is None:
                assert found_operator in {"IS", "IS NOT"}
            if found_operator in {"IS", "IS NOT"}:
                assert constant is None
            return WhereClause(qual_col_name, found_operator, constant)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            # A shared lock is needed to read (SELECT).
            get_and_check_locks(hash(self), "s", self.filename)

            def get_from_join_clause(tokens):
                left_table_name = tokens.pop(0)
                if tokens[0] != "LEFT":
                    return FromJoinClause(left_table_name, None, None, None)
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                right_table_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                left_col_name = get_qualified_column_name(tokens)
                pop_and_check(tokens, "=")
                right_col_name = get_qualified_column_name(tokens)
                return FromJoinClause(left_table_name,
                                      right_table_name,
                                      left_col_name,
                                      right_col_name)

            pop_and_check(tokens, "SELECT")

            is_distinct = tokens[0] == "DISTINCT"
            if is_distinct:
                tokens.pop(0)

            output_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                output_columns.append(qual_col_name)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            # FROM or JOIN
            from_join_clause = get_from_join_clause(tokens)
            table_name = from_join_clause.left_table_name

            # WHERE
            where_clause = get_where_clause(tokens, table_name)

            # ORDER BY
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                order_by_columns.append(qual_col_name)
                if not tokens:
                    break
                pop_and_check(tokens, ",")

            if self.open_transaction:
                if self.autocommit:
                    _LOCKS.pop(hash(self))
                return self.transaction_database.select(
                    output_columns,
                    order_by_columns,
                    from_join_clause=from_join_clause,
                    where_clause=where_clause,
                    is_distinct=is_distinct)
            else:
                if self.autocommit:
                    _LOCKS.pop(hash(self))
                return self.database.select(
                    output_columns,
                    order_by_columns,
                    from_join_clause=from_join_clause,
                    where_clause=where_clause,
                    is_distinct=is_distinct)

        def begin_transaction(tokens):
            """
            Called when a transaction begins

            Sets the transaction mode:
                DEFERRED = locks are acquired when needed by a statement in the transaction
                IMMEDIATE = a reserved lock is acquired at start of the transaction and an exclusive lock is acquired when needed
                EXCLUSIVE = an exclusive lock is acquired at start of transaction

            Creates a copy of the database (self.transaction_database) and statements will modify the copy until commit/rollback
            """
            if self.open_transaction:
                raise Exception("There is already an open transaction for this connection.")
            else:
                assert hash(self) not in _LOCKS.keys()

                pop_and_check(tokens, "BEGIN")
                self.autocommit = False


                next_token = tokens.pop(0)
                if next_token == "DEFERRED":
                    self.mode = "d"
                    pop_and_check(tokens, "TRANSACTION")
                
                elif next_token == "IMMEDIATE":
                    self.mode = "i"
                    pop_and_check(tokens, "TRANSACTION")
                    get_and_check_locks(hash(self), 'r', self.filename)
                
                elif next_token == "EXCLUSIVE":
                    self.mode = "e"
                    pop_and_check(tokens, "TRANSACTION")
                    get_and_check_locks(hash(self), 'e', self.filename)
                
                elif next_token == "TRANSACTION":
                    self.mode = "d"
                
                else:
                    raise Exception(f"Invalid mode '{next_token}' provided.")
                
                self.open_transaction = True
                self.transaction_database = copy.deepcopy(self.database)
                
        def commit_transaction(tokens):
            """
            Commits the open transaction

            "An exclusive lock is needed to commit a write. Specifically, if the transaction has a reserved lock, it must be
                promoted to an exclusive lock upon commit. Exclusive locks block all other locks."

            "Locks are released upon commit."

            Transaction database is copied over the database (changes are saved) and is then cleared and autocommit is re-enabled
            """
            if not self.open_transaction:
                raise Exception("There are no open transactions to commit for this connection.")
            else:
                if self.open_transaction and self.didWrite:
                    get_and_check_locks(hash(self), 'e', self.filename)
                
                if hash(self) in _LOCKS.keys():
                    _LOCKS.pop(hash(self))

                self.open_transaction = False
                self.database = self.transaction_database
                _ALL_DATABASES[self.filename] = self.transaction_database
                self.transaction_database = None
                self.autocommit = True

        def rollback(tokens):
            """
            Undoes any changes to the database made during the transaction (clears the transaction_database copy)
            """
            if not self.open_transaction:
                raise Exception("There are no open transactions to rollback for this connection.")
            else:
                self.open_transaction = False
                self.transaction_database = None
                self.autocommit = True

                if hash(self) in _LOCKS.keys():
                    _LOCKS.pop(hash(self))

        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "DROP":
            drop(tokens)
            return []
        elif tokens[0] == "BEGIN":
            begin_transaction(tokens)
            return []
        elif tokens[0] == "COMMIT":
            commit_transaction(tokens)
            return []
        elif tokens[0] == "ROLLBACK":
            rollback(tokens)
            return []
        else:
            raise AssertionError(
                "Unexpected first word in statements: " + tokens[0])

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass
####################
## Connection class
####################




####################
## Connect function
####################
def connect(filename, timeout=None, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)
####################
## Connect function
####################




##############################
## Qualified Column class
##############################
class QualifiedColumnName:

    def __init__(self, col_name, table_name=None):
        self.col_name = col_name
        self.table_name = table_name

    def __str__(self):
        return "QualifiedName({}.{})".format(
            self.table_name, self.col_name)

    def __eq__(self, other):
        same_col = self.col_name == other.col_name
        if not same_col:
            return False
        both_have_tables = (self.table_name is not None and
                            other.col_name is not None)
        if not both_have_tables:
            return True
        return self.table_name == other.table_name

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.col_name, self.table_name))

    def __repr__(self):
        return str(self)
##############################
## Qualified Column class
##############################




####################
## Database class
####################
class Database:

    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents, qual_col_names=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, qual_col_names=qual_col_names)
        return []

    def update(self, table_name, update_clauses, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(update_clauses, where_clause)

    def delete(self, table_name, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete(where_clause)

    def select(self, output_columns, order_by_columns,
               from_join_clause,
               where_clause=None, is_distinct=False):
        assert from_join_clause.left_table_name in self.tables
        if from_join_clause.right_table_name:
            assert from_join_clause.right_table_name in self.tables
            left_table = self.tables[from_join_clause.left_table_name]
            right_table = self.tables[from_join_clause.right_table_name]
            all_columns = itertools.chain(
                zip(left_table.column_names, left_table.column_types),
                zip(right_table.column_names, right_table.column_types))
            left_col = from_join_clause.left_join_col_name
            right_col = from_join_clause.right_join_col_name
            join_table = Table("", all_columns)
            combined_rows = []
            for left_row in left_table.rows:
                left_value = left_row[left_col]
                found_match = False
                for right_row in right_table.rows:
                    right_value = right_row[right_col]
                    if left_value is None:
                        break
                    if right_value is None:
                        continue
                    if left_row[left_col] == right_row[right_col]:
                        new_row = dict(left_row)
                        new_row.update(right_row)
                        combined_rows.append(new_row)
                        found_match = True
                        continue
                if left_value is None or not found_match:
                    new_row = dict(left_row)
                    new_row.update(zip(right_row.keys(),
                                       itertools.repeat(None)))
                    combined_rows.append(new_row)

            join_table.rows = combined_rows
            table = join_table
        else:
            table = self.tables[from_join_clause.left_table_name]
        return table.select_rows(output_columns, order_by_columns,
                                 where_clause=where_clause,
                                 is_distinct=is_distinct)
####################
## Database class
####################




####################
## Table class
####################
class Table:

    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents, qual_col_names=None):
        if not qual_col_names:
            qual_col_names = self.column_names
        assert len(qual_col_names) == len(row_contents)
        row = dict(zip(qual_col_names, row_contents))
        for null_default_col in set(self.column_names) - set(qual_col_names):
            row[null_default_col] = None
        self.rows.append(row)

    def update(self, update_clauses, where_clause):
        for row in self.rows:
            if self._row_match_where(row, where_clause):
                for update_clause in update_clauses:
                    row[update_clause.col_name] = update_clause.constant

    def delete(self, where_clause):
        self.rows = [row for row in self.rows
                     if not self._row_match_where(row, where_clause)]

    def _row_match_where(self, row, where_clause):
        if not where_clause:
            return True
        new_rows = []
        value = row[where_clause.col_name]

        op = where_clause.operator
        cons = where_clause.constant
        if ((op == "IS NOT" and (value is not cons)) or
                (op == "IS" and value is cons)):
            return True

        if value is None:
            return False

        if ((op == ">" and value > cons) or
            (op == "<" and value < cons) or
            (op == "=" and value == cons) or
                (op == "!=" and value != cons)):
            return True
        return False

    def select_rows(self, output_columns, order_by_columns,
                    where_clause=None, is_distinct=False):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col.col_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names
                       for col in columns)

        def ensure_fully_qualified(columns):
            for col in columns:
                if col.table_name is None:
                    col.table_name = self.name

        def sort_rows(rows, order_by_columns):
            return sorted(rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        def remove_duplicates(tuples):
            seen = set()
            uniques = []
            for row in tuples:
                if row in seen:
                    continue
                seen.add(row)
                uniques.append(row)
            return uniques

        expanded_output_columns = expand_star_column(output_columns)

        check_columns_exist(expanded_output_columns)
        ensure_fully_qualified(expanded_output_columns)
        check_columns_exist(order_by_columns)
        ensure_fully_qualified(order_by_columns)

        filtered_rows = [row for row in self.rows
                         if self._row_match_where(row, where_clause)]
        sorted_rows = sort_rows(filtered_rows, order_by_columns)

        list_of_tuples = generate_tuples(sorted_rows, expanded_output_columns)
        if is_distinct:
            return remove_duplicates(list_of_tuples)
        return list_of_tuples
####################
## Table class
####################




####################
## Tokenize query
####################
def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


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
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    if (query[0] == "'"):
        delimiter = "'"
    else:
        delimiter = '"'
    query = query[1:]
    end_quote_index = query.find(delimiter)
    while query[end_quote_index + 1] == delimiter:
        # Remove Escaped Quote
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index = query.find(delimiter, end_quote_index + 1)
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
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

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] in "(),;*.><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in {"'", '"'}:
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError(
                "Query didn't get shorter. query = {}".format(query))

    return tokens
####################
## Tokenize query
####################


############################################################################################################################################
## INFO ON LOCK FUNCTIONALITY:
############################################################################################################################################
"""
A shared lock is needed to read (SELECT). Shared locks block exclusive locks.

A reserved lock is needed to write (INSERT, UPDATE, and DELETE). Reserved locks block exclusive locks and other reserved locks.

An exclusive lock is needed to commit a write. Specifically, if the transaction has a reserved lock, it must be promoted to an exclusive
  lock upon commit. Exclusive locks block all other locks.

Locks are released upon commit.

If a lock can't be granted when requested, raise an exception.

Remember: sqlite only locks the entire database, not individual tables or rows.

Shared    blocks exclusive
Reserved  blocks exclusive and other reserved
Exclusive blocks all others
"""
############################################################################################################################################
## INFO ON LOCK FUNCTIONALITY:
############################################################################################################################################


