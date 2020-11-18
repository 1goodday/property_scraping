import mysql.connector


def create_table(db_user, db_pw, db_host, db_name, table_name, table_fields, table_types):
    '''Creates a table with fields and types given as tuples.'''

    lst = ""
    for f, t in zip(table_fields, table_types):
        lst += f + " " + t + ", "
    lst = lst[:-2]

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS " +
        table_name + " (" +
        lst + ")"
    )

    cnx.commit()
    cursor.close()
    cnx.close()


def fetch_column_from_db(db_user, db_pw, db_host, db_name, table_name, table_field):
    '''Fetches a field (a table column) from MySQL DB and returns them in a Python list type.'''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()
    cursor.execute('SELECT ' + table_field + ' FROM ' +
                   db_name + '.' + table_name, ())

    field_list_in_db = []
    for f in cursor:
        field_list_in_db.append(f[0])

    cursor.close()
    cnx.close()

    return field_list_in_db


def fetch_row_from_db(db_user, db_pw, db_host, db_name, table_name, table_field, field_value):
    '''Fetches a row with condition table_field = table_value from MySQL DB and returns it in a Python list type.
    It's assumed that table_field=table_value represents a unique row in the DB.
    '''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()
    cursor.execute('SELECT * FROM ' + db_name + '.' +
                            table_name + " WHERE " + table_field + " = " + field_value, ())

    row_list_in_db = []
    for f in cursor:
        row_list_in_db += f

    cursor.close()
    cnx.close()

    return row_list_in_db


def clean_db_keep_updated(db_user, db_pw, db_host, db_name, table_name, rmv_dup_field, rmv_dup_decide_field):
    '''If a rmv_dup_field has a duplicate value, the older rows, based on lower rmv_dup_decide_field values, will be deleted and the most recent row with the highest rmv_dup_decide_field remains.'''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    cursor.execute(
        "DELETE t1 FROM " +
        table_name +
        " t1 INNER JOIN " +
        table_name +
        " t2 WHERE " +
        "t1." + rmv_dup_field + " = " + "t2." + rmv_dup_field +
        " AND " +
        "(t1." + rmv_dup_decide_field + " < " + "t2." + rmv_dup_decide_field +
        " OR " +
        "(t1." + rmv_dup_decide_field + " IS NULL AND " +
        "t2." + rmv_dup_decide_field + " IS NOT NULL))"
    )

    cnx.commit()
    cursor.close()
    cnx.close()


def clean_db_delete_null(db_user, db_pw, db_host, db_name, table_name, null_field):
    '''Deletes rows with null_field equals NULL from the table'''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    cursor.execute(
        "DELETE FROM " +
        table_name +
        " WHERE " +
        null_field +
        " IS NULL"
    )

    cnx.commit()
    cursor.close()
    cnx.close()


def clean_db_remove_duplicate_rows(db_user, db_pw, db_host, db_name, table_name):
    '''Deletes duplicate rows from the table.'''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    try:
        cursor.execute("DROP TABLE " + table_name + "_temp")
    except:
        pass

    cursor.execute("CREATE TABLE " + table_name + "_temp LIKE " + table_name)
    cursor.execute("INSERT INTO " + table_name +
                   "_temp (SELECT DISTINCT * FROM " + table_name + ")")
    cursor.execute("DELETE FROM " + table_name)
    cursor.execute("INSERT INTO " + table_name +
                   " (SELECT * FROM " + table_name + "_temp)")

    cursor.execute("DROP TABLE " + table_name + "_temp")

    cnx.commit()
    cursor.close()
    cnx.close()


def read_column_names(db_user, db_pw, db_host, db_name, table_name):
    '''Reads column names (field names) into a tuple of strings.'''
    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = '" +
        table_name + "' AND table_schema = '" + db_name + "'", ())

    column_names = ()
    for c in cursor:
        column_names = column_names.__add__((c[0],))

    cursor.close()
    cnx.close()

    return column_names


def write_rows_to_db(db_user, db_pw, db_host, db_name, table_name, rows):
    '''Writes a list of tuples (each tuple presents a row) to the table.'''

    cnx = mysql.connector.connect(user=db_user, password=db_pw,
                                  host=db_host,
                                  database=db_name)

    cursor = cnx.cursor()

    no_of_columns = len(rows[0])
    written_lines, not_written_lines = 0, 0

    table_fields = read_column_names(
        db_user, db_pw, db_host, db_name, table_name)
    table_fields_str = ''
    for tf in table_fields:
        table_fields_str += (tf + ', ')
    table_fields_str = '(' + table_fields_str[:-2] + ')'

    for row in rows:
        try:
            cursor.execute(
                "INSERT INTO " + db_name + "." + table_name + " " +
                table_fields_str +
                " VALUES (" + (no_of_columns * "%s, ")[:-2] + ")",
                row)
            written_lines += 1

        except:
            cursor.execute(
                "INSERT INTO " + db_name + "." + table_name + " " +
                table_fields_str +
                " VALUES (" + (no_of_columns * "%s, ")[:-2] + ")",
                (row[0],) + (no_of_columns - 1) * (None,))
            not_written_lines += 1

    print("\nOf total %i lines, %i lines were written completely and %i lines were written with only URL (because of any write error) to the DB.\n" % (
        len(rows), written_lines, not_written_lines))

    cnx.commit()
    cursor.close()
    cnx.close()