import json
import sqlite3
from itertools import islice
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def insert_publication(conn, publications):
    """
    Insert a new publication into the publications table
    :param conn: Connection object
    :param publication:
    :return: publication id
    """
    sql = ''' INSERT INTO publications(id,title,year,n_citation,
                                       page_start,page_end,
                                       doc_type,lang,publisher,
                                       volume,issue)
              VALUES(?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, publications)
    conn.commit()
    return cur.lastrowid


def insert_author(conn, authors):
    """
    Insert a new author into the authors table
    :param conn: Connection object
    :param author:
    :return: author id
    """

    sql = ''' INSERT OR IGNORE INTO authors(author_id,name,org)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, authors)
    conn.commit()
    return cur.lastrowid


def insert_publication_author(conn, pub_authors):
    """
        Insert a new entry into the publication_author table
        :param conn: Connection object
        :param publication_id:
        :param author_id:
        :return: publication author id
        """
    sql = ''' INSERT INTO publication_author(pub_id,aut_id)
                  VALUES(?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, pub_authors)
    conn.commit()
    return cur.lastrowid


def main():
    database = r"D:\Repos\DataScience\db\test.db"

    sql_create_publications_table = """
        CREATE TABLE IF NOT EXISTS publications (
            id integer PRIMARY KEY,
            title text,
            year integer,
            n_citation integer,
            page_start text,
            page_end text,
            doc_type text,
            lang text,
            publisher text,
            volume text,
            issue text
        );
    """

    sql_create_authors_table = """
        CREATE TABLE IF NOT EXISTS authors (
            author_id integer PRIMARY KEY,
            name text NOT NULL,
            org text
        );
    """

    sql_create_publication_author_table = """
        CREATE TABLE IF NOT EXISTS publication_author (
            pub_id integer NOT NULL,
            aut_id integer NOT NULL,
            FOREIGN KEY (pub_id) REFERENCES publications (id),
            FOREIGN KEY (aut_id) REFERENCES authors (author_id),
            PRIMARY KEY (pub_id, aut_id)
        );
    """

    sql_create_publication_reference_table = """
        CREATE TABLE IF NOT EXISTS publication_reference (
            pub_id integer NOT NULL,
            ref_id integer NOT NULL,
            FOREIGN KEY (pub_id) REFERENCES publications (id),
            FOREIGN KEY (ref_id) REFERENCES publications (id),
            PRIMARY KEY (pub_id, ref_id)
        );
    """

    # open data file
    path = 'D:\Repos\DataScience\dblp.v12\dblp.v12.json'
    enc = 'utf-8'
    data_file = open(path, 'r', encoding=enc)

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create publications table
        create_table(conn, sql_create_publications_table)

        # create authors table
        create_table(conn, sql_create_authors_table)

        # create publication author table
        create_table(conn, sql_create_publication_author_table)

        # create publication reference table
        # create_table(conn, sql_create_publication_reference_table)
    else:
        print("Error! cannot create the database connection.")

    with conn and data_file:
        while True:
            next_n_lines = list(islice(data_file, 1000))
            if not next_n_lines:
                break

            publications = []
            authors = []
            pub_authors = []

            for line in next_n_lines:
                if line[0] == '[' or line[0] == ']':
                    continue
                if line[0] == ',':
                    line = line[1:]

                json_dict = json.loads(line)
                publications.append(
                    (json_dict.get('id'), json_dict.get('title'), json_dict.get('year'), json_dict.get('n_citation'),
                     json_dict.get('page_start'), json_dict.get('page_end'), json_dict.get('doc_type'),
                     json_dict.get('lang'), json_dict.get('publisher'), json_dict.get('volume'), json_dict.get('issue'))
                )

                aut = json_dict.get('authors')
                for item in aut:
                    authors.append(
                        (item.get('id'), item.get('name'), item.get('org'))
                    )
                    pub_authors.append(
                        (json_dict.get('id'), item.get('id'))
                    )

            insert_publication(conn, publications)
            insert_author(conn, authors)
            insert_publication_author(conn, pub_authors)
            break


if __name__ == '__main__':
    main()
