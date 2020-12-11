import json
import sqlite3
from itertools import islice
from sqlite3 import Error


def keyword_id_generator():
    keyword_id_generator.counter += 1
    return keyword_id_generator.counter


def fos_id_generator():
    fos_id_generator.counter += 1
    return fos_id_generator.counter


keyword_id_generator.counter = 0
fos_id_generator.counter = 0


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


def sql_insert_many(conn, sql, items):
    cur = conn.cursor()
    cur.executemany(sql, items)
    conn.commit()
    return cur.lastrowid


def insert_publication(conn, publications):
    """
    Insert new entries into the publications table
    :param conn: Connection object
    :param publications: List of publication tuples (id, title, ...)
    :return: last row id
    """
    sql = ''' INSERT INTO publications(id, title, year, n_citation, n_author, n_reference, abstract_length, page_start,
                                       page_end, doc_type, venue, lang, publisher, volume, issue, issn, isbn)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    return sql_insert_many(conn, sql, publications)


def insert_author(conn, authors):
    """
    Insert new entries into the authors table
    :param conn: Connection object
    :param authors: List of author tuples (id, name, org)
    :return: last row id
    """
    sql = ''' INSERT OR IGNORE INTO authors(author_id,name,org)
              VALUES(?,?,?) '''
    return sql_insert_many(conn, sql, authors)


def insert_publication_author(conn, pub_authors):
    """
        Insert new entries into the publication_author table
        :param conn: Connection object
        :param pub_authors: List of (publication_id, author_id) tuples
        :return: last row id
    """
    sql = ''' INSERT INTO publication_author(pub_id,aut_id)
                  VALUES(?,?) '''
    return sql_insert_many(conn, sql, pub_authors)


def insert_publication_reference(conn, pub_references):
    """
        Insert new entries into the publication_reference table
        :param conn: Connection object
        :param pub_references: List of (publication_id, publication_id) tuples
        :return: last row id
    """
    sql = ''' INSERT INTO publication_reference(pub_id,ref_id)
                  VALUES(?,?) '''
    return sql_insert_many(conn, sql, pub_references)


def insert_abstract(conn, abstracts):
    """
        Insert new entries into the abstracts table
        :param conn: Connection object
        :param abstracts: List of abstract tuples
        :return: last row id
    """
    sql = ''' INSERT INTO abstracts(abstract, indexed_abstract, pub_id)
                      VALUES(?,?,?) '''
    return sql_insert_many(conn, sql, abstracts)


def insert_venue(conn, venues):
    """
        Insert new entries into the venues table
        :param conn: Connection object
        :param venues: List of (venue_id, name, type) tuples
        :return: last row id
    """
    sql = ''' INSERT OR IGNORE INTO venues(venue_id, name, type)
                          VALUES(?,?,?) '''
    return sql_insert_many(conn, sql, venues)


def insert_publication_venue(conn, pub_venues):
    """
        Insert new entries into the publication_venue table
        :param conn: Connection object
        :param pub_venues: List of (pub_id, ven_id) tuples
        :return: last row id
    """
    sql = ''' INSERT INTO publication_venue(pub_id, ven_id)
                              VALUES(?,?) '''
    return sql_insert_many(conn, sql, pub_venues)


def insert_fields_of_study(conn, fos):
    """
        Insert new entries into the fos table
        :param conn: Connection object
        :param fos: List of (fos_id, name) tuples
        :return: last row id
    """
    sql = ''' INSERT OR IGNORE INTO fos(fos_id, name)
                          VALUES(?,?) '''
    return sql_insert_many(conn, sql, fos)


def insert_publication_fos(conn, pub_fos):
    """
        Insert new entries into the publication_fos table
        :param conn: Connection object
        :param pub_fos: List of (pub_id, fos_id) tuples
        :return: last row id
    """
    sql = ''' INSERT OR IGNORE INTO publication_fos(pub_id, fos_id)
                              VALUES(?,?) '''
    return sql_insert_many(conn, sql, pub_fos)


def insert_keywords(conn, keywords):
    """
        Insert new entries into the keywords table
        :param conn: Connection object
        :param keywords: List of (fos_id, name) tuples
        :return: last row id
    """
    sql = ''' INSERT OR IGNORE INTO keywords(kw_id, keyword)
                          VALUES(?,?) '''
    return sql_insert_many(conn, sql, keywords)


def insert_publication_keyword(conn, pub_keyword):
    """
        Insert new entries into the publication_keyword table
        :param conn: Connection object
        :param pub_keyword: List of (pub_id, fos_id) tuples
        :return: last row id
    """
    sql = ''' INSERT INTO publication_keyword(pub_id, kw_id)
                              VALUES(?,?) '''
    return sql_insert_many(conn, sql, pub_keyword)


def get_abstract_length(abstract, indexed_abstract):
    if abstract is not None:
        return len(abstract)
    elif indexed_abstract is not None:
        if indexed_abstract.get('IndexLength') > 0:
            abstract_length = 0
            inv_idx = indexed_abstract.get('InvertedIndex')
            for key in inv_idx:
                abstract_length += (len(inv_idx.get(key)) + 1) * len(key)
            return abstract_length - 1
        else:
            return 0
    else:
        return None


def get_n_author(authors):
    if authors is not None:
        return len(authors)
    else:
        return None


def get_n_reference(references):
    if references is not None:
        return len(references)
    else:
        return None


def main():
    database = r"D:\Repos\DataScience\db\test.db"

    sql_create_publications_table = """
        CREATE TABLE IF NOT EXISTS publications (
            id integer PRIMARY KEY,
            title text,
            year integer,
            n_citation integer,
            n_author integer,
            n_reference integer,
            abstract_length integer,
            page_start text,
            page_end text,
            doc_type text,
            venue text,
            lang text,
            publisher text,
            volume text,
            issue text,
            issn text,
            isbn text
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

    sql_create_keywords_table = """
            CREATE TABLE IF NOT EXISTS keywords (
                kw_id integer PRIMARY KEY,
                keyword text
            );
    """

    sql_create_publication_keyword_table = """
        CREATE TABLE IF NOT EXISTS publication_keyword (
            pub_id integer NOT NULL,
            kw_id integer NOT NULL,
            FOREIGN KEY (pub_id) REFERENCES publications (id),
            FOREIGN KEY (kw_id) REFERENCES keywords (kw_id),
            PRIMARY KEY (pub_id, kw_id)
        );
    """

    sql_create_venues_table = """
        CREATE TABLE IF NOT EXISTS venues (
            venue_id integer PRIMARY KEY,
            name text,
            type text
        )
    """

    sql_create_publication_venue_table = """
        CREATE TABLE IF NOT EXISTS publication_venue (
            pub_id integer NOT NULL,
            ven_id integer NOT NULL,
            FOREIGN KEY (pub_id) REFERENCES publications (id),
            FOREIGN KEY (ven_id) REFERENCES venues (venue_id),
            PRIMARY KEY (pub_id, ven_id)
        );
    """

    sql_create_fos_table = """
        CREATE TABLE IF NOT EXISTS fos (
            fos_id integer PRIMARY KEY,
            name text
        )
    """

    sql_create_publication_fos_table = """
        CREATE TABLE IF NOT EXISTS publication_fos (
            pub_id integer NOT NULL,
            fos_id integer NOT NULL,
            FOREIGN KEY (pub_id) REFERENCES publications (id),
            FOREIGN KEY (fos_id) REFERENCES fos (fos_id),
            PRIMARY KEY (pub_id, fos_id)
        );
    """

    sql_create_abstracts_table = """
        CREATE TABLE IF NOT EXISTS abstracts (
            abstract text,
            indexed_abstract text,
            pub_id integer,
            FOREIGN KEY(pub_id) REFERENCES publications (id)
        )
    """

    # open data file
    path = r'D:\Repos\DataScience\dblp.v12\dblp.v12.json'
    enc = 'utf-8'
    data_file = open(path, 'r', encoding=enc)

    # create a database connection
    conn = create_connection(database)

    # dictionaries for keywords and fos
    keyword_id_dict = {}
    fos_id_dict = {}

    # create tables
    if conn is not None:
        # create publications table
        create_table(conn, sql_create_publications_table)

        # create authors table
        create_table(conn, sql_create_authors_table)

        # create keywords table
        create_table(conn, sql_create_keywords_table)

        # create venues table
        # create_table(conn, sql_create_venues_table)

        # create fields of study table
        create_table(conn, sql_create_fos_table)

        # create abstracts table
        create_table(conn, sql_create_abstracts_table)

        # create relations tables
        create_table(conn, sql_create_publication_author_table)  # publication <-> author
        create_table(conn, sql_create_publication_reference_table)  # publication <-> publication
        create_table(conn, sql_create_publication_keyword_table)  # publication <-> keyword
        # create_table(conn, sql_create_publication_venue_table)  # publication <-> venue
        create_table(conn, sql_create_publication_fos_table)  # publication <-> fos

    else:
        print("Error! cannot create the database connection.")

    with conn and data_file:
        while True:
            next_n_lines = list(islice(data_file, 100000))
            if not next_n_lines:
                break

            publications = []
            authors = []
            keywords = []
            fields_of_study = []
            # venues = []
            abstracts = []

            pub_authors = []
            pub_references = []
            pub_keywords = []
            pub_fos = []
            # pub_venues = []

            for line in next_n_lines:
                if not line:
                    break
                if line[0] == '[' or line[0] == ']':
                    continue
                if line[0] == ',':
                    line = line[1:]

                pub = json.loads(line)
                aut = pub.get('authors')
                ref = pub.get('references')
                kw = pub.get('keywords')
                fos = pub.get('fos')
                # venue = pub.get('venue')
                abstr = pub.get('abstract')
                idx_abstr = pub.get('indexed_abstract')

                if pub.get('venue') is not None:
                    venue = pub.get('venue').get('raw')
                else:
                    venue = None

                publications.append(
                    (pub.get('id'), pub.get('title'), pub.get('year'), pub.get('n_citation'), get_n_author(aut),
                     get_n_reference(ref), get_abstract_length(abstr, idx_abstr), pub.get('page_start'),
                     pub.get('page_end'), pub.get('doc_type'), pub.get('lang'), venue, pub.get('publisher'),
                     pub.get('volume'), pub.get('issue'), pub.get('issn'), pub.get('isbn'))
                )

                abstracts.append(
                    (abstr, json.dumps(idx_abstr), pub.get('id'))
                )

                # if venue is not None:
                #     venues.append(
                #         (venue.get('id'), venue.get('raw'), venue.get('type'))
                #     )
                #     pub_venues.append(
                #         (pub.get('id'), venue.get('id'))
                #     )

                if aut is not None:
                    for item in aut:
                        authors.append(
                            (item.get('id'), item.get('name'), item.get('org'))
                        )
                        pub_authors.append(
                            (pub.get('id'), item.get('id'))
                        )

                if ref is not None:
                    for item in ref:
                        pub_references.append(
                            (pub.get('id'), int(item))
                        )

                if kw is not None:
                    pass

                if fos is not None:
                    for item in fos:
                        name = item.get('name')
                        if name not in fos_id_dict:
                            fos_id = fos_id_generator()
                            fos_id_dict[name] = fos_id
                            fields_of_study.append(
                                (fos_id, name)
                            )

                        pub_fos.append(
                            (pub.get('id'), fos_id_dict[name])
                        )

                if kw is not None:
                    for keyword in kw:
                        if keyword not in keyword_id_dict:
                            kw_id = keyword_id_generator()
                            keyword_id_dict[keyword] = kw_id
                            keywords.append(
                                (kw_id, keyword)
                            )

                        pub_keywords.append(
                            (pub.get('id'), keyword_id_dict[keyword])
                        )

            insert_publication(conn, publications)
            insert_author(conn, authors)
            insert_publication_author(conn, pub_authors)
            insert_publication_reference(conn, pub_references)
            insert_abstract(conn, abstracts)
            # insert_venue(conn, venues)
            # insert_publication_venue(conn, pub_venues)
            insert_fields_of_study(conn, fields_of_study)
            insert_publication_fos(conn, pub_fos)
            insert_keywords(conn, keywords)
            insert_publication_keyword(conn, pub_keywords)


if __name__ == '__main__':
    main()
