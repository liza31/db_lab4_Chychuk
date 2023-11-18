from typing import Any
from collections.abc import Sequence, Mapping

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from itertools import chain
from inspect import cleandoc


# PostgreSQL Connection config

DB_HOST = 'localhost'   # PostgreSQL Database server host name
DB_PORT = '5432'        # PostgreSQL Database server port number
DB_NAME = 'db_lab3'     # PostgreSQL Database name
DB_USER = 'postgres'   # PostgreSQL Database user name
DB_PASS = '1234'      # PostgreSQL Database user password


# Database queries 1-3 according to the laboratory task

# -- Query 1: info string & sql query

QUERY_1_INFO = cleandoc(
    '''
    The number of attacks in which "Kalibr" cruise missiles were used by month
    Кількість атак з використанням крилатих ракет "Калібр" за місяцями
    '''
)

QUERY_1_SQL = cleandoc(
    '''
    SELECT
        date_part('year', attacks.start_datatime)::integer AS year,
        date_part('month', attacks.start_datatime)::integer AS month,
        count(DISTINCT attacks.attack_id) AS attacks_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_missiles USING (group_id)
    WHERE
        group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Kalibr')
    GROUP BY
        year, month
    ORDER BY
        year, month
    '''
)

# -- Query 2: info string & sql query

QUERY_2_INFO = cleandoc(
    '''
    Distribution of the total number of attacks by targets
    Розподіл загальної кількості атак за цілями
    '''
)

QUERY_2_SQL = cleandoc(
    '''
    SELECT
        potential_targets.general_name as target,
        count(DISTINCT attack_id) as attacks_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_targets USING (group_id)
        JOIN potential_targets USING (target_id)
    GROUP BY
        target
    ORDER BY
        attacks_count desc
    '''
)

# -- Query 3: info string & sql query

QUERY_3_INFO = cleandoc(
    '''
    Mass (total number of used missiles/drones) of attacks in which "Shahed-136/131" strike drones were used by month
    Масовість атак з використанням баражуючих боєприпасів "Shahed-136/131" за місяцями
    '''
)

QUERY_3_SQL = cleandoc(
    '''
    SELECT
        date_part('year', attacks.start_datatime)::integer AS year,
        date_part('month', attacks.start_datatime)::integer AS month,
        sum(attack_groups.units_launched) AS missiles_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_missiles USING (group_id)
    WHERE
        group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Shahed-136/131')
    GROUP BY
        year, month
    ORDER BY
        year, month
    '''
)


# Auxiliary functions definitions

def tabulate(
        rows: Sequence[Mapping],
        col_keys: Sequence = None,
        head_row: bool = True,
        col_heads: Mapping[Any, str] = None) -> str:
    """
    Tabulates table, represented by :class:`Sequence` of rows represented columnKey-cellValue :class:`Mapping` objects.

    :param rows: :class:`Sequence` of table rows represented by columnKey-cellValue :class:`Mapping` objects
    :param col_keys: keys of columns to display
    :param head_row: whether to display headings row
    :param col_heads: columnKey-columnHeading :class:`Mapping`

    :return: tabulated table :class:`str`
    """

    # Handle input parameters
    col_keys = col_keys or next(iter(rows)).keys()
    col_heads = (col_heads or {col_key: str(col_key) for col_key in col_keys}) if head_row else None

    # Build columns widths map
    col_widths = {
        col_key: max(
            max(len(str(row[col_key])) for row in rows),
            len(col_heads[col_key]) if head_row else 0
        ) + 1
        for col_key in col_keys
    }

    # Build rows strings iterator
    rows_str = (
        '│' + '│'.join(str(row[col_key]).ljust(col_widths[col_key]) for col_key in col_keys) + '│'
        for row in (chain((col_heads,), rows) if head_row else rows)
    )

    # Build table string
    table_str = '\n'.join(
        chain([next(rows_str), '├' + '┼'.join('─' * col_widths[col_key] for col_key in col_keys) + '┤'], rows_str)
        if head_row else rows_str
    )

    return table_str


# Script `main()` function & entry point

def main(conn: psycopg2.extensions.connection):

    queries = [[QUERY_1_SQL, QUERY_1_INFO], [QUERY_2_SQL, QUERY_2_INFO], [QUERY_3_SQL, QUERY_3_INFO]]

    for query_sql, query_info in queries:

        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query_sql)
                query_res: Sequence[Mapping] = cur.fetchall()

        print(query_info, end='\n\n')
        print(tabulate(query_res), end='\n\n\n')


if __name__ == '__main__':

    # Try to open connection and execute script logic
    try:

        # Open database connection
        _conn: psycopg2.extensions.connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        # Execute script logic
        try:
            main(conn=_conn)

        # Properly close the database connection
        finally:
            _conn.close()

    # Print error message if any error occurred
    except psycopg2.Error as _err:
        print(_err)
