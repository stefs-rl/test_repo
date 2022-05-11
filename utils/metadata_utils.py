import os.path
import yaml
import MySQLdb


def get_tables(tables_file):
    # Reading tables.yaml, extracting tables to be split
    sql_table_names = {
        "backend.accounts_payment": "shapeupclub.accounts_payment",
        "backend.dietsettings": "shapeupclub.diary_dietsetting",
        "backend.food_food": "shapeupclub.tblfood",
        "backend.food_mealitem": "shapeupclub.tblmealitem",
        "backend.tbluser": "shapeupclub.tbluser",
    }

    with open(tables_file, "r") as file:
        try:
            parsed_yaml = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    tables = {}
    for group, table_names in parsed_yaml.items():
        if (group.endswith("_split")):
            for table_name in table_names:
                sql_db_table = sql_table_names.get(table_name)
                if (sql_db_table is not None):
                    sql_db_table_split = sql_db_table.split(".")
                    sql_db = sql_db_table_split[0]
                    sql_table = sql_db_table_split[1]
                    tables[table_name] = {"sql_db": sql_db, "sql_table": sql_table}

    print(tables)
    return tables


def get_metadata(tables):
    # SQL connection setup
    connection = MySQLdb.connect(
            **{
                "port": 3306,
                "user": os.environ.get("MYSQL_USER", "user"),
                "passwd": os.environ.get("MYSQL_PASSWD", "password"),
                "host": os.environ.get("MYSQL_HOST", "host"),
                "db": os.environ.get("MYSQL_DB", "db"),
                "ssl": {"ssl_mode": "DISABLED"}
            }
        )

    # Select data
    for table, metadata in tables.items():
        print(f"""Getting metadata for {metadata.get("sql_db")}.{metadata.get("sql_table")} table.""")

        sql = f"""
        SELECT count, size_mb
        FROM (
            SELECT COUNT(*) AS count
            FROM {metadata.get("sql_db")}.{metadata.get("sql_table")}
        ) AS counts
        JOIN (
            SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
            FROM information_schema.TABLES
            WHERE table_schema = "{metadata.get("sql_db")}"
            AND table_name = "{metadata.get("sql_table")}"
        ) AS sizes
        """

        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        print(f"count = {int(result[0])}, size = {float(result[1])} MB")

        tables[table]["count"] = int(result[0])
        tables[table]["size_mb"] = float(result[1])

    connection.close()

    print(tables)
    return tables


def write_metadata(tables_split_file, metadata):
    # Writing values into tables_counts.yaml
    with open(tables_split_file, "w") as file:
        documents = yaml.dump(metadata, file, indent = 4, sort_keys = False)


if __name__ == '__main__':
    path = "dags/conf"
    tf = f"{path}/tables.yaml"
    tsf = f"{path}/tables_split.yaml"

    if os.path.isfile("env/env.py"):
        print("Env file does exist, pulling the environment variables locally.")
        from env.env import init_env
        init_env()
    else:
        print("Env file does not exist, pulling the environment variables from server.")

    tbls = get_tables(tf)
    mtd = get_metadata(tbls)
    #write_metadata(tsf, mtd)
