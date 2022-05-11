

def get_metadata():
    # Reading tables.yaml, extracting tables to be split

    import yaml

    path = "dags/conf"
    tables_file = f"{path}/tables.yaml"
    tables_split_file = f"{path}/tables_split.yaml"
    split_row_count = 500000
    default_split_parts = 10

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

    tables_with_metadata = {}
    for group, table_names in parsed_yaml.items():
        if (group.endswith("_split")):
            for table_name in table_names:
                sql_db_table = sql_table_names.get(table_name)
                if (sql_db_table is not None):
                    sql_db_table_split = sql_db_table.split(".")
                    sql_db = sql_db_table_split[0]
                    sql_table = sql_db_table_split[1]
                    tables_with_metadata[table_name] = {"sql_db": sql_db, "sql_table": sql_table}

    print(tables_with_metadata)

    # SQL connection setup

    import MySQLdb

    connection = MySQLdb.connect(
            **{
                "port": 3306,
                "user": user,
                "passwd": password,
                "host": host,
                "db": db
            }
        )

    # Select data

    for table, metadata in tables_with_metadata.items():
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
        result = cursor.fetchall()
        cursor.close()
        for row in result:
            print(f"count = {int(row[0])}, size = {float(row[1])} MB")

        tables_with_metadata[table]["count"] = int(row[0])
        tables_with_metadata[table]["size_mb"] = float(row[1])

    # Writing values into tables_counts.yaml

    with open(tables_split_file, "w") as file:
        documents = yaml.dump(tables_with_metadata, file, indent = 4, sort_keys = False)

if __name__ == '__main__':
    get_metadata()