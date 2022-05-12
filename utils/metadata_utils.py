from math import ceil
import MySQLdb
import os.path
import yaml


def filter_dict_by_list(_dict, _list):
    intersection = sorted(set(_dict) & set(_list))
    return {x: _dict[x] for x in intersection}


# Reading tables.yaml, and metadata.yaml extracting tables to be split
def get_tables_metadata(tables_file, metadata_file):
    # Reading tables.yaml file
    with open(tables_file, "r") as file:
        try:
            tables_yaml = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    # Reading metadata.yaml file
    with open(metadata_file, "r") as file:
        try:
            metadata_yaml = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)

    # Extracting matched values
    tables_metadata = {}
    for metadata_table_group, metadata_tables in metadata_yaml.items():
        # Matching group
        if metadata_table_group in tables_yaml:
            # Matching items (tables)
            tables_metadata[metadata_table_group] =\
                filter_dict_by_list(metadata_tables, tables_yaml[metadata_table_group])

    return tables_metadata


def get_sql_metadata(metadata):
    # SQL connection setup
    connection = MySQLdb.connect(
            **{
                "port": 3306,
                "user": os.environ.get("MYSQL_USER", "user"),
                "passwd": os.environ.get("MYSQL_PASSWD", "password"),
                "host": os.environ.get("MYSQL_HOST", "host"),
                "db": os.environ.get("MYSQL_DB", "shapeupclub"),
                "ssl": False
            }
        )

    # Select data
    for metadata_table_group, metadata_tables in metadata.items():
        for metadata_table in metadata_tables:
            sql_db = metadata_tables[metadata_table].get("sql_db")
            sql_table = metadata_tables[metadata_table].get("sql_table")
            print(f"""Getting SQL metadata for {sql_db}.{sql_table} table.""")

            sql = f"""
            SELECT count, size_mb
            FROM (
                SELECT COUNT(*) AS count
                FROM {sql_db}.{sql_table}
            ) AS counts
            JOIN (
                SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                FROM information_schema.TABLES
                WHERE table_schema = "{sql_db}"
                AND table_name = "{sql_table}"
            ) AS sizes
            """

            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            cursor.close()
            print(f"count = {int(result[0])}, size = {float(result[1])} MB")

            metadata[metadata_table_group][metadata_table]["count"] = int(result[0])
            metadata[metadata_table_group][metadata_table]["size_mb"] = float(result[1])

    connection.close()

    return metadata


def enrich_metadata(metadata):
    # Calculate desired split sizes
    # TODO: Extract into YAML file?
    split_data = {
        "backend_split": {
            "measured_throughput_mb_m": 160,
            "target_split_run_time_m": 30,
            "default_split": 5
        }
    }

    for metadata_table_group, metadata_tables in metadata.items():
        # Matching group
        if metadata_table_group in split_data:
            for metadata_table in metadata_tables:
                # Calculating the split size based on the measurements and current table size
                sql_db = metadata_tables[metadata_table].get("sql_db")
                sql_table = metadata_tables[metadata_table].get("sql_table")
                size_mb = metadata_tables[metadata_table].get("size_mb")
                throughput = split_data[metadata_table_group].get("measured_throughput_mb_m")
                target_run_time = split_data[metadata_table_group].get("target_split_run_time_m")
                default_split = split_data[metadata_table_group].get("default_split")
                split = max(default_split, int(ceil(size_mb / throughput / target_run_time)))

                print(f"""Split size for {sql_db}.{sql_table} ({size_mb} MB) = {split} (default {default_split}).""")

                metadata[metadata_table_group][metadata_table]["default_split"] = default_split
                metadata[metadata_table_group][metadata_table]["split"] = split

    return metadata


def write_tables_metadata(tables_metadata_file, metadata):
    # Writing values into tables_counts.yaml
    with open(tables_metadata_file, "w") as file:
        documents = yaml.dump(metadata, file, indent = 4, sort_keys = False)


if __name__ == '__main__':
    path = "dags/conf"
    tf = f"{path}/tables.yaml"
    mf = f"{path}/metadata.yaml"
    tmf = f"{path}/tables_metadata.yaml"

    if os.path.isfile("env/env.py"):
        print("Env file does exist, pulling the environment variables locally.")
        from env.env import init_env
        init_env()
    else:
        print("Env file does not exist, pulling the environment variables from server.")

    mtd = get_tables_metadata(tf, mf)
    mtd = get_sql_metadata(mtd)
    mtd = enrich_metadata(mtd)
    write_tables_metadata(tmf, mtd)
