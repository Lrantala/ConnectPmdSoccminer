import logging
import argparse
import csv
from sql_connector import SqliteConnector
import pandas as pd


def argument_parser():
    parser = argparse.ArgumentParser(description="Parser to read a filename from the command.")
    parser.add_argument("-db", "--database",
                        help="Name of the path containing the database", required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="Whether to display logging information.")
    parser.add_argument("-c", "--numbercommits", action="store_true", help="How many commits should the comment survive.")
    parser.add_argument("--merges", dest="merges", action="store_true",
                        help="Use only merge commits for tasks.")
    parser.add_argument("--no-merges", dest="merges", action="store_false",
                        help="Do not use only merge commits for tasks.")
    return parser


def save_results_to_file(file_to_save, filename):
    logging.info("Saving")
    file_to_save.to_csv(filename + ".csv",
                        index=False, encoding='utf-8', sep=",",
                        quoting=csv.QUOTE_NONNUMERIC)


def initialize_sql_query():
    query = '''select pmd.Project_ID,
            pmd.Project,
            comment.Comment_Content,
            class.Class_Name,
            method.Method_Name,
            comment.Comment_Source_File,
            pmd.Rule,
            pmd.Rule_Set,
            pmd.Priority,
            pmd.Description,
            pmd.Begin_Line,
            pmd.End_Line,
            comment.Comment_Line_No,
            method.Method_Line_No,
            method.Method_Line_No + method.Method_LOC,
            class.Class_Line_No,
            class.Class_Line_No + class.Class_LOC
            from comment join file on comment.Comment_Source_File = file.Source_File
            join class on class.Class_Source_File = method.Method_Source_File
            and method.Method_Line_No between class.Class_Line_No and class.Class_Line_No + class.Class_LOC 
            join method on method.Method_Source_File = comment.Comment_Source_File
            and comment.Comment_Line_No between method.Method_Line_No and method.Method_Line_No + method.Method_LOC
            join pmd on pmd.Project_ID = file.Project_ID
            and pmd.Filename = file.Source_File
            and pmd.Begin_Line between method.Method_Line_No and method.Method_Line_No + method.Method_LOC'''
    return query


if __name__ == "__main__":
    parser = argument_parser()
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    sql_connector = SqliteConnector()
    sql_connector.db_name = args.database
    sql_connector.connect_to_db()
    mycur = sql_connector.connection.cursor()
    mycur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    available_table = (mycur.fetchall())
    sql_query = initialize_sql_query()

    df1 = pd.read_sql_query(sql=sql_query, con=sql_connector.connection)
    logging.info("Read SQL to DF")
    df2 = pd.read_csv(filepath_or_buffer=".\\data\\technical_debt_dataset.csv")
    df2["clean_comment"] = df2["commenttext"]
    df2["clean_comment"] = df2["clean_comment"].replace("[^\w\s]", " ", regex=True)
    df2["clean_comment"] = df2["clean_comment"].replace("\s\s+", " ", regex=True)
    df2["clean_comment"] = df2["clean_comment"].str.strip()

    df1["Clean_Comment_Content"] = df1["Comment_Content"]
    df1["Clean_Comment_Content"] = df1["Clean_Comment_Content"].replace("[^\w\s]", " ", regex=True)
    df1["Clean_Comment_Content"] = df1["Clean_Comment_Content"].replace("\s\s+", " ", regex=True)
    df1["Clean_Comment_Content"] = df1["Clean_Comment_Content"].str.strip()

    logging.info("Read TD Data to DF")
    save_results_to_file(file_to_save=df1, filename="cleaned_soccminer_10_projects")
    save_results_to_file(file_to_save=df2, filename="cleaned_technical_debt_dataset")