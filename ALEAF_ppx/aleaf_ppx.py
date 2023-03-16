import pandas as pd
import os
import plotly.express as px
import kaleido
import sqlite3
import argparse
import glob
import logging
import openpyxl
from pathlib import Path

# Specification for database tables
tables = {"sim_config_data":
          [("Case_ID", "text"),
           ("ATB_Setting", "text"),
           ("NDAY_Groups", "integer"),
           ("NDAYS_in_Single_Group", "integer"),
           ("NDAY_Groups_OP", "integer"),
           ("NDAYS_in_Single_Group_OP", "integer"),
           ("HFREQ", "integer"),
           ("FIVEMIN", "integer"),
           ("FIVEMIN_OP", "integer"),
           ("PD", "real"),
           ("VOLL", "real"),
           ("SRSP", "real"),
           ("NSRSP", "real"),
           ("FLEXRSP", "real"),
           ("ITC_Flag", "text"),
           ("PTC_Flag", "text"),
           ("CTAX", "real"),
           ("RPS_Flag", "text"),
           ("Energy_Stroage_AET_Limit_Flag", "text"),
           ("Clean_Energy_Generation_Target_Flag", "text"),
           ("Clean_Energy_Generation_Target_Start_Year", "integer"),
           ("Carbon_Emission_Reduction_Target_Flag", "text"),
           ("Carbon_Emission_Reduction_Target_Start_Year", "integer")
          ],


         "unit_specs":
          [("Scenario_ID", "text"),
           ("Tech_ID", "text"),
           ("UNITGROUP", "text"),
           ("UNIT_CATEGORY", "text"),
           ("UNIT_TYPE", "text"),
           ("FUEL", "text"),
           ("CAP", "real"),
           ("Charge_CAP", "real"),
           ("STOHR_MIN", "real"),
           ("STOHR_MAX", "real"),
           ("PMAX", "real"),
           ("PMIN", "real"),
           ("FOR", "real"),
           ("CAPEX", "text"),
           ("STO_CAPEX", "text"),
           ("FCR", "text"),
           ("RETC", "real"),
           ("DECC", "real"),
           ("FOM", "text"),
           ("VOM", "text"),
           ("FC", "real"),
           ("NLC", "real"),
           ("SUC", "real"),
           ("SDC", "real"),
           ("HR", "real"),
           ("Ramp", "real"),
           ("RUL", "real"),
           ("RDL", "real"),
           ("MAXR", "real"),
           ("MAXC", "real"),
           ("CAPCRED", "real"),
           ("EMSFAC", "real"),
           ("STOMIN", "real"),
           ("INVEST_FLAG", "text"),
           ("ES_STO_INVEST_FLAG", "text"),
           ("MAXINVEST", "real"),
           ("MININVEST", "real"),
           ("RET_FLAG", "text"),
           ("MINRET", "real"),
           ("VRE_Flag", "text"),
           ("Clean_Energy_Flag", "text"),
           ("BATEFF", "text"),
           ("AET", "text"),
           ("Storage Commitment", "text"),
           ("Commitment", "text"),
           ("Integrality", "text"),
           ("Emission", "real"),
           ("Material_Flag", "text"),
           ("Resource_Limit_Flag", "text"),
           ("Locational_Scailing_Flag", "text"),
           ("Life", "real"),
           ("ITC Flag", "text"),
           ("PTC Flag", "text"),
           ("Must_Run_Flag", "text"),
           ("Must_Run_Level", "real")
          ],

         "system_tech_summary_data":
          [("Scenario", "text"),
           ("Year", "integer"),
           ("Bus_ID", "text"),
           ("Bus_Name", "text"),
           ("Region_Name", "text"),
           ("Tech_ID", "text"),
           ("UnitGroup", "text"),
           ("Unit_Category", "text"),
           ("Unit_Type", "text"),
           ("Fuel", "text"),
           ("TotalUnits", "real"),
           ("NewUnits", "real"),
           ("RetUnits", "real"),
           ("ICAP", "real"),
           ("UCAP", "real"),
           ("ICap_New", "real"),
           ("ICap_Ret", "real"),
           ("ICap_PlanRet", "real"),
           ("ICap_EconRet", "real"),
           ("UCap_New", "real"),
           ("UCap_Ret", "real"),
           ("UCap_PlanRet", "real"),
           ("UCap_EconRet", "real"),
           ("Storage_Hr", "real"),
           ("Generation", "real"),
           ("Reserve_RegUp", "real"),
           ("Reserve_RegDn", "real"),
           ("Reserve_Spin", "real"),
           ("Reserve_FlexUp", "real"),
           ("Reserve_FlexDn", "real"),
           ("UnitRevenue", "real"),
           ("UnitProfit", "real"),
           ("FuelConsumption", "real"),
           ("FuelCost", "real"),
           ("Annual_INVC", "real"),
           ("Annual_STO_INVC", "real"),
           ("FOM", "real"),
           ("MC", "real")
          ]
}


def cli_args():
    parser = argparse.ArgumentParser(description="Postprocess C2N A-LEAF data into a single database.")

    parser.add_argument(
        "--dir",
        type=str,
        help="relative path to directory containing data to add",
        default=Path.cwd()
    )

    parser.add_argument(
        "--db_file",
        type=str,
        help="relative path to database file to create or update",
        default=Path(Path.cwd() / "C2N_db.db")
    )

    parser.add_argument(
        "--overwrite", "-o",
        action="store_true",
        help="overwrite the database file, if it already exists"
    )


    args = parser.parse_args()

    return args


def get_data_dirs(data_dir):
    # List of directories from which to retrieve data
    data_dirs_list = []

    # Required file globs
    globs = ["ALEAF_Master_LC_GTEP*.xlsx", "*system_tech_summary*.csv"]

    # Check whether necessary file globs are found in the top level of data_dir
    if all([len(list(Path(data_dir).glob(pattern))) > 0 for pattern in globs]):
        data_dirs_list.append(Path(data_dir))

    # For all subdirectories in data_dir, check whether it shows at least one
    #   match for each required file glob
    for path in Path(data_dir).glob("**/*"):
        if path.is_dir():
            if all([len(list(path.glob(pattern))) > 0 for pattern in globs]):
                data_dirs_list.append(path)

    return data_dirs_list        


def read_ALEAF_data(data_dir):
    sts_data = read_sts_data(data_dir)
    simconfig_data, unit_specs = read_settings_data(data_dir)

    return sts_data, simconfig_data, unit_specs


def read_sts_data(data_dir):
    sts_data_files = glob.glob(
                         str(Path(Path.cwd() / data_dir / "*system_tech_summary*"))
                     )

    if len(sts_data_files) > 1:
        logging.warning(f"Multiple system_tech_summary files found in {data_dir}!! Using the first one found...")

    try:
        sts_data = pd.read_csv(sts_data_files[0])
    except:
        logging.error(f"Could not read system_tech_summary data from {sts_data_files[0]}.")
        raise IOError

    return sts_data


def read_settings_data(data_dir):
    settings_data_files = glob.glob(
                              str(Path(Path.cwd() / data_dir / "ALEAF_Master_LC_GTEP*.xlsx"))
                          )

    if len(settings_data_files) > 1:
        logging.warning(f"Multiple ALEAF_Master_LC_GTEP* files found in {data_dir}!! Using the first one found...")

    simconfig_data = pd.read_excel(
                         settings_data_files[0],
                         sheet_name="Simulation Configuration",
                         engine="openpyxl"
                     )

    unit_specs = pd.read_excel(
                     settings_data_files[0],
                     sheet_name="Gen Technology",
                     engine="openpyxl"
                 )

    # Add Scenario_ID to unit_specs for indexing later
    unit_specs["Scenario_ID"] = simconfig_data.loc[0, "Case_ID"]
    # Remove useless unnamed columns
    unit_specs = unit_specs.drop(columns=[col for col in unit_specs.columns if "Unnamed" in col])

    return simconfig_data, unit_specs


def open_db(args):
    db_file = Path(Path.cwd() / args.db_file).resolve()

    # Check whether the database exists
    if Path.is_file(db_file):
        if not args.overwrite:
            # Warn the user and exit
            logging.error(f"A database file already exists at {db_file}. Either move or rename this existing file, or re-run this script with the --overwrite/-o flag set in the command line.")
            exit()

        else:
            # Overwrite the existing database file
            os.remove(db_file)
            logging.info(f"Existing database file at {db_file} removed.")
            db = sqlite3.connect(str(db_file))
            make_db_tables(db, tables)
            logging.info(f"New database file created at {db_file}.")

    else:
        # Create a connection to the existing database file
        db = sqlite3.connect(str(db_file))

    return db    


def make_db_tables(db, tables):
    cur = db.cursor()

    for table in tables:
        # Get the column names and types from the 'tables' specification
        sql_cols = []
        for column in tables[table]:
            sql_cols.append(f"'{column[0]}' {column[1]}")
        cmd = f"CREATE TABLE {table} (" + ", ".join(sql_cols) + ")"

        # Add this table to the database
        cur.execute(cmd)
        db.commit()


def add_ALEAF_data_to_db(db, simconfig_data, unit_specs, sts_data):
    simconfig_data.to_sql("sim_config_data", db, if_exists="append", index=False)
    unit_specs.to_sql("unit_specs", db, if_exists="append", index=False)
    sts_data.to_sql("system_tech_summary_data", db, if_exists="append", index=False)

    db.commit()


def postprocess_aleaf_data(args):
    # Set up the logger
    logging.basicConfig(level=logging.INFO)

    # Create the database file (if necessary) and open a sqlite connection
    #   to it
    db = open_db(args)

    # Get the list of all directories containing the necessary sets of
    #   data files
    data_dirs_list = get_data_dirs(args.dir)

    for data_dir in data_dirs_list:
        logging.info(f"Retrieving data from {data_dir}.")
        # Read in the A-LEAF data
        sts_data, simconfig_data, unit_specs = read_ALEAF_data(data_dir)

        # Add the A-LEAF data to the database
        add_ALEAF_data_to_db(db, simconfig_data, unit_specs, sts_data)

if __name__ == "__main__":
    args = cli_args()
    postprocess_aleaf_data(args)

