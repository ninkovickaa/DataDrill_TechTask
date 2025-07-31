import argparse
from scripts import main

def run_pipeline(mode):
# Optionally perform different pipeline modes
    if mode == "extract":
        print("---> Running Bronze layer... \n READ csv \n VALIDATE \n SAVE IN BRONZE DIR")
        main.bronze_layer()
    elif mode == "transform":
        print("---> Running Silver layer... \n FORMAT DATE COL \n CALC METRIC \n MERGE ALL DF-s \n SAVE IN SILVER DIR ")
        main.silver_layer()
    elif mode == "report":
        print("🔧 Running Gold layer... \n LOAD VALIDATE DATAS TO SQL FOR MAKING DATA MODEL AND REPORTING ")
        main.gold_layer()
    elif mode == "full":
        print("---> Validation - formatting - calculation - visualisation")
        main.bronze_layer()
        main.silver_layer()
        main.gold_layer()
        main.visualisation()
    else:
        print(f"---> Unsupported mode: {mode}")

def main_cli():
    parser = argparse.ArgumentParser(description="Run the ETL pipeline")
    parser.add_argument("--mode", default="full", choices=["full", "extract", "transform", "report"], help="Pipeline mode")

    args = parser.parse_args()
    run_pipeline(mode=args.mode)

if __name__ == "__main__":
    main_cli()