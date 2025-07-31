from sqlalchemy import create_engine
import urllib

try:
    coxn = create_engine(
            "mssql+pyodbc:///?odbc_connect=" +
            urllib.parse.quote_plus(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=DESKTOP-IGQQ4TU;"
                "DATABASE=GoldLayer;"
                "Trusted_Connection=yes;"
            )
    )
    print("pass")
except:
    print("fail")

