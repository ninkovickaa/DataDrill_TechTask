import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
# if run direcrtly from main.py use import witout scripts.
# from help_functions import *
# from db_secrets import coxn 
from scripts.help_functions import *
from scripts.db_secrets import coxn 
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

local_file_path = 'C:/Users/Dell E7440/Desktop/datadrill_data_lake_project'

f_depart = fr'{local_file_path}/departments.csv'  
f_empl = fr'{local_file_path}/employees.csv'  
f_salaries = fr'{local_file_path}/salaries.csv'  
    
df_departments = pd.read_csv(f_depart)
df_employees = pd.read_csv(f_empl)
df_salaries = pd.read_csv(f_salaries)

def csv_report():
    output = StringIO()
    df = silver_layer()

    # 1. Avg gross salary by department
    avg_salary_by_dept = df.groupby('department')['gross_salary'].mean().reset_index()
    avg_salary_by_dept.rename(columns={'gross_salary': 'avg_gross_salary'}, inplace=True)

    # 2. Count of employees by location
    employees_by_location = df.groupby('location')['employee_id'].nunique().reset_index()
    employees_by_location.rename(columns={'employee_id': 'num_employees'}, inplace=True)

    # 3. Department with the longest tenure in months
    avg_tenure_by_dept = df.groupby('department')['tenure_in_months'].mean().reset_index()
    top_dept_by_tenure = avg_tenure_by_dept.sort_values('tenure_in_months', ascending=False).head(1)

    output.write("Average gross salary by department\n")
    avg_salary_by_dept.to_csv(output, index=False)

    output.write("Count of employees by location\n")
    employees_by_location.to_csv(output, index=False)

    output.write("Department with the longest tenure in months\n")
    top_dept_by_tenure.to_csv(output, index=False)

    with open("gold/summary_report.csv", "w", encoding="utf-8") as f:
        f.write(output.getvalue())
    print("Summary_report.csv is ready")

#-------1. Bronze layer-------------------
def bronze_layer():
    f_depart = fr'{local_file_path}/departments.csv'  
    f_empl = fr'{local_file_path}/employees.csv'  
    f_salaries = fr'{local_file_path}/salaries.csv'  
    
    #--read
    df_departments = pd.read_csv(f_depart)
    df_employees = pd.read_csv(f_empl)
    df_salaries = pd.read_csv(f_salaries)

    #--validation
    if validate_data(df_employees,"employees"):
    #--save in bronze dict, parquet format
        try:
            df_employees.to_parquet(f"{local_file_path}/bronze/employees.parquet")
            print(f"Validation is done. Df_employees is successfuly convert in parquet format. Directory: bronze")
        except Exception as e:
            print(f"Failed to convert in parquet format: {e}")
    else:
        print('Validation for employees can not be done')


    if validate_data(df_departments,"departments"):
        try:
            df_departments.to_parquet(f"{local_file_path}/bronze/departments.parquet")
            print(f"Validation is done. Df_departments is successfuly convert in parquet format")
        except Exception as e:
            print(f"Failed to convert in parquet format: {e}")
    else:
        print('Validation for departments can not be done')

    if validate_data(df_salaries,"salaries",df_employees):
        try:
            df_salaries.to_parquet(f"{local_file_path}/bronze/salaries.parquet")
            print(f"Validation is done. Df_salaries is successfuly convert in parquet format")
        except Exception as e:
            print(f"Failed to convert in parquet format: {e}")
    else:
        print('Validation for salaries can not be done')

#-------2. Silver layer-------------------
def silver_layer():    
    # For avoiding double conversion, fist I am calculating tenure_in_months (so I can use relativedelta function for datediff) and then fomat start_date col
    df_employees['start_date'] = pd.to_datetime(df_employees['start_date'], errors='coerce')

    # Date difference in months between today and start date  
    df_employees['tenure_in_months'] = df_employees['start_date'].apply(
        lambda start: relativedelta(datetime.now(), start).years * 12 + relativedelta(current_date, start).months
        if pd.notnull(start) else None
    )
    # Format start_date column in d-m-y
    df_employees['start_date'] = df_employees['start_date'].dt.strftime('%d-%m-%Y')

    # First merge: salaries + employees on employee_id as fk
    df_salaries_empl_enriched = pd.merge(
        df_salaries,
        df_employees[['employee_id', 'first_name', 'last_name', 'department', 'position', 'start_date', 'tenure_in_months']],
        on='employee_id',
        how='left'  
    )

    # Second merge: enriched salaries + department od department as fk
    df_salaries_enriched = pd.merge(
        df_salaries_empl_enriched,
        df_departments[['department', 'location', 'manager']],
        on='department',
        how='left'  
    )
    # print(df_salaries_enriched)
    try:
        df_employees.to_parquet(f"{local_file_path}/silver/employees.parquet")
        df_salaries_enriched.to_parquet(f"{local_file_path}/silver/salaries.parquet")
        print(f"Df_employees and df_salaries_enriched are successfuly converted in parquet format. Directory: silver")
    except Exception as e:
        print(f"Failed to convert in parquet format: {e}")
    return df_salaries_enriched
# silver_layer()

#-------3. Gold layer-------------------
def gold_layer():
    #pull validated df in sql for making data model
    try:
        # if validate_data(df_employees,"employees"):
        #     df_employees.to_sql(name="employees", schema="stg", con=coxn, if_exists='replace', index=False)
        #     print("Table employees is loaded successfully")

        # if validate_data(df_departments,"departments"):
        #     df_departments.to_sql(name="departments", schema="stg", con=coxn, if_exists='replace', index=False)
        #     print("Table departments is loaded successfully")

        # if validate_data(df_salaries,"salaries", df_employees):
        #     df_salaries.to_sql(name="salaries", con=coxn, schema="stg", if_exists='replace', index=False)
        #     print("Table salaries is loaded successfully")
        #make csv report from python
        csv_report()

    except Exception as e:
        print(f"Error msg during building report: {e}")

# gold_layer()

#-------4. Other-------------------
def visualisation():
    df = silver_layer()

    # metrics
    avg_salary_per_dept = df.groupby('department')['gross_salary'].mean().sort_values(ascending=False)
    num_employees_by_location = df.groupby('location')['employee_id'].nunique().sort_values()
    top3_positions = df.groupby('position')['gross_salary'].mean().sort_values(ascending=False).head(3)

    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    avg_salary_trend = df.groupby('date')['gross_salary'].mean()

    # Subplot configuration
    fig, axes = plt.subplots(2, 2, figsize=(20, 10))

    axes[0, 0].barh(avg_salary_per_dept.index, avg_salary_per_dept.values, color='skyblue')
    axes[0, 0].set_title('Average gross salary by department')
    axes[0, 0].set_xlabel('Gross salary')
    axes[0, 0].grid(axis='x', linestyle='--', alpha=0.5)

    axes[0, 1].bar(num_employees_by_location.index, num_employees_by_location.values, color='lightgreen')
    axes[0, 1].set_title('Count of employees per location')
    axes[0, 1].set_xlabel('Location')
    axes[0, 1].set_ylabel('Count of employees')
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 1].grid(axis='y', linestyle='--', alpha=0.5)
    axes[0, 1].yaxis.set_major_locator(MaxNLocator(integer=True)) 

    axes[1, 0].plot(avg_salary_trend.index, avg_salary_trend.values, marker='o', color='orange')
    axes[1, 0].set_title('Gross salary trendline')
    axes[1, 0].set_xlabel('Date')
    axes[1, 0].set_ylabel('Average gross salary')
    axes[1, 0].grid(True)

    axes[1, 1].bar(top3_positions.index, top3_positions.values, color='salmon')
    axes[1, 1].set_title('Top 3 highest paid positions')
    axes[1, 1].set_xlabel('Position')
    axes[1, 1].set_ylabel('Avg gross salary')
    axes[1, 1].tick_params(axis='x', rotation=30)
    axes[1, 1].grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.show()
    
# visualisation()
