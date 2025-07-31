import pandas as pd
from datetime import datetime

current_date = pd.Timestamp.today()

def validate_data(df,df_name, reference_df=None):
    string_columns = ['first_name', 'last_name', 'position', 'department','location', 'manager']
    error = False
    error_msg = ""
    print(f"\n Validation for {df_name } starts ...")

# Universal checks for every df
    # 1. Remove rows where all values are NaN
    df_cleaned = df.dropna(how='all')

    # 2. Check for columns where all values are NaN
    empty_cols = df_cleaned.columns[df_cleaned.isnull().all()].tolist()
    if empty_cols:
        error_msg += f"\n -Column {empty_cols} can't be empty"

    # 3. Check for partially missing values in columns
    partial_missing = df_cleaned.columns[df_cleaned.isnull().any()].tolist()
    if partial_missing:
        error_msg += f"\n -Column {partial_missing} has missing values."
        error = True


#Particular check for specific df
    if df_name == "employees": 
        for col in df_cleaned.columns:
            if col == "employee_id":
                try:
                    df_cleaned[col] = df_cleaned[col].astype(int)
                except Exception as e:
                    error_msg += f"\n -Failed to convert column '{col}' to int. Error: {e}"
                    error = True
                
            elif col in string_columns:
                if not pd.api.types.is_string_dtype(df_cleaned[col]):
                    try:
                        df_cleaned[col] = df_cleaned[col].astype(str)
                    except Exception as e:
                        error_msg += f"\n -Failed to convert column '{col}' to string. Error: {e}"
                        error = True

            elif col == "start_date":
                # Try to convert to datetime
                try:
                    df_cleaned['start_date'] = pd.to_datetime(df_cleaned['start_date'], errors='coerce')
                except Exception as e:
                    error_msg += f"\n -Failed to convert 'start_date' to datetime. Error: {e}"
                    error = True

                # Check if any future dates is set for start_date
                future_dates = df_cleaned[df_cleaned[col] > current_date]
                if not future_dates.empty:

                    error_msg += f"\n -Found {len(future_dates)} employees with future start_date. \n Details: {future_dates} "
                    error = True
    
    if df_name == "departments": 
            for col in df_cleaned.columns:
                if col in string_columns:
                    if not pd.api.types.is_string_dtype(df_cleaned[col]):
                        try:
                            df_cleaned[col] = df_cleaned[col].astype(str)
                        except Exception as e:
                            error_msg += f"\n -Failed to convert column '{col}' to string. Error: {e}"
                            error = True
    
    if df_name == "salaries": 
        for col in df_cleaned.columns:
            if col == "gross_salary":
                try:
                    df_cleaned[col] = df_cleaned[col].astype(float)
                    for val in df_cleaned[col]:
                        if val < 0:
                            error_msg += f"\n -{col} can't be negative number"
                            error = True
                except Exception as e:
                    error_msg += f"\n -Failed to convert column '{col}' to float. Error: {e}"
                    error = True
            
            elif  col == "month":
                for val in df_cleaned[col]:
                    if val < 1 or val > 12:
                        error_msg += f"\n -{col} needs to be in range from 1 to 12"
                        error = True
            
            elif  col == "year":
                for val in df_cleaned[col]:
                    if val > datetime.now().year:
                        error_msg += f"\n -{col} in salaries can't be bigger than current year"
                        error = True

            elif col != "gross_salary":  
                if not pd.api.types.is_integer_dtype(df_cleaned[col]):
                    try:
                        df_cleaned[col] = df_cleaned[col].astype(int)
                    except Exception as e:
                        error_msg += f"\n -Failed to convert column '{col}' to integer. Error: {e}"
                        error = True


        if reference_df is None:
                error_msg += f"\n -'Employees' reference DataFrame is required to validate salaries."
                error = True
                # return False, None

        elif  'employee_id' not in df_cleaned.columns or 'employee_id' not in reference_df.columns:
                error_msg += f"\n -'Employee_id' must exist in both salaries and employees DataFrames."
                error = True

        else:
            unknown_ids = df_cleaned[~df_cleaned['employee_id'].isin(reference_df['employee_id'])]
            if not unknown_ids.empty:
                missing_ids = unknown_ids['employee_id'].drop_duplicates()
                error_msg += f"\n -Found {len(missing_ids)} salary records with unknown employee_id:\n{missing_ids.to_string(index=False)}"
                error = True

    if error:
        print(f"\n List of errors: {error_msg}")
        print("******************************************")
        return False
    
    return True,df_cleaned