
#  Mini Data Lake – Zadatak za Python kandidata

## Zadatak

Zamislite da radite na implementaciji internog mini data lake-a. Klijent vam je poslao sledeće CSV fajlove:

- `employees.csv`
- `salaries.csv`
- `departments.csv`

### Vaš zadatak:

#### 1. Bronze layer
- Učitati CSV fajlove
- Validirati podatke
- Sačuvati ih u `bronze/` folder u Parquet formatu

**<u> SOLUTION: </u>**
 - Segmented in function bronze layer() in main.py  -> read every csv file into separated dataframe
 - Validation is done using validate_date function inside helper functions inside help_functions.py. Explainining function
    - Passing arguments: df (dataset) and df_name (file name) are required; reference_df is required for salaries df  - salary can't be defined for employee_id who doesn't exist
    - Remove all NULL rows
    - Check for whole NULL col. Not removing because in current dataset every col is important so that can be flag that dataset is not good
    - Check for missing values in col. Not removing because in current dataset every col should have some value so that can be flag that dataset is not good
    - Conversion of columns into specific type if is possible 
    - Specific cheks based on specific col into specific df
        * start_date must be earlier or equal than current date
        * gross_salary must be positive value
        * month must be in range [1,12] 
        * year must be earlier/same as current year
        * salary can't be defined for user who doesn't exist  
     - If validation is good, function returns cleaned_df and True value. If validation fails, function shows error msg and returns False.
- After successfull validation, df are saved in parquet format into bronze dir
<br>______________________________________________________________________________________________________________________________________________
#### 2. Silver layer
- Formatirati `start_date`
- Izračunati `tenure_in_months` po zaposlenom
- Obogatiti `salaries` podacima iz drugih fajlova
- Sačuvati kao Parquet fajlove u `silver/`

**<u> SOLUTION: </u>**
 - Segmented in function silver layer() in main.py  -> use raw df_employees. First I calculated tenure_in_months using relativedelta function for calculating difference in months between current date and start date.
 - After that I formatted start_date because I wanted to avoid double conversion (relativedate works with datetime formats, not with string)
 - Merge salary df with employees by employee_id as fk and then with departments by department as fk. Merged df is saved in parquet format into silver dir
<br>______________________________________________________________________________________________________________________________________________

#### 3. Gold layer (izveštaj)
Napraviti CSV izveštaj:
- prosečna bruto plata po departmanu
- broj zaposlenih po lokaciji
- departman sa najdužim prosečnim stažom
- Sačuvati kao `gold/summary_report.csv`.

**<u> SOLUTION: </u>**
- Segmented in function gold_layer() in main.py  -> if datas are validated, I pull every df in local sql db (ssms) as table so I can make datamodel for building summary_report. I will leave xlsx file so you can be able to see datamodel. Now pull to sql is commented because code will be broken if you run because db is local (I will send you db export with tables and procedures).
- I also made csv report from python code using merged df. Function csv_report() where you can see calculated metrics.
 
 <br>______________________________________________________________________________________________________________________________________________


#### 4. Ostalo
- Vizualizacija u Matplotlib/Seaborn
- CLI skripta `run_pipeline.py`
- README sa uputstvima
- requirements.txt

**<u> SOLUTION: </u>**
- Segmented in function visualisation() in main.py. It is done using matplot lib. I made subplot configuration with 2 rows and 2 col so I can represents 4 graphs. I calculated metrics for average, count, max and use them for specific visual.
- **For running cli script**: 
    - python run_pipeline.py 
        -   default mode is full so it means all layers function will be executed (bronze_layer(),silver_layer(),visualisation()) 
        - mode extract executes bronze_layer()
        - mode transform executed silver_layer()
        - mode sql executes gold_layer() - pull df to sql 

 <br>______________________________________________________________________________________________________________________________________________


#### 5. Predaja
Pošaljite:
- GitHub link do koda
- README.md sa opisom rešenja

Srećno!
