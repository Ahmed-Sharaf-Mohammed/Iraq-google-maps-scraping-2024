from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, LabelEncoder, StandardScaler # OrdinalEncoder for multiple columns, LabelEncoder for single column ex: "yes" with ordinal will has 1 value but with label will value in each column
import pandas as pd
import os
import sys
import shutil
import pyfiglet
import numpy as np
import pyodbc


# Display all columns in the DataFrame
pd.set_option('display.max_columns', None)

# Display all rows in the DataFrame
# pd.set_option('display.max_rows', None)

base_dirs = [
    "C:",
    "D:",
    "E:"
]

csv_file = None

for base_dir in base_dirs:
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".csv") and "Students" in file:
                csv_file = os.path.join(root, file)
                break
        if csv_file:
            break
    if csv_file:
        break

if csv_file:

    df = pd.read_csv("Students.csv")
    ######################## 1. Dataset Overview ##################
    print("\n\n\n=================Dataset Overview=================")
    print(df.shape,"\n\n\n================info=================")
    print(df.info(),"\n\n\n==================isnull.sum===============") 
    print(df.isnull().sum(),"\n\n\n=================dublicate.sum================")
    print(df.duplicated().sum(),"\n\n\n==================head(5)===============")
    #print(df.drop_duplicated(),"\n==================================\n") No Duplicates
    print(df.head(5),"\n\n\n==================================\n")
    print(df['State'].describe(),"\n\n\n==================================\n")
    print(df.describe())



    ########################## 2. Dataset Mapping #################
    # print("\n=================Dataset Mapping=================\n") # No Need, All Data is clear
    #Device_Used_Mapping = {
    #    "Mobile": "Phone",
    #}
    # Create a new column with replaced values
    #df["Device_Used_V2"] = df["Device_Used"].replace(Device_Used_Mapping)
    #df = df.rename(columns={"Device_Used_V2": "Device_Used_V1"})
    # Another way to do the same thing
    #df["Device_Used"] = df["Device_Used"].replace(Device_Used_Mapping)



    ######################### 3. Handling Missing Values #################
    print("\n\n\n=================Handling Missing Values=================\n")
    # Filling missing values with specific values
    df["State"] = df["State"].fillna("Egypt （づ￣3￣）づ╭❤️～")
    print("==================now state with zero nulls===============") 
    print(df.isnull().sum())




    ######################### 4. remove unnecessary outliers#############
    # Example: Remove outliers in SalePrice using IQR
    #Q1 = df['Impact_on_Grades'].quantile(0.25)
    #Q3 = df['Impact_on_Grades'].quantile(0.75)
    #IQR = Q3 - Q1
    #df = df[~((df['Impact_on_Grades'] < (Q1 - 1.5 * IQR)) | (df['Impact_on_Grades'] > (Q3 + 1.5 * IQR)))]


    ########################## 5.Check data to choose best Data Encoding Strategy
    print("\n\n\n=================Check data to choose best Data Encoding Strategy=================\n")
    for col in df.select_dtypes(include=['object', 'int', 'float']).columns:
        print(f"ColumnName: {col}")
        print(df[col].value_counts())
        print("\n")



    ########################## 5.1. Encoding with OneHotEncoder
    print("\n\n\n=================Encoding with OneHotEncoder==============>>>>> column: Device_Used")
    columns = ['Device_Used']
    one = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    encoded = one.fit_transform(df[columns])
    encoded_df = pd.DataFrame(encoded, columns=one.get_feature_names_out(columns), index=df.index)
    df = df.drop(columns, axis=1).join(encoded_df)


    ########################## 5.2. Encoding Ordinal Variables
    print("\n\n\n=================Encoding Ordinal Variables==============>>>>> columns: Internet_Access, Willing_to_Pay_for_Access, Do_Professors_Allow_Use")
    ord = OrdinalEncoder()
    columns = ['Internet_Access', 'Willing_to_Pay_for_Access', 'Do_Professors_Allow_Use']
    df[columns] = ord.fit_transform(df[columns].astype(str))


    ########################## 5.3. Encoding Categorical Variables
    print("\n\n\n=================Encoding Categorical Variables==============>>>>> columns: all object type")
    le = LabelEncoder()
    columns = df.select_dtypes(include=['object']).columns
    for col in columns:
        df[col] = le.fit_transform(df[col].astype(str))
    

    print("\n\n\n=================Final DataFrame after Encoding==============")
    print(df.head(5))


    ########################## 6. Normalization with StandardScaler ##################
    print("\n\n\n=================Normalization with StandardScaler==============>>>>> columns: all number types")
    num_cols = df.select_dtypes(include=['float','int']).columns
    scaler = StandardScaler()
    df[num_cols] = scaler.fit_transform(df[num_cols])



    print("\n\n\n=================Final DataFrame after Normalization==============")
    print(df.head(5))

    # Save the processed DataFrame to a new CSV file
    df.to_csv("encoded_data.csv", index=False)


else:
    print("No Students.CSV found in any path!")

try:
    base_path = sys._MEIPASS
except AttributeError:
    base_path = os.path.abspath(".")

ascii_art = pyfiglet.figlet_format("Ahmed SHARAF", font="doom")
print(ascii_art)

try:
    base_path = sys._MEIPASS  # PyInstaller temp folder
except AttributeError:
    base_path = os.path.abspath(".")

src_file = os.path.join(base_path, "code.py")
dst_file = os.path.join(os.path.dirname(sys.executable), "code.py")
shutil.copy(src_file, dst_file)
print(f"my python file code.py and encoded_data.csv extracted to: {dst_file}")

print("Program finished. Press Enter to exit...")
input()
