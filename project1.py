"""CENSUS DATA STANDARDIZATION AND ANALYSIS PIPELINE 

Author:GOKULARAJA R"""

#importing modules 
import pandas as pd
import credentials
import mysql.connector
import streamlit as st
from pymongo import MongoClient

# Reading the dataset
df=pd.read_excel("census_2011.xlsx")

# Renaming the columns to maintain uniformity and clarity and Ensure the column names do not exceed 60 characters
df.rename(columns={ 'State name': 'State_UT',
                   'District code':'District_Code',
                   'District name': 'District',
                   'Male_Literate': 'Literate_Male',
                   'Female_Literate': 'Literate_Female',
                   'Rural_Households': 'Households_Rural',
                   'Urban_Households': 'Households_Urban',
                   'Age_Group_0_29': 'Young_and_Adult', 'Age_Group_30_49': 'Middle_Aged',
                   'Age_Group_50': 'Senior_Citizen', 'Age not stated': 'Age_Not_Stated',
                   'Housholds_with_Electric_Lighting' : 'Households_with_Electric_Lighting',
                   'Married_couples_5__Households' : 'Married_couples_5_Households',
                   'Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car' : 'HH_TV_Computer_Phone_Vehicle',
                   'Condition_of_occupied_census_houses_Dilapidated_Households' : 'Dilapidated_Census_Houses',
                   'Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households' : 'Main_Water_Source_Other',
                   'Location_of_drinking_water_source_Near_the_premises_Households' : 'Water_Location_Near',
                   'Households_with_Telephone_Mobile_Phone_Landline_only' : 'HH_Phone_Landline',
                   'Households_with_Telephone_Mobile_Phone_Mobile_only' : 'HH_Phone_Mobile',
                   'Households_with_separate_kitchen_Cooking_inside_house' : 'HH_Separate_Kitchen',
                   'Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households' : 'No_Latrine_Alt_Source',
                   'Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households' : 'Flush_Latrine_System',
                   'Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households' : 'Main_Water_Source_Handpump',
                   'Main_source_of_drinking_water_Tank_Pond_Lake_Households' : 'Main_Water_Source_Tank',
                   'Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households' : 'Latrine_Night_Soil_Open_Drain',
                   'Location_of_drinking_water_source_Within_the_premises_Households' : 'Water_Location_Within_Premises'},inplace=True)

# Use a lambda function to format state/UT names to Title Case and take care of "&" too
df['State_UT'] = df['State_UT'].apply(lambda name: ' '.join([word.capitalize() if word.lower() != 'and' else word.lower() for word in name.replace('&', 'and').split()]))

# Handle new State/UT formation after 2011
with open('Telangana.txt', 'r',encoding='utf-8') as file:
    telangana_districts = file.read().splitlines()

df.loc[df['District'].isin(telangana_districts) & (df['State_UT'] == 'Andhra Pradesh'), 'State_UT'] = 'Telangana'
df.loc[df['District'].isin(['Leh(Ladakh)', 'Kargil']) & (df['State_UT'] == 'Jammu and Kashmir'), 'State_UT'] = 'Ladakh'

# Process missing data and Data cleaning
Before_cleaning=pd.DataFrame(df.isnull().mean()*100)  #Storing the initial null percentage in the dataset for comparison purpose

#Data cleaning
# Male, Female, Population
mean_female_to_male = (df['Female'] / df['Male']).mean(skipna=True)
mean_male_to_female = (df['Male'] / df['Female']).mean(skipna=True)

# Female Fill
df.loc[df['Female'].isna() & df['Male'].notna(), 'Female'] = df['Male'] * mean_female_to_male
# Male Fill
df.loc[df['Male'].isna() & df['Female'].notna(), 'Male'] = df['Female'] * mean_male_to_female
# Both Male & Female NULL (Fill from Population)
df.loc[df['Female'].isna() & df['Male'].isna() & df['Population'].notna(), 'Female'] = (df['Population'] / (1 + mean_female_to_male))
df.loc[df['Female'].notna() & df['Male'].isna() & df['Population'].notna(), 'Male'] = df['Population'] - df['Female']

df['Population'] = df['Population'].fillna(df['Male'] + df['Female'])

# Literate_Male, Literate_Female, Literate
mean_literate_female_to_male = (df['Literate_Female'] / df['Literate_Male']).mean(skipna=True)
mean_literate_male_to_female = (df['Literate_Male'] / df['Literate_Female']).mean(skipna=True)

df.loc[df['Literate_Female'].isna() & df['Literate_Male'].notna(), 'Literate_Female'] = df['Literate_Male'] * mean_literate_female_to_male
df.loc[df['Literate_Male'].isna() & df['Literate_Female'].notna(), 'Literate_Male'] = df['Literate_Female'] * mean_literate_male_to_female
df.loc[df['Literate_Female'].isna() & df['Literate_Male'].isna() & df['Literate'].notna(), 'Literate_Female'] = (df['Literate'] / (1 + mean_literate_female_to_male))
df.loc[df['Literate_Female'].notna() & df['Literate_Male'].isna() & df['Literate'].notna(), 'Literate_Male'] = df['Literate'] - df['Literate_Female']

df['Literate'] = df['Literate'].fillna(df['Literate_Male'] + df['Literate_Female'])

# SC

mean_female_sc_to_male = (df['Female_SC'] / df['Male_SC'])[ (df['Female_SC'] != 0) & (df['Male_SC'] != 0) ].mean(skipna=True)
mean_male_sc_to_female = (df['Male_SC'] / df['Female_SC'])[ (df['Male_SC'] != 0) & (df['Female_SC'] != 0) ].mean(skipna=True)

df.loc[df['Female_SC'].isna() & df['Male_SC'].notna(), 'Female_SC'] = df['Male_SC'] * mean_female_sc_to_male
df.loc[df['Male_SC'].isna() & df['Female_SC'].notna(), 'Male_SC'] = df['Female_SC'] * mean_male_sc_to_female
df.loc[df['Female_SC'].isna() & df['Male_SC'].isna() & df['SC'].notna(), 'Female_SC'] = (df['SC'] / (1 + mean_female_sc_to_male))
df.loc[df['Female_SC'].notna() & df['Male_SC'].isna() & df['SC'].notna(), 'Male_SC'] = df['SC'] - df['Female_SC']

df['SC'] = df['SC'].fillna(df['Male_SC'] + df['Female_SC'])

# ST
mean_female_st_to_male = (df['Female_ST'] / df['Male_ST']).mean(skipna=True)
mean_male_st_to_female = (df['Male_ST'] / df['Female_ST']).mean(skipna=True)

df.loc[df['Female_ST'].isna() & df['Male_ST'].notna(), 'Female_ST'] = df['Male_ST'] * mean_female_st_to_male
df.loc[df['Male_ST'].isna() & df['Female_ST'].notna(), 'Male_ST'] = df['Female_ST'] * mean_male_st_to_female
df.loc[df['Female_ST'].isna() & df['Male_ST'].isna() & df['ST'].notna(), 'Female_ST'] = (df['ST'] / (1 + mean_female_st_to_male))
df.loc[df['Female_ST'].notna() & df['Male_ST'].isna() & df['ST'].notna(), 'Male_ST'] = df['ST'] - df['Female_ST']

df['ST'] = df['ST'].fillna(df['Male_ST'] + df['Female_ST'])

# Workers
mean_female_workers_to_male = (df['Female_Workers'] / df['Male_Workers']).mean(skipna=True)
mean_male_workers_to_female = (df['Male_Workers'] / df['Female_Workers']).mean(skipna=True)

df.loc[df['Female_Workers'].isna() & df['Male_Workers'].notna(), 'Female_Workers'] = df['Male_Workers'] * mean_female_workers_to_male
df.loc[df['Male_Workers'].isna() & df['Female_Workers'].notna(), 'Male_Workers'] = df['Female_Workers'] * mean_male_workers_to_female
df.loc[df['Female_Workers'].isna() & df['Male_Workers'].isna() & df['Workers'].notna(), 'Female_Workers'] = (df['Workers'] / (1 + mean_female_workers_to_male))
df.loc[df['Female_Workers'].notna() & df['Male_Workers'].isna() & df['Workers'].notna(), 'Male_Workers'] = df['Workers'] - df['Female_Workers']

df['Workers'] = df['Workers'].fillna(df['Male_Workers'] + df['Female_Workers'])

# Filling the na's with respective mean

df['Main_Workers'] = df['Main_Workers'].fillna(round(df['Main_Workers'].mean()))
df['Marginal_Workers'] = df['Marginal_Workers'].fillna(round(df['Marginal_Workers'].mean()))
df['Non_Workers'] = df['Non_Workers'].fillna(round(df['Non_Workers'].mean()))
df['Cultivator_Workers'] = df['Cultivator_Workers'].fillna(round(df['Cultivator_Workers'].mean()))
df['Agricultural_Workers'] = df['Agricultural_Workers'].fillna(round(df['Agricultural_Workers'].mean()))
df['Household_Workers'] = df['Household_Workers'].fillna(round(df['Household_Workers'].mean()))
df['Other_Workers'] = df['Other_Workers'].fillna(round(df['Other_Workers'].mean()))
df['Hindus'] = df['Hindus'].fillna(round(df['Hindus'].mean()))
df['Muslims'] = df['Muslims'].fillna(round(df['Muslims'].mean()))
df['Christians'] = df['Christians'].fillna(round(df['Christians'].mean()))
df['Sikhs'] = df['Sikhs'].fillna(round(df['Sikhs'].mean()))
df['Buddhists'] = df['Buddhists'].fillna(round(df['Buddhists'].mean()))
df['Jains'] = df['Jains'].fillna(round(df['Jains'].mean()))
df['Others_Religions'] = df['Others_Religions'].fillna(round(df['Others_Religions'].mean()))
df['Religion_Not_Stated'] = df['Religion_Not_Stated'].fillna(round(df['Religion_Not_Stated'].mean()))
df['LPG_or_PNG_Households'] = df['LPG_or_PNG_Households'].fillna(round(df['LPG_or_PNG_Households'].mean()))
df['Households_with_Electric_Lighting'] = df['Households_with_Electric_Lighting'].fillna(round(df['Households_with_Electric_Lighting'].mean()))
df['Households_with_Internet'] = df['Households_with_Internet'].fillna(round(df['Households_with_Internet'].mean()))
df['Households_with_Computer'] = df['Households_with_Computer'].fillna(round(df['Households_with_Computer'].mean()))
df['Households_Rural'] = df['Households_Rural'].fillna(round(df['Households_Rural'].mean()))
df['Households_Urban'] = df['Households_Urban'].fillna(round(df['Households_Urban'].mean()))
df['Households'] = df['Households'].fillna(round(df['Households'].mean()))
df['Below_Primary_Education'] = df['Below_Primary_Education'].fillna(round(df['Below_Primary_Education'].mean()))
df['Primary_Education'] = df['Primary_Education'].fillna(round(df['Primary_Education'].mean()))
df['Middle_Education'] = df['Middle_Education'].fillna(round(df['Middle_Education'].mean()))
df['Secondary_Education'] = df['Secondary_Education'].fillna(round(df['Secondary_Education'].mean()))
df['Higher_Education'] = df['Higher_Education'].fillna(round(df['Higher_Education'].mean()))
df['Graduate_Education'] = df['Graduate_Education'].fillna(round(df['Graduate_Education'].mean()))
df['Other_Education'] = df['Other_Education'].fillna(round(df['Other_Education'].mean()))
df['Literate_Education'] = df['Literate_Education'].fillna(round(df['Literate_Education'].mean()))
df['Illiterate_Education'] = df['Illiterate_Education'].fillna(round(df['Illiterate_Education'].mean()))
df['Young_and_Adult'] = df['Young_and_Adult'].fillna(round(df['Young_and_Adult'].mean()))
df['Middle_Aged'] = df['Middle_Aged'].fillna(round(df['Middle_Aged'].mean()))
df['Senior_Citizen'] = df['Senior_Citizen'].fillna(round(df['Senior_Citizen'].mean()))
df['Age_Not_Stated'] = df['Age_Not_Stated'].fillna(round(df['Age_Not_Stated'].mean()))
df['Households_with_Bicycle'] = df['Households_with_Bicycle'].fillna(round(df['Households_with_Bicycle'].mean()))
df['Households_with_Car_Jeep_Van'] = df['Households_with_Car_Jeep_Van'].fillna(round(df['Households_with_Car_Jeep_Van'].mean()))
df['Households_with_Radio_Transistor'] = df['Households_with_Radio_Transistor'].fillna(round(df['Households_with_Radio_Transistor'].mean()))
df['Households_with_Scooter_Motorcycle_Moped'] = df['Households_with_Scooter_Motorcycle_Moped'].fillna(round(df['Households_with_Scooter_Motorcycle_Moped'].mean()))
df['HH_Phone_Landline'] = df['HH_Phone_Landline'].fillna(round(df['HH_Phone_Landline'].mean()))
df['HH_Phone_Mobile'] = df['HH_Phone_Mobile'].fillna(round(df['HH_Phone_Mobile'].mean()))
df['HH_TV_Computer_Phone_Vehicle'] = df['HH_TV_Computer_Phone_Vehicle'].fillna(round(df['HH_TV_Computer_Phone_Vehicle'].mean()))
df['Households_with_Television'] = df['Households_with_Television'].fillna(round(df['Households_with_Television'].mean()))
df['Households_with_Telephone_Mobile_Phone'] = df['Households_with_Telephone_Mobile_Phone'].fillna(round(df['Households_with_Telephone_Mobile_Phone'].mean()))
df['Households_with_Telephone_Mobile_Phone_Both'] = df['Households_with_Telephone_Mobile_Phone_Both'].fillna(round(df['Households_with_Telephone_Mobile_Phone_Both'].mean()))
df['Dilapidated_Census_Houses'] = df['Dilapidated_Census_Houses'].fillna(round(df['Dilapidated_Census_Houses'].mean()))
df['HH_Separate_Kitchen'] = df['HH_Separate_Kitchen'].fillna(round(df['HH_Separate_Kitchen'].mean()))
df['Having_bathing_facility_Total_Households'] = df['Having_bathing_facility_Total_Households'].fillna(round(df['Having_bathing_facility_Total_Households'].mean()))
df['Having_latrine_facility_within_the_premises_Total_Households'] = df['Having_latrine_facility_within_the_premises_Total_Households'].fillna(round(df['Having_latrine_facility_within_the_premises_Total_Households'].mean()))
df['Ownership_Owned_Households'] = df['Ownership_Owned_Households'].fillna(round(df['Ownership_Owned_Households'].mean()))
df['Ownership_Rented_Households'] = df['Ownership_Rented_Households'].fillna(round(df['Ownership_Rented_Households'].mean()))
df['Type_of_bathing_facility_Enclosure_without_roof_Households'] = df['Type_of_bathing_facility_Enclosure_without_roof_Households'].fillna(round(df['Type_of_bathing_facility_Enclosure_without_roof_Households'].mean()))
df['Type_of_fuel_used_for_cooking_Any_other_Households'] = df['Type_of_fuel_used_for_cooking_Any_other_Households'].fillna(round(df['Type_of_fuel_used_for_cooking_Any_other_Households'].mean()))
df['Type_of_latrine_facility_Pit_latrine_Households'] = df['Type_of_latrine_facility_Pit_latrine_Households'].fillna(round(df['Type_of_latrine_facility_Pit_latrine_Households'].mean()))
df['Type_of_latrine_facility_Other_latrine_Households'] = df['Type_of_latrine_facility_Other_latrine_Households'].fillna(round(df['Type_of_latrine_facility_Other_latrine_Households'].mean()))
df['Latrine_Night_Soil_Open_Drain'] = df['Latrine_Night_Soil_Open_Drain'].fillna(round(df['Latrine_Night_Soil_Open_Drain'].mean()))
df['Flush_Latrine_System'] = df['Flush_Latrine_System'].fillna(round(df['Flush_Latrine_System'].mean()))
df['Not_having_bathing_facility_within_the_premises_Total_Households'] = df['Not_having_bathing_facility_within_the_premises_Total_Households'].fillna(round(df['Not_having_bathing_facility_within_the_premises_Total_Households'].mean()))
df['No_Latrine_Alt_Source'] = df['No_Latrine_Alt_Source'].fillna(round(df['No_Latrine_Alt_Source'].mean()))
df['Main_source_of_drinking_water_Un_covered_well_Households'] = df['Main_source_of_drinking_water_Un_covered_well_Households'].fillna(round(df['Main_source_of_drinking_water_Un_covered_well_Households'].mean()))
df['Main_Water_Source_Handpump'] = df['Main_Water_Source_Handpump'].fillna(round(df['Main_Water_Source_Handpump'].mean()))
df['Main_source_of_drinking_water_Spring_Households'] = df['Main_source_of_drinking_water_Spring_Households'].fillna(round(df['Main_source_of_drinking_water_Spring_Households'].mean()))
df['Main_source_of_drinking_water_River_Canal_Households'] = df['Main_source_of_drinking_water_River_Canal_Households'].fillna(round(df['Main_source_of_drinking_water_River_Canal_Households'].mean()))
df['Main_source_of_drinking_water_Other_sources_Households'] = df['Main_source_of_drinking_water_Other_sources_Households'].fillna(round(df['Main_source_of_drinking_water_Other_sources_Households'].mean()))
df['Main_Water_Source_Other'] = df['Main_Water_Source_Other'].fillna(round(df['Main_Water_Source_Other'].mean()))
df['Water_Location_Near'] = df['Water_Location_Near'].fillna(round(df['Water_Location_Near'].mean()))
df['Water_Location_Within_Premises'] = df['Water_Location_Within_Premises'].fillna(round(df['Water_Location_Within_Premises'].mean()))
df['Main_Water_Source_Tank'] = df['Main_Water_Source_Tank'].fillna(round(df['Main_Water_Source_Tank'].mean()))
df['Main_source_of_drinking_water_Tapwater_Households'] = df['Main_source_of_drinking_water_Tapwater_Households'].fillna(round(df['Main_source_of_drinking_water_Tapwater_Households'].mean()))
df['Main_source_of_drinking_water_Tubewell_Borehole_Households'] = df['Main_source_of_drinking_water_Tubewell_Borehole_Households'].fillna(round(df['Main_source_of_drinking_water_Tubewell_Borehole_Households'].mean()))
df['Household_size_1_person_Households'] = df['Household_size_1_person_Households'].fillna(round(df['Household_size_1_person_Households'].mean()))
df['Household_size_2_persons_Households'] = df['Household_size_2_persons_Households'].fillna(round(df['Household_size_2_persons_Households'].mean()))
df['Household_size_4_persons_Households'] = df['Household_size_4_persons_Households'].fillna(round(df['Household_size_4_persons_Households'].mean()))
df['Household_size_5_persons_Households'] = df['Household_size_5_persons_Households'].fillna(round(df['Household_size_5_persons_Households'].mean()))
df['Household_size_6_8_persons_Households'] = df['Household_size_6_8_persons_Households'].fillna(round(df['Household_size_6_8_persons_Households'].mean()))
df['Household_size_9_persons_and_above_Households'] = df['Household_size_9_persons_and_above_Households'].fillna(round(df['Household_size_9_persons_and_above_Households'].mean()))
df['Location_of_drinking_water_source_Away_Households'] = df['Location_of_drinking_water_source_Away_Households'].fillna(round(df['Location_of_drinking_water_source_Away_Households'].mean()))
df['Married_couples_1_Households'] = df['Married_couples_1_Households'].fillna(round(df['Married_couples_1_Households'].mean()))
df['Married_couples_2_Households'] = df['Married_couples_2_Households'].fillna(round(df['Married_couples_2_Households'].mean()))
df['Married_couples_3_Households'] = df['Married_couples_3_Households'].fillna(round(df['Married_couples_3_Households'].mean()))
df['Married_couples_3_or_more_Households'] = df['Married_couples_3_or_more_Households'].fillna(round(df['Married_couples_3_or_more_Households'].mean()))
df['Married_couples_4_Households'] = df['Married_couples_4_Households'].fillna(round(df['Married_couples_4_Households'].mean()))
df['Married_couples_5_Households'] = df['Married_couples_5_Households'].fillna(round(df['Married_couples_5_Households'].mean()))
df['Married_couples_None_Households'] = df['Married_couples_None_Households'].fillna(round(df['Married_couples_None_Households'].mean()))
df['Power_Parity_Less_than_Rs_45000'] = df['Power_Parity_Less_than_Rs_45000'].fillna(round(df['Power_Parity_Less_than_Rs_45000'].mean()))
df['Power_Parity_Rs_45000_90000'] = df['Power_Parity_Rs_45000_90000'].fillna(round(df['Power_Parity_Rs_45000_90000'].mean()))
df['Power_Parity_Rs_90000_150000'] = df['Power_Parity_Rs_90000_150000'].fillna(round(df['Power_Parity_Rs_90000_150000'].mean()))
df['Power_Parity_Rs_150000_240000'] = df['Power_Parity_Rs_150000_240000'].fillna(round(df['Power_Parity_Rs_150000_240000'].mean()))
df['Power_Parity_Rs_240000_330000'] = df['Power_Parity_Rs_240000_330000'].fillna(round(df['Power_Parity_Rs_240000_330000'].mean()))
df['Power_Parity_Rs_330000_425000'] = df['Power_Parity_Rs_330000_425000'].fillna(round(df['Power_Parity_Rs_330000_425000'].mean()))
df['Power_Parity_Rs_425000_545000'] = df['Power_Parity_Rs_425000_545000'].fillna(round(df['Power_Parity_Rs_425000_545000'].mean()))
df['Power_Parity_Above_Rs_545000'] = df['Power_Parity_Above_Rs_545000'].fillna(round(df['Power_Parity_Above_Rs_545000'].mean()))
df['Household_size_1_to_2_persons'] = df['Household_size_1_to_2_persons'].fillna(round(df['Household_size_1_to_2_persons'].mean()))
df['Household_size_3_persons_Households'] = df['Household_size_3_persons_Households'].fillna(round(df['Household_size_3_persons_Households'].mean()))
df['Household_size_3_to_5_persons_Households'] = df['Household_size_3_to_5_persons_Households'].fillna(round(df['Household_size_3_to_5_persons_Households'].mean()))
df['Power_Parity_Rs_45000_150000'] = df['Power_Parity_Rs_45000_150000'].fillna(round(df['Power_Parity_Rs_45000_150000'].mean()))
df['Power_Parity_Rs_150000_330000'] = df['Power_Parity_Rs_150000_330000'].fillna(round(df['Power_Parity_Rs_150000_330000'].mean()))
df['Power_Parity_Rs_330000_545000'] = df['Power_Parity_Rs_330000_545000'].fillna(round(df['Power_Parity_Rs_330000_545000'].mean()))

# Filling datas with analogy
df['Total_Education'] = df['Total_Education'].fillna(df['Literate_Education'] + df['Illiterate_Education'])
df['Total_Power_Parity'] = df['Power_Parity_Less_than_Rs_45000'] + df['Power_Parity_Rs_45000_150000'] + df['Power_Parity_Rs_150000_330000'] + df['Power_Parity_Rs_330000_545000'] + df['Power_Parity_Above_Rs_545000']

After_cleaning=pd.DataFrame(df.isnull().mean()*100)   #Storing the null percentage in the dataset after cleaning for comparison purpose

# Comparing the dataframes before and after data cleaning and storing it in a csv file
Before_cleaning.reset_index(inplace=True)
After_cleaning.reset_index(inplace=True)
Comparison=Before_cleaning.merge(After_cleaning,on='index')
Comparison.columns=['Column_Name','Before_Cleaning','After_Cleaning']
Comparison.to_csv('Comparison_After_Cleaning.csv',index=False)

# To visualize the census_2011_Dataset after cleaning 
file_name = "Actual_census_data_after_cleaning.xlsx"
df.to_excel(file_name, index=False)

# Save the processed data to MongoDB in the collection named "census"
Mongo_Uri = credentials.MONGO_URI
client=MongoClient(Mongo_Uri)
print("connected to MongoDB successfully")
db=client['MINI_PROJECT']
collection=db['census']
print("Collection named census created")

# Insert data into the collection
collection.insert_many(df.to_dict('records'))

# Fetch data from MongoDB and upload to a relational database using mysql.connector
mongo_read = list(collection.find())
mongo_df = pd.DataFrame(mongo_read)
mongo_df.drop('_id',axis=1,inplace=True)

print("check1111")
# Connect to the MySQL database
c=credentials.mysql_credentials
connection = mysql.connector.connect(**c)
cursor = connection.cursor()

DDL= '''
CREATE TABLE IF NOT EXISTS census (
    District_Code VARCHAR(100) PRIMARY KEY,
    State_UT VARCHAR(100),
    District VARCHAR(100),
    Population VARCHAR(100),
    Male VARCHAR(100),
    Female VARCHAR(100),
    Literate VARCHAR(100),
    Literate_Male VARCHAR(100),
    Literate_Female VARCHAR(100),
    SC VARCHAR(100),
    Male_SC VARCHAR(100),
    Female_SC VARCHAR(100),
    ST VARCHAR(100),
    Male_ST VARCHAR(100),
    Female_ST VARCHAR(100),
    Workers VARCHAR(100),
    Male_Workers VARCHAR(100),
    Female_Workers VARCHAR(100),
    Main_Workers VARCHAR(100),
    Marginal_Workers VARCHAR(100),
    Non_Workers VARCHAR(100),
    Cultivator_Workers VARCHAR(100),
    Agricultural_Workers VARCHAR(100),
    Household_Workers VARCHAR(100),
    Other_Workers VARCHAR(100),
    Hindus VARCHAR(100),
    Muslims VARCHAR(100),
    Christians VARCHAR(100),
    Sikhs VARCHAR(100),
    Buddhists VARCHAR(100),
    Jains VARCHAR(100),
    Others_Religions VARCHAR(100),
    Religion_Not_Stated VARCHAR(100),
    LPG_or_PNG_Households VARCHAR(100),
    Households_with_Electric_Lighting VARCHAR(100),
    Households_with_Internet VARCHAR(100),
    Households_with_Computer VARCHAR(100),
    Households_Rural VARCHAR(100),
    Households_Urban VARCHAR(100),
    Households VARCHAR(100),
    Below_Primary_Education VARCHAR(100),
    Primary_Education VARCHAR(100),
    Middle_Education VARCHAR(100),
    Secondary_Education VARCHAR(100),
    Higher_Education VARCHAR(100),
    Graduate_Education VARCHAR(100),
    Other_Education VARCHAR(100),
    Literate_Education VARCHAR(100),
    Illiterate_Education VARCHAR(100),
    Total_Education VARCHAR(100),
    Young_and_Adult VARCHAR(100),
    Middle_Aged VARCHAR(100),
    Senior_Citizen VARCHAR(100),
    Age_Not_Stated VARCHAR(100),
    Households_with_Bicycle VARCHAR(100),
    Households_with_Car_Jeep_Van VARCHAR(100),
    Households_with_Radio_Transistor VARCHAR(100),
    Households_with_Scooter_Motorcycle_Moped VARCHAR(100),
    HH_Phone_Landline VARCHAR(100),
    HH_Phone_Mobile VARCHAR(100),
    HH_TV_Computer_Phone_Vehicle VARCHAR(100),
    Households_with_Television VARCHAR(100),
    Households_with_Telephone_Mobile_Phone VARCHAR(100),
    Households_with_Telephone_Mobile_Phone_Both VARCHAR(100),
    Dilapidated_Census_Houses VARCHAR(100),
    HH_Separate_Kitchen VARCHAR(100),
    Having_bathing_facility_Total_Households VARCHAR(100),
    Having_latrine_facility_within_the_premises_Total_Households VARCHAR(100),
    Ownership_Owned_Households VARCHAR(100),
    Ownership_Rented_Households VARCHAR(100),
    Type_of_bathing_facility_Enclosure_without_roof_Households VARCHAR(100),
    Type_of_fuel_used_for_cooking_Any_other_Households VARCHAR(100),
    Type_of_latrine_facility_Pit_latrine_Households VARCHAR(100),
    Type_of_latrine_facility_Other_latrine_Households VARCHAR(100),
    Latrine_Night_Soil_Open_Drain VARCHAR(100),
    Flush_Latrine_System VARCHAR(100),
    Not_having_bathing_facility_within_the_premises_Total_Households VARCHAR(100),
    No_Latrine_Alt_Source VARCHAR(100),
    Main_source_of_drinking_water_Un_covered_well_Households VARCHAR(100),
    Main_Water_Source_Handpump VARCHAR(100),
    Main_source_of_drinking_water_Spring_Households VARCHAR(100),
    Main_source_of_drinking_water_River_Canal_Households VARCHAR(100),
    Main_source_of_drinking_water_Other_sources_Households VARCHAR(100),
    Main_Water_Source_Other VARCHAR(100),
    Water_Location_Near VARCHAR(100),
    Water_Location_Within_Premises VARCHAR(100),
    Main_Water_Source_Tank VARCHAR(100),
    Main_source_of_drinking_water_Tapwater_Households VARCHAR(100),
    Main_source_of_drinking_water_Tubewell_Borehole_Households VARCHAR(100),
    Household_size_1_person_Households VARCHAR(100),
    Household_size_2_persons_Households VARCHAR(100),
    Household_size_1_to_2_persons VARCHAR(100),
    Household_size_3_persons_Households VARCHAR(100),
    Household_size_3_to_5_persons_Households VARCHAR(100),
    Household_size_4_persons_Households VARCHAR(100),
    Household_size_5_persons_Households VARCHAR(100),
    Household_size_6_8_persons_Households VARCHAR(100),
    Household_size_9_persons_and_above_Households VARCHAR(100),
    Location_of_drinking_water_source_Away_Households VARCHAR(100),
    Married_couples_1_Households VARCHAR(100),
    Married_couples_2_Households VARCHAR(100),
    Married_couples_3_Households VARCHAR(100),
    Married_couples_3_or_more_Households VARCHAR(100),
    Married_couples_4_Households VARCHAR(100),
    Married_couples_5_Households VARCHAR(100),
    Married_couples_None_Households VARCHAR(100),
    Power_Parity_Less_than_Rs_45000 VARCHAR(100),
    Power_Parity_Rs_45000_90000 VARCHAR(100),
    Power_Parity_Rs_90000_150000 VARCHAR(100),
    Power_Parity_Rs_45000_150000 VARCHAR(100),
    Power_Parity_Rs_150000_240000 VARCHAR(100),
    Power_Parity_Rs_240000_330000 VARCHAR(100),
    Power_Parity_Rs_150000_330000 VARCHAR(100),
    Power_Parity_Rs_330000_425000 VARCHAR(100),
    Power_Parity_Rs_425000_545000 VARCHAR(100),
    Power_Parity_Rs_330000_545000 VARCHAR(100),
    Power_Parity_Above_Rs_545000 VARCHAR(100),
    Total_Power_Parity VARCHAR(100)
);
'''
#create a Table in the relational database
cursor.execute(DDL)  

columns = df.columns.tolist()

print("check222")
# Insert data row by row
try:
    for index, row in df.iterrows():
        check_query = f"SELECT COUNT(*) FROM census WHERE District_Code = '{row['District_Code']}';"
        cursor.execute(check_query)
        count = cursor.fetchone()[0]  # Get count result

        if count == 0:  # Only insert if District_Code is not already present
            values = ', '.join([f"'{row[col]}'" for col in columns])
            query = f"INSERT INTO census ({', '.join(columns)}) VALUES ({values});"
            cursor.execute(query)
    
    # Commit the transaction
    connection.commit()

except Exception as e:
    print(f"An error occurred: {e}")
    connection.rollback()  # Rollback in case of an error
    
# Define the queries
queries = {
    "Total population of each district": 
        "SELECT District, Population FROM census;",
    
    "Literate males and females in each district": 
        "SELECT District, ROUND(Literate_Male), ROUND(Literate_Female) FROM census;",
    
    "Percentage of workers (both male and female) in each district": 
        "SELECT District,"
        "ROUND((CAST(Male_Workers AS FLOAT) / CAST(Population AS FLOAT)) * 100, 2) AS Male_Worker_Percentage,"
        "ROUND((CAST(Female_Workers AS FLOAT) / CAST(Population AS FLOAT)) * 100, 2) AS Female_Worker_Percentage "
        "FROM census;",
    
    "Households with access to LPG or PNG as a cooking fuel in each district": 
        "SELECT District, LPG_or_PNG_Households FROM census;",
    
    "Religious composition (Hindus, Muslims, Christians, etc.) in each district": 
        "SELECT District,"
        "ROUND(((CAST(Hindus AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Hindu_Percentage,"
        "ROUND(((CAST(Muslims AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Muslim_Percentage,"
        "ROUND(((CAST(Christians AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Christian_Percentage,"
        "ROUND(((CAST(Sikhs AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Sikh_Percentage,"
        "ROUND(((CAST(Buddhists AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Buddhist_Percentage,"
        "ROUND(((CAST(Jains AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Jain_Percentage,"
        "ROUND(((CAST(Others_Religions AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Other_Religion_Percentage,"
        "ROUND(((CAST(Religion_Not_Stated AS FLOAT) / CAST(Population AS FLOAT)) * 100), 2) AS Religion_Not_Stated_Percentage "
        "FROM census;",
    
    "Households with internet access in each district": 
        "SELECT District,Households_with_Internet from census order by Households_with_Internet desc ;",
    
    "Educational attainment distribution in each district": 
        "SELECT District,"
        "ROUND(((CAST(Below_Primary_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Below_Primary_Percentage,"
        "ROUND(((CAST(Primary_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Primary_Percentage,"
        "ROUND(((CAST(Middle_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Middle_Percentage,"
        "ROUND(((CAST(Secondary_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Secondary_Percentage,"
        "ROUND(((CAST(Higher_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Higher_Secondary_Percentage,"
        "ROUND(((CAST(Graduate_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Graduate_Percentage,"
        "ROUND(((CAST(Other_Education AS FLOAT) / CAST(Literate_Education AS FLOAT)) * 100), 2) AS Other_Education_Percentage "
        "FROM census;",
    
    "Households with access to various modes of transportation in each district": 
        "SELECT District,Households_with_Bicycle,Households_with_Car_Jeep_Van,Households_with_Scooter_Motorcycle_Moped,HH_TV_Computer_Phone_Vehicle FROM census;",
    
    "Condition of occupied census houses in each district": 
        "SELECT Dilapidated_Census_Houses,"
        "HH_Separate_Kitchen,Having_bathing_facility_Total_Households,Having_latrine_facility_within_the_premises_Total_Households,"
        "Ownership_Owned_Households,Ownership_Rented_Households,Type_of_bathing_facility_Enclosure_without_roof_Households,"
        "Type_of_fuel_used_for_cooking_Any_other_Households,Type_of_latrine_facility_Pit_latrine_Households,Type_of_latrine_facility_Other_latrine_Households,"
        "Latrine_Night_Soil_Open_Drain,Flush_Latrine_System,Not_having_bathing_facility_within_the_premises_Total_Households,"
        "No_Latrine_Alt_Source FROM census;",
    
    "Household size distribution in each district": 
        "SELECT District,Household_size_1_person_Households,Household_size_2_persons_Households,"
        "Household_size_1_to_2_persons,Household_size_3_persons_Households,Household_size_4_persons_Households,"
        "Household_size_5_persons_Households,Household_size_3_to_5_persons_Households,"
        "Household_size_6_8_persons_Households,Household_size_9_persons_and_above_Households from census;",
    
    "Total number of households in each state": 
        "Select State_Ut,SUM(Households) as Total_Households_In_State from census group by State_UT order by Total_Households_In_State desc;",
    
    "Households with a latrine facility within the premises in each state": 
        "Select State_Ut,SUM(Having_latrine_facility_within_the_premises_Total_Households) as Households_with_latrine_within_premises " 
        "from census group by State_UT order by Households_with_latrine_within_premises desc;",
    
    "Average household size in each state": 
        "SELECT State_UT,ROUND(SUM(CAST(Population AS FLOAT)) / SUM(CAST(Households AS FLOAT)), 2) AS Average_Household_Size_For_State "
        "FROM census GROUP BY State_UT ORDER BY Average_Household_Size_For_State desc;",
    
    "Owned versus rented households in each state": 
        "Select State_Ut,SUM(Ownership_Owned_Households) AS Owned_Households ,SUM(Ownership_Rented_Households) AS  Rented_Households from census" 
        " group by State_UT order by State_UT ;",
    
    "Distribution of different types of latrine facilities in each state": 
        "SELECT State_UT,SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_Latrine,SUM(Type_of_latrine_facility_Other_latrine_Households) AS Other_Latrine,"
        "SUM(Latrine_Night_Soil_Open_Drain) AS Night_Soil_Disposed_Latrine,SUM(Flush_Latrine_System) AS Flush_Latrine,"
        "SUM(No_Latrine_Alt_Source) AS Alternate_Source_Latrine FROM census GROUP BY State_UT ORDER BY State_UT;",
    
    "Households with access to drinking water sources near the premises in each state": 
        "Select State_Ut,SUM(Water_Location_Near) AS Households_near_drinking_water from census group by State_UT order by State_UT;",
    
    "Average household income distribution in each state based on power parity categories": 
        "SELECT State_UT,ROUND((SUM(Power_Parity_Less_than_Rs_45000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_Less_than_45000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_45000_90000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_45000_90000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_90000_150000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_90000_150000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_150000_240000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_150000_240000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_240000_330000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_240000_330000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_330000_425000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_330000_425000_Percentage,"
        "ROUND((SUM(Power_Parity_Rs_425000_545000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_425000_545000_Percentage,"
        "ROUND((SUM(Power_Parity_Above_Rs_545000) / SUM(Total_Power_Parity)) * 100, 2) AS Income_Above_545000_Percentage "
        "FROM census GROUP BY State_UT ORDER BY State_UT;",

    
    "Percentage of married couples with different household sizes in each state": 
        "SELECT State_UT,ROUND((SUM(Married_couples_1_Households) / SUM(Households)) * 100, 2) AS Married_1_Couple_Percentage,"
        "ROUND((SUM(Married_couples_2_Households) / SUM(Households)) * 100, 2) AS Married_2_Couples_Percentage,"
        "ROUND((SUM(Married_couples_3_Households) / SUM(Households)) * 100, 2) AS Married_3_Couples_Percentage,"
        "ROUND((SUM(Married_couples_4_Households) / SUM(Households)) * 100, 2) AS Married_4_Couples_Percentage,"
        "ROUND((SUM(Married_couples_5_Households) / SUM(Households)) * 100, 2) AS Married_5_Couples_Percentage "
        "FROM census GROUP BY State_UT ORDER BY State_UT;",
    
    "Households below the poverty line in each state based on power parity categories": 
        "SELECT State_UT,SUM(Power_Parity_Less_than_Rs_45000) AS Below_45000_Households,SUM(Power_Parity_Rs_45000_90000) AS Between_45000_90000_Households,"
        "SUM(Power_Parity_Less_than_Rs_45000 + Power_Parity_Rs_45000_90000) AS Total_Households_Below_Poverty,"
        "ROUND((SUM(Power_Parity_Less_than_Rs_45000 + Power_Parity_Rs_45000_90000) / SUM(Total_Power_Parity)) * 100, 2) AS Poverty_Percentage "
        "FROM census GROUP BY State_UT ORDER BY Poverty_Percentage DESC;",
    
    "Overall literacy rate in each state": 
"SELECT State_UT, ROUND((SUM(Literate) / SUM(Population)) * 100, 2) AS Literacy_Percentage"
" FROM census"
" GROUP BY State_UT"
" ORDER BY Literacy_Percentage DESC;"

}

# Streamlit app title
st.title("Census Data Standardization and Analysis Pipeline: From Cleaning to Visualization")

# Sidebar for navigation  
task = st.sidebar.selectbox("Select Task", [
    "Total population of each district",
    "Literate males and females in each district",
    "Percentage of workers (both male and female) in each district",
    "Households with access to LPG or PNG as a cooking fuel in each district",
    "Religious composition (Hindus, Muslims, Christians, etc.) in each district",
    "Households with internet access in each district",
    "Educational attainment distribution in each district",
    "Households with access to various modes of transportation in each district",
    "Condition of occupied census houses in each district",
    "Household size distribution in each district",
    "Total number of households in each state",
    "Households with a latrine facility within the premises in each state",
    "Average household size in each state",
    "Owned versus rented households in each state",
    "Distribution of different types of latrine facilities in each state",
    "Households with access to drinking water sources near the premises in each state",
    "Average household income distribution in each state based on power parity categories",
    "Percentage of married couples with different household sizes in each state",
    "Households below the poverty line in each state based on power parity categories",
    "Overall literacy rate in each state"
])

# Function to execute and display query results
def display_query_results(query):
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    st.dataframe(df)

# Execute and display the selected query
st.subheader(task)
display_query_results(queries[task])

# Close the cursor and connection after all tasks are done
if st.button("Close Connection"):
    cursor.close()
    connection.close()
    st.write("Connection closed")       