import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder

def cleaning(path):
    '''
    Cleaning dataset
    Input:
        path: str, input file path
    Return:
        None, cleaned data exported to current directory
    '''
    
    df = pd.read_csv(path)
    
    # Filter out cancelled flight, duplicate record. Drop 'cancelled', 'dup', 'icao24'
    filters = (df['cancelled']==0) & (df['dup']=='N')
    df = df[filters].drop(['cancelled', 'dup', 'icao24'], axis=1)
    
    # Fill null values for 'weather_delay', 'late_aircraft_delay', 'dep_delay' with 0             note: after check, null for dep_delay means airplane departured on time.
    df[['dep_delay', 'weather_delay', 'late_aircraft_delay']] = df[['dep_delay', 'weather_delay', 'late_aircraft_delay']].fillna(0.0)
    
    # Drop rows that contain null values in 'arr_time'
    df.dropna(subset=['arr_time'], inplace=True)
    
    # Fill null values in 'arr_delay' by substracting 'crs_arr_time' from 'arr_time' 
    # (condition applied to adjust for hhmm format and arrive in next day issue)
    to_min = lambda x: x.astype(str).str[:-2].str.zfill(4).str[:2].astype(int) * 60 + x.astype(str).str[:-2].str.zfill(4).str[2:].astype(int)
    df['arr_delay'] = df['arr_delay'].fillna(to_min(df['arr_time'])+24*60 - to_min(df['crs_arr_time'])\
                                         if (df['arr_time'][0]<500 and df['crs_arr_time'][0]>2300) \
                                         else (to_min(df['arr_time']) - to_min(df['crs_arr_time'])))
    
    # Combine same brand of manufactures
    manufactures = {
        'Embraer S A': 'Embraer',
        'Embraer-empresa Brasileira De': 'Embraer',
        'Mcdonnell Douglas Corporation': 'Mcdonnell Douglas',
        'Mcdonnell Douglas Aircraft Co': 'Mcdonnell Douglas',
        'Airbus Industrie': 'Airbus',
        'Boeing Of Canada/dehav Div': 'Boeing',
        'Saab-scania': 'Saab',
        'Gulfstream Aerospace Corp': 'Gulfstream Aerospace',
        'Bombardier Inc': 'Bombardier'
    }

    df['manufacturername'].replace(manufactures, inplace=True)
    
    # One hot encode the 'manufacturername'
    enc = OneHotEncoder(sparse=False, handle_unknown='ignore')
    encoded = enc.fit_transform(df['manufacturername'].to_numpy().reshape(-1,1))
    ohe_df = pd.DataFrame(encoded, columns=enc.get_feature_names(['man']))
    df = pd.concat([df.reset_index(), ohe_df.reset_index()], axis=1).drop(['index', 'manufacturername', 'man_nan'], axis=1)
    
    # Create 'aircraft_age' and fillna with mean value
    df['aircraft_age'] = df['fl_date'].str[:4].astype(float) - df['built'].str[:4].astype(float)
    df['aircraft_age'] = df['aircraft_age'].fillna(int(df['aircraft_age'].mean()))
    df['aircraft_age'] = df['aircraft_age'].astype(int)
    
    # Drop 'built'
    df.drop(['built'], axis=1, inplace=True)
    
    # Print out all columns after data cleaning
    print('Columns after data cleaning\n')
    print(df.columns)
    
    # Final check if there is any null values exist
    print(f'\nIs there any null values now: {df.isnull().any().any()}')
    
    # Export cleaned data
    file_name = path.split('/')[-1]
    df.to_csv(f'cleaned_{file_name}', index=None)