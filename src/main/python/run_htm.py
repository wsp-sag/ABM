# -*- coding: utf-8 -*-
"""
Heavy Truck Model
FAF Annual Tonnage to Truck Number by TAZ Disaggregation

Author: Maddie Hasani, Fehr & Peers <br/>
Reviewer: Fatemeh Ranaiefar, Fehr & Peers<br/>
Last update: 12/19/2023
"""

## REQUIRED LIBRARIES
import pandas as pd
import numpy as np
import openmatrix as omx
from scipy import io
pd.set_option('display.max_columns', None)  # Display all columns
import sys
import shutil

#===================================================================================================================
#===================================================================================================================


## Functions
def load_and_preprocess_data(path, model_dir, output_dir, faf_file_name, mgra_file_name, skim_file_name):

    """
    Parameters:
    - Path: a full path to the input file. For example: 'C:\GitLab\HVM scripts\input_file.xlsx'

    input_file:
    - HTM input excel file

    Returns:
    - df_dict (dict): A dictionary containing DataFrames for each sheet in the input Excel file.
    - traffic_skims (DataFrame): Processed DataFrame containing traffic skims.

    Description:
    This function loads and preprocesses input files and returns DataFrames and processed traffic skims.

    """

    # Initialize a dictionary to hold DataFrames
    df_dict = {}

    # 1. Load in input files
    input_path = path
    inputs_sandag_HTM = pd.ExcelFile(input_path)
    sheet_names = [sheet_name for sheet_name in inputs_sandag_HTM.sheet_names if sheet_name.lower() not in ['userguide', 'reference']]

    # 1.1 Load all sheets into separate DataFrames with lowercase names
    for sheet_name in sheet_names:
        df_name = sheet_name.lower()  # Convert sheet name to lowercase
        df_dict[df_name] = inputs_sandag_HTM.parse(sheet_name)  # Save DataFrame to the dictionary

    # 2. Load in FAF data
    faf = df_dict['faf']  # Use the dictionary to get the DataFrame
    faf_name = faf_file_name
    faf_path = model_dir + "\input\htm"
    full_faf_path = f"{faf_path}\\{faf_name}"
    df = pd.read_csv(full_faf_path)

    # 3. Load in MGRA data
    mgra_loc = df_dict['mgra']  # Use the dictionary to get the DataFrame
    mgra_name = mgra_file_name
    #mgra_path = mgra_loc.loc[0, 'Path']
    full_mgra_path = f"{model_dir}\\{mgra_file_name}"
    mgra = pd.read_csv(full_mgra_path)

    # 4. Load in Skim file
    skim = df_dict['skim']  # Use the dictionary to get the DataFrame
    skim_name = skim_file_name
    skim_path = output_dir
    full_skim_path = f"{output_dir}\\skims\\{skim_name}.omx"

    traffic_skims_PM = []
    with omx.open_file(full_skim_path) as omx_file:
        # for name in ["PM_TRK_H_DIST"]:
        for name in ["TRK_H_DIST__PM"]:
            matrix = omx_file[name]
            df_skim = pd.DataFrame(matrix[:])
            stacked_df = df_skim.stack().reset_index()  # Reset the index to make it separate columns
            stacked_df.columns = ["origin", "destination", name]  # Rename the columns
            traffic_skims_PM.append(stacked_df)
    traffic_skims = pd.concat(traffic_skims_PM, axis=1)

    # 5. Filter out OD pairs where at least one end is a gateway
    traffic_skims = traffic_skims.loc[(traffic_skims['origin'] >= 13) & (traffic_skims['destination'] >= 13)]

    return df, mgra, traffic_skims, df_dict


#===================================================================================================================
#===================================================================================================================

def clean_faf(df, faz_gateway, faz_county, sd_flows, othermode_truck, commodity_group):
    """
    Process a DataFrame containing transportation data.

    Parameters:
    - df (DataFrame): The input DataFrame containing transportation data.
    - faz_gateway (DataFrame): DataFrame containing FAZ and Gateways/Airports/Ports TAZ.
    - faz_county (DataFrame): DataFrame containing FAZ and County information.
    - sd_flows (DataFrame): DataFrame containing Origin and Destination pair that will generate truck trips to/from SANDAG or Pass through SANDAG region.
    - othermode_truck (DataFrame): DataFrame with mode information and percentages.
    - commodity_group (DataFrame): DataFrame mapping commodities to CG values.

    Returns:
    - processed_df (DataFrame): Processed DataFrame with updated columns.

    Description:
    This function takes FAF data and performs the following steps:
    1. Determine what are the OD pairs that may pass through SANDAG region or start/end in SANDAG
    2. Includes only specified modes and calculates tonnage.
    3. Deletes unnecessary columns.
    4. Assigns SANDAG commodity groups based on SCTG commodity group.
    5. Aggregates tonnage data by OD FAZ and Commodity Group.
    6. Adds columns identifying if OD pairs have at least one end within Orange County or Mexico.
    The processed DataFrame is returned.
    """
    sd_flows = df_dict[sd_flows]
    faz_gateway = df_dict[faz_gateway]
    faz_county = df_dict[faz_county]
    othermode_truck = df_dict[othermode_truck]
    commodity_group = df_dict[commodity_group]


    # 1. Determine what are the OD pairs that may pass through SANDAG region or start/end in SANDAG
    # Mapping columns 'dms_orig' and 'dms_dest' based on the lookup table to determine the area code (check out the reference tab in the input spreadsheet for more info)
    df = df.merge(faz_gateway[['FAZ', 'AreaCode']], how='left', left_on='dms_orig', right_on='FAZ').drop('FAZ', axis=1).rename(columns={'AreaCode': 'code_orig'})
    df = df.merge(faz_gateway[['FAZ', 'AreaCode']], how='left', left_on='dms_dest', right_on='FAZ').drop('FAZ', axis=1).rename(columns={'AreaCode': 'code_dest'})
    # if FAZ is within SANDAG, the areacode should be 8
    faz_sandag = faz_county[faz_county["County"] == "San Diego"]["FAZ"]
    df.loc[df['dms_orig'].isin(faz_sandag), 'code_orig'] = 8
    df.loc[df['dms_dest'].isin(faz_sandag), 'code_dest'] = 8
    # merge the distribution of truck trips between each OD pair
    df = df.merge(sd_flows[['OriginCode', 'DestinationCode', 'Dist']], how='left', left_on=['code_orig', 'code_dest'], right_on=['OriginCode', 'DestinationCode']).drop(['OriginCode', 'DestinationCode'], axis=1)
    # calculate how much of each OD pairs will travel through SANDAG - multiply by the ton
    df['distons_2025'] = df['distons_2025'] * df['Dist']
    # remove all OD pairs that do not have any tons traveled through or within SANDAG
    df = df[df['distons_2025'] > 0]
    df.drop(['Dist'], axis=1, inplace=True)

    # 2. Include some modes
    mode_to_include = othermode_truck.set_index('Mode_Num')['Percentage'].to_dict()
    df = df[df['Mode'].isin(mode_to_include.keys())]
    df['truck_perc'] = df['Mode'].map(mode_to_include)
    df['ton'] = df['distons_2025'] * df['truck_perc']
    df.drop(['truck_perc', 'distons_2025'], axis=1, inplace=True)

    # 3. Delete unnecessary columns
    delete_col = ['Direction', 'Trade', 'disvalue_2017', 'distons_2017', 'disvalue_2025', 'distons_2030', 'disvalue_2030', 'distons_2035', 
                    'disvalue_2035', 'distons_2040', 'disvalue_2040', 'distons_2045', 'disvalue_2045', 'distons_2050', 'disvalue_2050'] 
    df.drop(delete_col, axis=1, inplace=True)

    # 4. Assign SANDAG commodity groups based on SCTG commodity group
    commodity_to_cg = commodity_group.set_index('SCTG')['CG'].to_dict()
    df['CG'] = df['Commodity'].map(commodity_to_cg)
    df.drop('Commodity', axis=1, inplace=True)

    # 5. Aggregate the Tonnage Data by Origin/Destination and Commodity Group
    df['fr_orig'] = df['fr_orig'].fillna(0)
    df['fr_dest'] = df['fr_dest'].fillna(0)
    df = df.groupby(['dms_orig', 'dms_dest', 'fr_orig', 'fr_dest', 'code_orig', 'code_dest', 'CG'], as_index=False).agg({'ton': 'sum'})

    # 6. Create a new column that identifies if at least one end of the OD is within Orange County and mexico
    df['one_end_orange'] = ((df['code_orig'] == 6) | (df['code_dest'] == 6)).astype(int)
    df['one_end_mx'] = ((df['fr_orig'].isin([802])) | (df['fr_dest'].isin([802]))).astype(int)                            

    return df

#===================================================================================================================
#===================================================================================================================

def prepare_taz_data(mgra, faz_county, emp_converter, taz_faz):
    """
    Prepare TAZ data by calculating NAICS employee category percentages within each TAZ.

    Parameters:
    - mgra (DataFrame): DataFrame containing MGRA data.
    - faz_county (DataFrame): DataFrame containing FAZ and County information.
    - emp_converter (DataFrame): DataFrame mapping SANDAG emp category to NAICS emp category.
    - taz_faz (DataFrame): DataFrame mapping TAZ values to FAZ values.

    Returns:
    - taz_long (DataFrame): Processed DataFrame with calculated employee category percentages.

    Description:
    This function calculates the percentage of each employee category (from the NAICS Dataset)
    within each TAZ. It performs the following steps:
    1. Calculate the number of SADNAG employees by TAZ.
    2. Add FAZ to the emp by TAZ table
    3. Reformat the TAZ table to long format using emp_ and pop columns.
    4. Reformat the emp_converter table to long format using FAZ columns
    5. Convert SANDAG emp category to NAICS emp category.
    6. Calculate the number of NAICS employees by TAZ.
    7. Aggregate total number of each NAICS employee category by TAZ.
    6. Create a mapping of TAZ values to FAZ values.
    8. Calculate how many percentage of each emp category is within a TAZ
    The processed DataFrame is returned.
    """

    emp_converter = df_dict[emp_converter]
    taz_faz = df_dict[taz_faz]
    faz_county = df_dict[faz_county]
    
    # 1. Calculate the number of employee by TAZ
    cols_to_sum = ['pop'] + [col for col in mgra.columns if col.startswith('emp_')]
    taz = mgra.groupby(['taz'], as_index=False)[cols_to_sum].sum()

    # 2. Add FAZ to the emp by TAZ table
    taz_to_faz = taz_faz.set_index('TAZ')['FAZ'].to_dict()
    # Use the map function to directly assign TAZ values
    taz['FAZ'] = taz['taz'].map(taz_to_faz)

    # 3. Reformat the taz table to long format using emp_ columns and pop
    taz_long = taz.melt(id_vars=['taz', 'FAZ'], value_vars=cols_to_sum, value_name='sandag_emp_num', var_name='sandag_emp')

    # 4. Reformat the emp_converter table to long format using FAZ columns
    faz_sandag = faz_county[faz_county["County"] == "San Diego"]["FAZ"]
    emp_converter_long = emp_converter.melt(id_vars=['NAICS_Emp', 'SANDAG_Emp'], value_vars=faz_sandag, value_name='Emp_PCT', var_name='FAZ')

    # 5. Convert SANDAG emp category to NAICS emp category
    taz_long = emp_converter_long.merge(taz_long, how='inner', right_on=['sandag_emp', 'FAZ'], left_on=['SANDAG_Emp', 'FAZ']).drop(['sandag_emp'], axis=1)

    # 6. Calculate the number of NAICS employees by TAZ.
    taz_long['naics_emp_num'] = taz_long['sandag_emp_num'] * taz_long['Emp_PCT']

    # 7. Aggregate NAICS employee number by TAZ
    taz_long = taz_long.groupby(['taz', 'FAZ', 'NAICS_Emp'], as_index=False).agg({'naics_emp_num': 'sum'})

    # 8. Calculate how many percentage of each emp category is within a TAZ
    taz_long['emp_naics_perc'] = taz_long['naics_emp_num'] / taz_long.groupby(['NAICS_Emp', 'FAZ'])['naics_emp_num'].transform('sum')
    taz_long = taz_long.loc[taz_long['naics_emp_num'] > 0]

    return taz_long

#===================================================================================================================
#===================================================================================================================

def faf_disaggregate_to_taz(df, faz_gateway, cg_emp_a, cg_emp_p, faz_county, taz, annual_factor):
    """
    Parameters:
    - df (DataFrame): faf data with aggregated commodity
    - faz_gateway (DataFrame): DataFrame mapping FAZ to SADNAG gateways.
    - faz_county (DataFrame): DataFrame containing FAZ and County information.

    Returns:
    - processed_df (DataFrame): Processed DataFrame with daily tonnage by TAZ origin and destination.

    Description:
    This function performs the following steps:
    1. Determine NAICS emp category for production and attraction in FAF.
    2. Bring TAZ numbers and the percentage of each emp category within each TAZ to relatively distribute tonnage to TAZ.
    3. For FAZ outside the SANDAG region, assume that the distribution of tonnage is 1 for both attraction/production.
    4. Calculate annual tonnage for each OD pair.
    5. Reformat the faz_gateway table to long format using emp_ columns and pop.
    6. Assign Gateway TAZ and percentage to the DataFrame.
    7. Calculate final tonnage where one end of trip is outside the SANDAG region (taz_a or taz_p is null) and assign corresponding gateways as taz_a or taz_p.
    8. Group by TAZ attraction and production and sum up the annual tonnage.
    9. Convert annual to daily tonnage . the annual to daily factor is basically the number of working days within a year.

    The processed DataFrame is returned.
    """
    faz_gateway = df_dict[faz_gateway]
    cg_emp_a = df_dict[cg_emp_a]
    cg_emp_p = df_dict[cg_emp_p]
    faz_county = df_dict[faz_county]
    annual_factor = df_dict[annual_factor]


    # 1. Determine NAICS emp category for production and attraction in FAF
    cg_to_emp_a = cg_emp_a.set_index('CG')['Emp_a'].to_dict()
    cg_to_emp_p = cg_emp_p.set_index('CG')['Emp_p'].to_dict()

    # Use the map function to directly assign Emp_a and Emp_p values
    df['Emp_a'] = df['CG'].map(cg_to_emp_a)
    df['Emp_p'] = df['CG'].map(cg_to_emp_p)
    
    # 2. Bring TAZ numbers and the percentage of each emp category within each TAZ to relatively distribute tonnage to TAZ.
    df = df.merge(taz[['taz', 'FAZ', 'NAICS_Emp', 'emp_naics_perc']], how='left', left_on=['Emp_p', 'dms_orig'], right_on=['NAICS_Emp', 'FAZ']).drop(['FAZ', 'NAICS_Emp', 'Emp_p'], axis=1)
    df.rename(columns={'taz': 'taz_p', 'emp_naics_perc': 'emp_naics_perc_p'}, inplace=True)

    df = df.merge(taz[['taz', 'FAZ', 'NAICS_Emp', 'emp_naics_perc']], how='left', left_on=['Emp_a', 'dms_dest'], right_on=['NAICS_Emp', 'FAZ']).drop(['FAZ', 'NAICS_Emp', 'Emp_a'], axis=1)
    df.rename(columns={'taz': 'taz_a', 'emp_naics_perc': 'emp_naics_perc_a'}, inplace=True)

    # 3. For FAZ outside the SANDAG region, assume that the distribution percentage is 1 for both attraction/production
    # create a list of FAZ in San Diego
    faz_sandag = faz_county[faz_county["County"] == "San Diego"]["FAZ"]
    df.loc[(~df['dms_orig'].isin(faz_sandag)) & (df['emp_naics_perc_p'].isnull()), 'emp_naics_perc_p'] = 1
    df.loc[(~df['dms_dest'].isin(faz_sandag)) & (df['emp_naics_perc_a'].isnull()), 'emp_naics_perc_a'] = 1
    
    # 4. Calculate annual tonnage for each OD pair
    df['dist_perc'] = df['emp_naics_perc_a'] * df['emp_naics_perc_p']
    df['ton_annual'] = df['ton'] * df['dist_perc'] * 1000  # FAF data is in thousand tons
    df.drop(['ton', 'emp_naics_perc_p', 'emp_naics_perc_a', 'dist_perc'], axis=1, inplace=True)
    
    # 5. Reformat the faz_gateway table to long format using emp_ columns and pop
    gateway_airport_port = faz_gateway.columns[4:]
    faz_gateway_long = faz_gateway.melt(id_vars=['FAZ'], value_vars=gateway_airport_port, value_name='faz_gtw_perc', var_name='gateways').dropna(subset=['faz_gtw_perc'])

    # 6. Assign Gateway TAZ and percentage to the DataFrame
    df = df.merge(faz_gateway_long, how='left', left_on='dms_orig', right_on='FAZ').drop('FAZ', axis=1)
    df.rename(columns={'gateways': 'gateways_p', 'faz_gtw_perc': 'faz_gtw_perc_p'}, inplace=True)

    df = df.merge(faz_gateway_long, how='left', left_on='dms_dest', right_on='FAZ').drop('FAZ', axis=1)
    df.rename(columns={'gateways': 'gateways_a', 'faz_gtw_perc': 'faz_gtw_perc_a'}, inplace=True)

    # 7. Calculate final tonnage 
    # For the end that is within SANDAG region, gateway distributions should be ignored 
    df.loc[(df['dms_orig'].isin(faz_sandag)) & (df['faz_gtw_perc_p'].isnull()), 'faz_gtw_perc_p'] = 1
    df.loc[(df['dms_dest'].isin(faz_sandag)) & (df['faz_gtw_perc_a'].isnull()), 'faz_gtw_perc_a'] = 1

    df['faz_gtw_perc'] = df['faz_gtw_perc_p'] * df['faz_gtw_perc_a']
    df['ton_tot'] = df['ton_annual'] * df['faz_gtw_perc']

    # df.loc[df['taz_a'].isnull(), 'ton_tot'] = df.loc[df['taz_a'].isnull(), 'ton_annual'] * df.loc[df['taz_a'].isnull(), 'faz_gtw_perc_a']
    df.loc[df['taz_a'].isnull(), 'taz_a'] = df.loc[df['taz_a'].isnull(), 'gateways_a']

    # df.loc[df['taz_p'].isnull(), 'ton_tot'] = df.loc[df['taz_p'].isnull(), 'ton_annual'] * df.loc[df['taz_p'].isnull(), 'faz_gtw_perc_p']
    df.loc[df['taz_p'].isnull(), 'taz_p'] = df.loc[df['taz_p'].isnull(), 'gateways_p']
    
    # 8. Group by TAZ attraction and production and sum up the annual tonnage
    # Note that there is a column that shows if at least one end of the trip is within Orange County. This column will be used in future steps to determine the OD distance.
    processed_df = df.groupby(['taz_a', 'taz_p', 'CG', 'one_end_orange', 'one_end_mx'], as_index=False).agg({'ton_tot': 'sum'})

    # 9. Convert annual to daily tonnage . the annual to daily factor is basically the number of working days within a year.
    annual_to_daily_factor = annual_factor['Factor'].values
    processed_df['ton_daily'] = processed_df['ton_tot'] / annual_to_daily_factor

    # Drop unnecessary columns
    processed_df.drop(['ton_tot'], axis=1, inplace=True)

    return processed_df


#===================================================================================================================
#===================================================================================================================

def daily_ton_to_truck_by_type_and_tod(df, traffic_skims, truck_dist, payload, time_of_day):
    """
    Convert daily tonnage to number of trucks by type and time of day.

    Parameters:
    - df (DataFrame): DataFrame containing the daily tonnage by TAZ.
    - traffic_skims (DataFrame): DataFrame containing OD pair distance data.
    - truck_dist (DataFrame): DataFrame containing truck type distribution data.
    - payload (DataFrame): DataFrame containing payload data for truck types.
    - time_of_day (DataFrame): DataFrame containing time of day factors.

    Returns:
    - final_df (DataFrame): Processed DataFrame with number of trucks by type and time of day (TOD).

    Description:
    This function performs the following steps:
    1. Identify distance between two OD pairs within San Diego using skim.
    2. Categorize the distance into different categories.
        1. less than 50 miles; 
        2. 51 to 100 miles ; 
        3. One end in OC ; 
        4. 201 miles or more; or one end outside of SANDAG and Orange county regions.
    3. Distribute daily tonnage between truck types based on OD distance.
    4. Convert tonnage by truck type to the number of trucks using truck type and commodity.
    5. Distribute the number of trucks by time of day.
    The final processed DataFrame is returned.
    """
    truck_dist = df_dict[truck_dist]
    payload = df_dict[payload]
    time_of_day = df_dict[time_of_day]

    # 1. Identify distance between two OD pairs within San Diego using skim
    df = df.merge(traffic_skims, how='left', left_on=['taz_a', 'taz_p'], right_on=['destination', 'origin']).drop(['origin', 'destination'], axis=1)

    # 2. Categorize the distance
    def categorize_dist(value):
        if value <= 50:
            return 1
        elif value <= 100:
            return 2
        elif value <= 150:
            return 3
        elif value > 200:
            return 4
        else:
            return 0

    df['dist_cat'] = np.where(df['one_end_orange'] == 1, 3, 0)
    df['dist_cat'] = np.where(df['one_end_mx'] == 1, 5, 0)
    # df['dist_cat'] = np.where(df['dist_cat'] == 0, df['PM_TRK_H_DIST'].apply(categorize_dist), df['dist_cat'])
    df['dist_cat'] = np.where(df['dist_cat'] == 0, df['TRK_H_DIST__PM'].apply(categorize_dist), df['dist_cat'])
    df['dist_cat'] = np.where(df['dist_cat'] == 0, 4, df['dist_cat'])
    # df.drop(['one_end_orange', 'PM_TRK_H_DIST'], axis=1, inplace=True)
    df.drop(['one_end_orange', 'TRK_H_DIST__PM'], axis=1, inplace=True)

    # 3. Distribute daily tonnage between truck types based on OD distance
    df = df.merge(truck_dist[['Dist_GP', 'Truck_Type', 'Dist']], how='left', left_on='dist_cat', right_on='Dist_GP').drop(['Dist_GP'], axis=1)
    df['ton_daily_bytruck'] = df['ton_daily'] * df['Dist']
    df.drop(['ton_daily', 'dist_cat', 'Dist'], axis=1, inplace=True)
    df = df.groupby(['taz_a', 'taz_p', 'CG', 'Truck_Type'], as_index=False)['ton_daily_bytruck'].sum()

    # 4. Convert tonnage by truck type to the number of trucks using truck type and commodity
    max_tonnage_dict = payload.set_index(['CG', 'Truck_Type'])['Pounds'].to_dict()
    df['Tonnage'] = df.apply(lambda row: max_tonnage_dict.get((row['CG'], row['Truck_Type']), 0), axis=1)
    df = df.loc[df['Tonnage'] > 0]
    df['tot_truck'] = (df['ton_daily_bytruck'] / df['Tonnage']) * 2000
    df.drop(['ton_daily_bytruck', 'Tonnage'], axis=1, inplace=True)

    # 5. Distribute the number of trucks by time of day
    peak_periods = ['AM', 'MD', 'PM', 'EA', 'EV']
    for period in peak_periods:
        factor_column = time_of_day.loc[time_of_day['Peak_Period'] == period, 'Factor'].values
        df[f'{period.lower()}_truck'] = df['tot_truck'] * factor_column

    final_df = df

    return final_df

#===================================================================================================================
#===================================================================================================================
#===================================================================================================================

## Main Script

arguments = sys.argv

model_dir = arguments[1]
output_dir = arguments[2]
#assignment_dir = arguments[3]
faf_file_name = arguments[3]
mgra_file_name = arguments[4]
skim_file_name = arguments[5]

# Step 1: Load and preprocess data
#df, mgra, traffic_skims, df_dict = load_and_preprocess_data("inputs_sandag_HTM.xlsx")
df, mgra, traffic_skims, df_dict = load_and_preprocess_data(model_dir + "\input\htm\inputs_sandag_HTM.xlsx", model_dir, output_dir, faf_file_name, mgra_file_name, skim_file_name)

# Step 2: Prepare TAZ data
taz = prepare_taz_data(mgra, 'faz_county', 'emp_converter', 'taz_faz')


# Process df in chunks
# chunk_size = 100000
chunk_size = 400000
num_chunks = len(df) // chunk_size + 1
final_results = []
final_ton = []

for chunk_num in range(num_chunks):
    start_idx = chunk_num * chunk_size
    end_idx = (chunk_num + 1) * chunk_size
    df_chunk = df[start_idx:end_idx]

    # Step 3: Clean FAF data
    df_chunk = clean_faf(df_chunk, 'faz_gateway', 'faz_county', 'sd_flows', 'othermode_truck', 'commodity_group')

    # Step 4: FAF disaggregation to TAZ
    df_chunk = faf_disaggregate_to_taz(df_chunk, 'faz_gateway', 'cg_emp_a', 'cg_emp_p', 'faz_county', taz, 'annual_factor')

    # Step 5: Daily tonnage to truck types and time of day
    final_chunk = daily_ton_to_truck_by_type_and_tod(df_chunk, traffic_skims, 'truck_dist', 'payload', 'time_of_day')
    
    # Append the results of the chunk to the final_results list
    final_results.append(final_chunk)
    final_ton.append(df_chunk)

    # Print progress
    progress = (chunk_num + 1) / num_chunks * 100
    print(f"Processing chunk {chunk_num + 1}/{num_chunks} - {progress:.2f}% done")


# Combine the results of all chunks
final_truck = pd.concat(final_results, ignore_index=True)
final_ton = pd.concat(final_ton, ignore_index=True)

#===================================================================================================================

# combine medium1 and 2 truck type to a medium type
htm = final_truck
htm.loc[htm['Truck_Type'] == "Medium1", 'Truck_Type'] = 'Medium'
htm.loc[htm['Truck_Type'] == "Medium2", 'Truck_Type'] = 'Medium'

htm = htm.groupby(['Truck_Type', 'taz_a', 'taz_p'], as_index=False)[['tot_truck', 'am_truck','md_truck', 'pm_truck', 'ea_truck', 'ev_truck']].sum()

#===================================================================================================================
# Identify the trip types: ei,ie (drop ii since it is not an HTM trip)
external = [1,2,3,4,5,6,7,8,9,10,11,12, 1154, 1294, 1338, 1457,1476,1485,1520, 2086,2193,2372,2384,2497,3693,4184]
htm['ei'] = 0
htm['ie'] = 0
htm.loc[(~htm.taz_a.isin(external)) & (htm.taz_p.isin(external)), 'ei'] = 1
htm.loc[(htm.taz_a.isin(external)) & (~htm.taz_p.isin(external)), 'ie'] = 1
# Define truck types and columns to process
truck_types = htm['Truck_Type'].unique()
columns_to_process = ['am_truck', 'md_truck', 'pm_truck', 'ea_truck', 'ev_truck']
conditions = ['ei', 'ie']

# Create empty matrices
matrix_size = 4947  #ABM3 zone system
#matrix_size = 4996
empty_matrix = np.zeros((matrix_size, matrix_size))

for column in columns_to_process:
    matrices = {}
    for condition in conditions:
        condition_matrices = {}
        for i, truck_type in enumerate(truck_types):
            filtered_data = htm[(htm[condition] == 1) & (htm['Truck_Type'] == truck_type)]
            filtered_data_group = filtered_data.groupby(['Truck_Type', 'taz_a', 'taz_p'], as_index=False)[[column]].sum()
            
            # Create a matrix filled with zeros
            matrix = pd.pivot_table(filtered_data_group, values=column, index='taz_a', columns='taz_p').fillna(0)
            
            # Reindex to ensure the matrix is of size 4996x4996
            matrix = matrix.reindex(range(1, matrix_size + 1), fill_value=0).fillna(0)
            matrix = matrix.reindex(columns=range(1, matrix_size + 1), fill_value=0).fillna(0)
            
            # Convert the matrix to a NumPy array
            condition_matrices[f"{truck_type}_{condition}"] = matrix.values

        matrices[f"{column}_{condition}_matrices"] = condition_matrices

    # Create OMX file for each column
    htm_period = column.split("_")[0].upper()
    file_name = f"\htmtrips_{htm_period}.omx"
    matrix_file = output_dir + "\htm" + file_name

    # Create an OMX file and write matrices
    #with omx.open_file(file_name, 'w') as f:
    with omx.open_file(matrix_file, 'w') as f:
        for matrix_name, matrix_data in matrices.items():
            for sub_matrix_name, sub_matrix_data in matrix_data.items():
                #clean_matrix_name = matrix_name.replace('_', '')  # Remove underscores
                matrix_tod = matrix_name.split("_")[0].upper()
                clean_sub_matrix_name = sub_matrix_name.replace('_', '')  # Remove underscores
                #f[f"{clean_matrix_name}_{clean_sub_matrix_name}"] = sub_matrix_data
                f[f"{clean_sub_matrix_name}_{matrix_tod}"] = sub_matrix_data
    #copy_dst = assignment_dir + "\\" + file_name
    #shutil.copyfile(matrix_file, copy_dst)
    print(f"Matrices for '{column}' saved as '{file_name}'")