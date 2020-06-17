import logging
import json
import csv
import requests
from datetime import datetime
import time
import twitter
import pandas as pd

def load_geo_data(file_prefix, url, count_params, rec_params):
    """declare initial varaibles"""

    total_count_url = url+count_params  # create url to for passing to GEOHub for counting records from API call
    response = requests.get(total_count_url)  # response from API call to count records
    json_data = json.loads(response.text)  # Conversion of response to JSON
    remaining_count = json_data['count']  # Count of total records
    rec_inc_val = 0  # Record increment start value
    max_records = 2000  # Max records returned from API call
    n = 1  # while loop incrementer
    file_dt = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')  # Date and time file was created format = 'yyyymmddHHMMSS'
    rec_dt = datetime.strftime(datetime.now(), '%Y%m%d') # Date record was created
    rec_tm = datetime.strftime(datetime.now(), '%H%M%S') # Time record was created
    raw_filename = file_prefix+'_'+file_dt+'.geojson'  # Name of raw GEOJSON file

    logging.info("Total records available for download: {}".format(remaining_count))

    """Retrieve records from GEOHub API call.  Records are limited to 'max_value' variable.  
    Several calls are made based on the remaining_count variable minus max_records until the remaining_count < 0"""

    logging.info("Start writing source file")

    while remaining_count > 0:

        if n == 1:
            raw_data = requests.get(url+rec_params+str(rec_inc_val))
            json_data = json.loads(raw_data.text)
        else:
            raw_data = requests.get(url + rec_params + str(rec_inc_val))
            json_data.update(json.loads(raw_data.text))
        remaining_count -= max_records
        rec_inc_val += max_records
        n += 1


        """Write GeoJSON data to raw file"""
        """Each GeoJSON file created is unique due to the datetime value in the file name"""

        with open(raw_filename, 'a') as geojson_file:  # create
            logging.info("Writing raw json")
            json.dump(json_data, geojson_file)


        """Writing GeoJSON data to CSV file"""
        """The CSV files are overwritten each time this process is run"""

        """Writing ony the 'properties'' data from the GeoJSON file to a specific CSV file"""

        additional_keys = ['rec_date', 'rec_time']  # Additional field names for CSV file to include date & time

        with open(file_prefix+'.csv', 'a', newline='') as csv_file:

            keys = list(json_data["features"][0]["properties"].keys())+additional_keys  # Get list of Keys for accessing items in JSON

            csv_writer = csv.DictWriter(csv_file, fieldnames=keys, delimiter=',')

            logging.info("Writing address CSV")

            csv_writer.writeheader()

            for i in json_data["features"]:
                row = i["properties"]
                update_row = {'rec_date': rec_dt, 'rec_time': rec_tm}
                row.update(update_row)
                csv_writer.writerow(row)

        """Writing ony the 'coordinate' data from the GeoJSON file to a specific CSV file"""

        with open(file_prefix + '_coord.csv', 'a', newline='') as csv_file:
            for k in json_data['features']:
                row_key = {'id': k['id']}
                row_key.update(k['geometry'])

            keys = list(row_key.keys())+(additional_keys)

            csv_writer = csv.DictWriter(csv_file, fieldnames=keys, delimiter=',')

            logging.info("Writing Parks and Natural Areas CSV")

            csv_writer.writeheader()

            for k in json_data['features']:
                row = {'id': k['id']}
                update_row = {'rec_date': rec_dt, 'rec_time': rec_tm}
                row.update(k['geometry'])
                row.update(update_row)
                csv_writer.writerow(row)

    logging.info("Completed writing CSV files")



def cleanse_r_v(file1):
    """Splitting the 'full_address' value into separate parts and standardsing the case to provide a consistent
    approach to data sets"""
    logging.info("Cleanse file: {}".format(file1))
    df = pd.read_csv(file1)  # Read data from file mentioned in function call

    """Spliting the 'full_address' value using regex and applying a consistent case"""
    df['street_no'] = df['full_address'].str.extract(r'(^[0-9- ]+[A-Za-z] |^[0-9]+)')
    df['street_no'] = df['street_no'].str.strip()
    df['street_no'] = df['street_no'].str.upper()
    df['street'] = df['full_address'].str.extract(r'([A-za-z]{2,} [A-za-z]{2,})')
    df['street'] = df['street'].str.upper()
    df['State_highway'] = df['full_address'].str.extract(r'[0-9 A-Za-z]+ (S H [0-9])')
    df['State_highway'] = df['State_highway'].str.upper()
    df['LocationArea'] = df['full_address'].str.extract(r'[0-9 A-Za-z]+, ([A-Za-z ]+)')
    df['LocationArea'] = df['LocationArea'].str.upper()
    df['alt_area'] = df['full_address'].str.extract(r'[0-9 A-Za-z]+, [A-Za-z ]+, ([A-Za-z ]+)')
    df['alt_area'] = df['alt_area'].str.upper()

    """Writing the data to a new CSV file"""
    df.to_csv('npdc_R_V_cleansed.csv', index=False)

    logging.info("End of cleansing file: {}".format(file1))
    return df

def twitter_module():
    """For this process, the entire available amount of tweets for the specified twitter account is downloaded"""
    logging.info("Begin downloading tweets")

    api = twitter.Api(consumer_key='N1VGVEsuxkRxjecJkCZESfyNT',
                      consumer_secret='bUs1mT0TeiuXSmqh0SjPKzpM09vEDl9x4uAWm5xwFtUN6vdiAv',
                      access_token_key='2377854260-eQg0hqbz5S4Los8irTwchKFPl8SNTtOylM5sIl0',
                      access_token_secret='uRharA83P8BYD3bYt4MFxu58wSwXBTPtcruK634bVJs3K',
                      tweet_mode='extended')

    tweets = api.GetUserTimeline(screen_name='NPDCouncil')  # The twitter user to download tweets from

    streets_df = pd.read_csv('npdc_R_V_cleansed.csv')  # CSV file used to generate the list of street names used when

    street_set = set(streets_df['street'])  # List of Streets is generate from dataframe

    st_in_tw = {}  # Blank dictionary to store street (as key) and tweet (as value)

    """For each tweet check if a value from the list of street names is in the tweet. If a street name is present 
    write the street name and the tweet to the specified dictionary.  The intention is for the tweets to be processed 
    real time. If more than one tweet contains the same street, the dictionary value will be overwritten"""
    for id in tweets:
        tweet = id.full_text
        tweet = tweet.upper()
        for street in street_set:
            if str(street) in tweet:
                st_in_tw[street] = id.full_text

    with open('st_in_tweet.csv', 'w', encoding='utf-8', newline='') as csv_file:

        csv_file.write('street,tweet\n')

        for street, tweet in st_in_tw.items():
            row_data = '{street},\"{tweet}\"\n'.format(street=street, tweet=tweet)
            csv_file.write(row_data)

    logging.info("End downloading tweets")

def summarise_p_na(file2):
    """Individual parks and natural areas for a given LocationArea are grouped into a single list and the total
    number of parks & natural areas and the total size of all the areas is calculated"""
    logging.info("Begin summarising parks data")

    df = pd.read_csv(file2)  # File used with park & natural area details

    df['LocationArea'] = df['LocationArea'].str.upper()  # Standardise case of values

    """Sum the total area and count the number of spaces for a given LocationArea"""
    summ = df.groupby('LocationArea').agg(
        total_natural_space = pd.NamedAgg(column='Shape__Area', aggfunc=sum)
        , number_of_natural_spaces = pd.NamedAgg(column='LocationArea', aggfunc='count')
    )

    """Create list of parks in a given LocationArea"""
    pdf = pd.read_csv('npdc_P_NA.csv')
    pdf = pdf[pdf.LocationSite.notnull()]
    pdf = pdf[['LocationArea', 'LocationSite']]
    pdf['LocationArea'] = pdf['LocationArea'].str.upper()
    parks = pdf.groupby('LocationArea')

    pdf = parks.aggregate(lambda x: tuple(x))

    """Merge the list of parks to the summarised values"""
    summ = summ.merge(pdf, on='LocationArea', how='left')

    """Write merged and summarised values to file"""
    summ.to_csv('n_pa_summ.csv', index=False)

    logging.info("End summarising parks data")

    return summ


def combine_add_and_na(file1, file2, file3):
    """Combine Cleaned Rates data, summarised parks data and twitter data to produce final data set"""

    df1 = cleanse_r_v(file1)
    twitter_module()  # This function is dependant on the previous function to complete
    df2 = summarise_p_na(file2)
    df3 = pd.read_csv(file3)

    logging.info("Begin merging data")

    """Left merge to combine rates data and parks data set"""
    merged_df = df1.merge(df2, on='LocationArea', how='left')

    """This action is not needed for futher processing.  File is writen to help with debugging"""
    #merged_df.to_csv('merged.csv', index=False)

    """Merge tweets that contain a street name in them"""
    merged2_df = merged_df.merge(df3, on='street', how='left')

    """This action is not needed for futher processing.  File is writen to help with debugging"""
    #merged2_df.to_csv('merged2.csv', index=False)


    """Two Pandas if statements to apply the tweet two the mentioned street if it exists or display the record date"""
    merged2_df.loc[merged2_df['tweet'].notnull(), 'latest_info']= merged2_df['tweet']
    merged2_df.loc[merged2_df['tweet'].isnull(), 'latest_info'] = merged2_df['rec_date']

    """Create the final dataframe prior to writing to file"""
    final_df = merged2_df[['AS_ASSESS_NO',
                           'full_address',
                           'capital_value',
                           'annual_rates',
                           'LocationSite',
                           'latest_info',
                           'total_natural_space',
                           'number_of_natural_spaces']]

    """Write the final file"""
    final_df.to_csv('final.csv', index=False)

    logging.info("End merging data")

def main():
    """Include basic logging"""
    logging.basicConfig(filename='TSB_test.log', level=logging.INFO,
                        format='%(asctime)s: %(levelname)s: %(message)s')

    script_st = int(time.time())

    logging.info("Script start")

    load_geo_data('npdc_R_V', 'https://atlas.npdc.govt.nz/server/rest/services/OpenData/Customer_Regulatory/FeatureServer/0/query?', 'where=1=1&returnCountOnly=true&f=json', 'outFields=*&outSR=4326&f=geojson&where=OBJECTID>')
    load_geo_data('npdc_P_NA', 'https://atlas.npdc.govt.nz/server/rest/services/OpenData/Infrastructure_Parks/FeatureServer/4/query?', 'where=1=1&returnCountOnly=true&f=json', 'outFields=*&outSR=4326&f=geojson&where=OBJECTID>')
    combine_add_and_na('npdc_R_V.csv', 'npdc_P_NA.csv', 'st_in_tweet.csv')

    logging.info("Script end")

    logging.info("duration of script: {}".format(int(time.time())- script_st))

if __name__ == '__main__':
    main()
