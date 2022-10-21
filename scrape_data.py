import requests
import json
import time

import pandas as pd 
import numpy as np

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By

from creds import *
import s3fs



def get_state_wide(candidates):
    
    # Set S3 save directory
    try: 
        if candidates[0] == "SHAPIRO, JOSHUA D":
            save_folder = "gov"
        else: 
            save_folder = "senate"

        # Setup s3fs
        s3 = s3fs.S3FileSystem(anon=False, key=ACCESS_ID, secret=SECRET)

        # URL to scrape
        url = f"https://www.electionreturns.pa.gov/"
        
        # Setup webbrowser to process javascript
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url) 
        
        # Wait to load then parse data
        time.sleep(1)
        text = driver.find_element(By.CSS_SELECTOR, "div.panel-body").get_attribute("innerText")
        split_text = text.split("\n")

        # Get candidate index loc to parse
        cand1_idx = [i for i, j in enumerate(split_text) if j == candidates[0]]
        cand2_idx = [i for i, j in enumerate(split_text) if j == candidates[1]]
        cand3_idx = [i for i, j in enumerate(split_text) if j == candidates[2]]
        cand4_idx = [i for i, j in enumerate(split_text) if j == candidates[3]]
        cand5_idx = [i for i, j in enumerate(split_text) if j == candidates[4]]

        # Get index
        cand1_idx = cand1_idx[0]    
        cand2_idx = cand2_idx[0]
        cand3_idx = cand3_idx[0]
        cand4_idx = cand4_idx[0]
        cand5_idx = cand5_idx[0]

        # Candidate 1 -- get name, election day count, mail count, provisional count
        cand1_name = split_text[cand1_idx].split(" ")[0].replace(",", "").title()
        cand1_elec_dat_count = int(split_text[cand1_idx + 4].split(" ")[-1])
        cand1_mail_count = int(split_text[cand1_idx + 5].split(" ")[-1])
        cand1_prov_count = int(split_text[cand1_idx + 6].split(" ")[-1])

        # Candidate 2- - get name, election day count, mail count, provisional count
        cand2_name = split_text[cand2_idx].split(" ")[0].replace(",", "").title()
        cand2_elec_dat_count = int(split_text[cand2_idx + 4].split(" ")[-1])
        cand2_mail_count = int(split_text[cand2_idx + 5].split(" ")[-1])
        cand2_prov_count = int(split_text[cand2_idx + 6].split(" ")[-1])

        # Candidate 3 -- get name, election day count, mail count, provisional count
        cand3_name = split_text[cand3_idx].split(" ")[0].replace(",", "").title()
        cand3_elec_dat_count = int(split_text[cand3_idx + 4].split(" ")[-1])
        cand3_mail_count = int(split_text[cand3_idx + 5].split(" ")[-1])
        cand3_prov_count = int(split_text[cand3_idx + 6].split(" ")[-1])

        # Candidate 4 -- get name, election day count, mail count, provisional count
        cand4_name = split_text[cand4_idx].split(" ")[0].replace(",", "").title()
        cand4_elec_dat_count = int(split_text[cand4_idx + 4].split(" ")[-1])
        cand4_mail_count = int(split_text[cand4_idx + 5].split(" ")[-1])
        cand4_prov_count = int(split_text[cand4_idx + 6].split(" ")[-1])

        # Candidate 5 -- get name, election day count, mail count, provisional count
        cand5_name = split_text[cand5_idx].split(" ")[0].replace(",", "").title()
        cand5_elec_dat_count = int(split_text[cand5_idx + 4].split(" ")[-1])
        cand5_mail_count = int(split_text[cand5_idx + 5].split(" ")[-1])
        cand5_prov_count = int(split_text[cand5_idx + 6].split(" ")[-1])

        # Build data frame
        outdat = pd.DataFrame({'candidate': [cand1_name, cand2_name, cand3_name, cand4_name, cand5_name], 
           'party': ['Dem', 'Rep', 'Lib', 'Grn', 'Key'],
           'elect_count': [cand1_elec_dat_count, cand2_elec_dat_count, cand3_elec_dat_count, cand4_elec_dat_count, cand5_elec_dat_count],
           'mail_count': [cand1_mail_count, cand2_mail_count, cand3_mail_count, cand4_mail_count, cand5_mail_count],
           'prov_count': [cand1_prov_count, cand2_prov_count, cand3_prov_count, cand4_prov_count, cand5_prov_count]})

        # Sum up total counts for each candidate and calc percentages
        outdat = outdat.assign(total_count = outdat['elect_count'] + outdat['mail_count'] + outdat['prov_count'])
        outdat = outdat.assign(perc_count = np.round( (outdat['total_count'] / np.sum(outdat['total_count'])) * 100, 2))
        
        outdat = outdat.replace(np.nan, 0)

        # Insert timestamp for last updated
        outdat.insert(0, 'timestamp', pd.Timestamp.today("US/Pacific"))
        outdat = outdat.assign(timestamp = pd.to_datetime(outdat['timestamp']).dt.strftime("%Y-%m-%d %H:%M:%S"))

        # if np.sum(outdat['total_count']) != 0:
        #     assert np.sum(np.round(outdat['perc_count'], 2)) >= 100

        # Save to S3
        outdat.to_csv(f"s3://2022-midterms/{save_folder}/state_wide.csv", index=False,
            storage_options={"key": ACCESS_ID,
                             "secret": SECRET})

        print(f"Saved {save_folder.title()} Wide")
    except Exception as e:
        print(e)


def get_county_wide(candidates):

    # Set S3 save directory
    if candidates[0] == "SHAPIRO, JOSHUA D":
        save_folder = "gov"
    else: 
        save_folder = "senate"

    # Setup s3fs
    s3 = s3fs.S3FileSystem(anon=False, key=ACCESS_ID, secret=SECRET)

    # Setup browser to process javascript
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    pa_counties = ['Adams', 'Allegheny', 'Armstrong', 'Beaver', 'Bedford', 'Berks', 'Blair', 
        'Bradford', 'Bucks', 'Butler', 'Cambria', 'Cameron', 'Carbon', 'Centre', 'Chester', 
        'Clarion', 'Clearfield', 'Clinton', 'Columbia', 'Crawford', 'Cumberland', 'Dauphin', 
        'Delaware', 'Elk', 'Erie', 'Fayette', 'Forest', 'Franklin', 'Fulton', 'Greene', 
        'Huntingdon', 'Indiana', 'Jefferson', 'Juniata', 'Lackawanna', 'Lancaster', 'Lawrence', 
        'Lebanon', 'Lehigh', 'Luzerne', 'Lycoming', 'McKEAN', 'Mercer', 'Mifflin', 'Monroe', 
        'Montgomery', 'Montour', 'Northampton', 'Northumberland', 'Perry', 'Philadelphia', 
        'Pike', 'Potter', 'Schuylkill', 'Snyder', 'Somerset', 'Sullivan', 'Susquehanna', 'Tioga', 
        'Union', 'Venango', 'Warren', 'Washington', 'Wayne', 'Westmoreland', 'Wyoming', 'York']

    # Loop through counties and save
    for county_ in pa_counties:
        try:
            # Set url to parse
            url = f"https://www.electionreturns.pa.gov/General/CountyResults?countyName={county_}&ElectionID=undefined&ElectionType=G&IsActive=undefined"

            # Setup browser
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url) 
            
            # Wait for page to load then scrape data
            time.sleep(1)
            text = driver.find_element(By.CSS_SELECTOR, "div.panel-body").get_attribute("innerText")
            split_text = text.split("\n")

            # Get candidate index loc to parse
            cand1_idx = [i for i, j in enumerate(split_text) if j == candidates[0]]
            cand2_idx = [i for i, j in enumerate(split_text) if j == candidates[1]]
            cand3_idx = [i for i, j in enumerate(split_text) if j == candidates[2]]
            cand4_idx = [i for i, j in enumerate(split_text) if j == candidates[3]]
            cand5_idx = [i for i, j in enumerate(split_text) if j == candidates[4]]

            # Get index
            cand1_idx = cand1_idx[0]    
            cand2_idx = cand2_idx[0]
            cand3_idx = cand3_idx[0]
            cand4_idx = cand4_idx[0]
            cand5_idx = cand5_idx[0]

            # Candidate 1 -- get name, election day count, mail count, provisional count
            cand1_name = split_text[cand1_idx].split(" ")[0].replace(",", "").title()
            cand1_elec_dat_count = int(split_text[cand1_idx + 4].split(" ")[-1])
            cand1_mail_count = int(split_text[cand1_idx + 5].split(" ")[-1])
            cand1_prov_count = int(split_text[cand1_idx + 6].split(" ")[-1])

            # Candidate 2 -- get name, election day count, mail count, provisional count
            cand2_name = split_text[cand2_idx].split(" ")[0].replace(",", "").title()
            cand2_elec_dat_count = int(split_text[cand2_idx + 4].split(" ")[-1])
            cand2_mail_count = int(split_text[cand2_idx + 5].split(" ")[-1])
            cand2_prov_count = int(split_text[cand2_idx + 6].split(" ")[-1])

            # Candidate 3 -- get name, election day count, mail count, provisional count
            cand3_name = split_text[cand3_idx].split(" ")[0].replace(",", "").title()
            cand3_elec_dat_count = int(split_text[cand3_idx + 4].split(" ")[-1])
            cand3_mail_count = int(split_text[cand3_idx + 5].split(" ")[-1])
            cand3_prov_count = int(split_text[cand3_idx + 6].split(" ")[-1])

            # Candidate 4 -- get name, election day count, mail count, provisional count
            cand4_name = split_text[cand4_idx].split(" ")[0].replace(",", "").title()
            cand4_elec_dat_count = int(split_text[cand4_idx + 4].split(" ")[-1])
            cand4_mail_count = int(split_text[cand4_idx + 5].split(" ")[-1])
            cand4_prov_count = int(split_text[cand4_idx + 6].split(" ")[-1])

            # Candidate 5 -- get name, election day count, mail count, provisional count
            cand5_name = split_text[cand5_idx].split(" ")[0].replace(",", "").title()
            cand5_elec_dat_count = int(split_text[cand5_idx + 4].split(" ")[-1])
            cand5_mail_count = int(split_text[cand5_idx + 5].split(" ")[-1])
            cand5_prov_count = int(split_text[cand5_idx + 6].split(" ")[-1])

            # Build data frame
            outdat = pd.DataFrame({'county': county_, 'candidate': [cand1_name, cand2_name, cand3_name, cand4_name, cand5_name], 
               'party': ['Dem', 'Rep', 'Lib', 'Grn', 'Key'],
               'elect_count': [cand1_elec_dat_count, cand2_elec_dat_count, cand3_elec_dat_count, cand4_elec_dat_count, cand5_elec_dat_count],
               'mail_count': [cand1_mail_count, cand2_mail_count, cand3_mail_count, cand4_mail_count, cand5_mail_count],
               'prov_count': [cand1_prov_count, cand2_prov_count, cand3_prov_count, cand4_prov_count, cand5_prov_count]})

            # Sum up total counts for each candidate and calc percentages
            outdat = outdat.assign(total_count = outdat['elect_count'] + outdat['mail_count'] + outdat['prov_count'])
            outdat = outdat.assign(perc_count = np.round( (outdat['total_count'] / np.sum(outdat['total_count'])) * 100, 2))
            
            # if np.sum(outdat['total_count']) != 0:
            #     assert np.sum(np.round(outdat['perc_count'], 2)) >= 100

            # Insert timestamp for last updated
            outdat.insert(0, 'timestamp', pd.Timestamp.today("US/Pacific"))
            outdat = outdat.assign(timestamp = pd.to_datetime(outdat['timestamp']).dt.strftime("%Y-%m-%d %H:%M:%S"))

            # If exists, load, and filter out last, else create new and save
            if s3.exists(f"s3://2022-midterms/{save_folder}/county_wide.csv"):
                savedat = pd.read_csv(f"s3://2022-midterms/{save_folder}/county_wide.csv", 
                    storage_options={"key": ACCESS_ID,
                                     "secret": SECRET})

                savedat = savedat[savedat['county'] != county_]
            else:
                savedat = pd.DataFrame()

            # Concat data and save
            savedat = pd.concat([savedat, outdat])
            savedat.to_csv(f"s3://2022-midterms/{save_folder}/county_wide.csv", index=False,
                storage_options={"key": ACCESS_ID,
                                 "secret": SECRET})

            print(f"Saved {save_folder.title()} Wide: {county_}")
        except Exception as e:
            print(e)




pa_gov = ["SHAPIRO, JOSHUA D", "MASTRIANO, DOUGLAS V", "HACKENBURG, JONATHAN MATTHEW", 
          "DIGIULIO, CHRISTINA PK", "SOLOSKI, JOSEPH P"]

pa_senate = ["FETTERMAN, JOHN K", "OZ, MEHMET C", "GERHARDT, ERIK", "WEISS, RICHARD L", "WASSMER, DANIEL"]

get_state_wide(pa_gov)
get_state_wide(pa_senate)

get_county_wide(pa_gov)
get_county_wide(pa_senate)



