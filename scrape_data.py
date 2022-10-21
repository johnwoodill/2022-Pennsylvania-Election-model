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





year = 2012
year = 2020

def get_county_wide(year):
    type_lookup = {'2022': 'undefined', '2020': '83', '2018': '63', '2016': '54', '2014': '41', '2012': '27',
        '2010': '19', '2008': '17', '2006': '15', '2004': '13', '2002': '11', '2000': '9'}
    
    ElectionID = type_lookup[str(year)]
    ElectionType = 'G'

    url = f"https://www.electionreturns.pa.gov/General/SummaryResults?ElectionID={ElectionID}&ElectionType={ElectionType}&IsActive=0"

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
    county_lst = []
    for county_ in pa_counties:

        # Set url to parse
        url = f"https://www.electionreturns.pa.gov/General/CountyResults?countyName={county_}&ElectionID={ElectionID}&ElectionType={ElectionType}&IsActive=0"

        # Setup browser
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url) 
        
        # Wait for page to load then scrape data
        time.sleep(1)
        text = driver.find_element(By.CSS_SELECTOR, "div.panel-body").get_attribute("innerText")
        split_text = text.split("\n")

        # Get list of all races
        panels = [i for i, j in enumerate(split_text) if j == 'Back to Top']
        races = [i for i, j in enumerate(split_text) if j == 'Filter Options']
    
        # Loop through Filter options and stop when repeated        
        lst_ = []
        for race_ in np.arange(20):
            lst_.append(split_text[race_])
            if any(lst_.count(x) > 1 for x in lst_):
                lst_ = lst_[1:-1]
                break

        # Get individual panel indexes
        race_panels_ = []
        for i, race_ in enumerate(lst_):
            race_ = lst_[i]
            start_panel = [i for i, j in enumerate(split_text) if j == race_]  
            start_panel = start_panel[1]              
            end_panel = panels[i]
            outdat = pd.DataFrame({'race': [race_], 'start_idx': [start_panel], 'end_idx': [end_panel]})
            race_panels_.append(outdat)

        # Concat data and filter
        race_data = pd.concat(race_panels_)

        race_data = race_data[race_data['race'] \
            .isin(['President of the United States', 
                   'United States Senator', 'Governor'])]

        race_data = race_data.reset_index(drop=True)

        # Loop through each race and get data
        race_lst = []  
        for i in np.arange(len(race_data)):
            race = race_data.loc[i, 'race']
            start_panel = race_data.loc[i, 'start_idx']
            end_panel = race_data.loc[i, 'end_idx']

            panel_text = split_text[start_panel:end_panel][1:]

            # Set index based on changes in scrape
            if any("Runningmate" in s for s in panel_text):
                if any("Election" in s for s in panel_text):
                    idx_ = 8
                elif not any("Election" in s for s in panel_text):
                    idx_ = 5
            elif any("Election" in s for s in panel_text):
                if not any("Runningmate" in s for s in panel_text):
                    idx_ = 7
            else:
                idx_ = 4

            # Number of panel splits
            splits = int(len(panel_text)/idx_)
          
            # Loop through splits and collect data
            for split_ in np.arange(splits):
                # Separate panel
                panel_split_text = panel_text[split_*idx_:(split_*idx_ + idx_)]
                
                # Get data
                candidate = panel_split_text[0].split(",")[0].title()
                party = panel_split_text[1]
                votes = int(panel_split_text[3].split("\xa0")[-1].replace(",", ""))
                
                # Build df and append to list
                indat = pd.DataFrame({'year': year, 'county': county_, 
                            'race': race, 
                            'candidate': [candidate], 
                            'party': party,
                            'votes': votes})
                race_lst.append(indat)

        # Merge data and get percentages
        outdat = pd.concat(race_lst).reset_index(drop=True)
        outdat = outdat.assign(perc_count = np.round( (outdat['votes'] / np.sum(outdat['votes'])) * 100, 2))
        county_lst.append(outdat)
        print(f"{year}: {county_}")
        
    # Concat and return
    rdat = pd.concat(county_lst).reset_index(drop=True)
    return rdat









pa_gov = ["SHAPIRO, JOSHUA D", "MASTRIANO, DOUGLAS V", "HACKENBURG, JONATHAN MATTHEW", 
          "DIGIULIO, CHRISTINA PK", "SOLOSKI, JOSEPH P"]

pa_senate = ["FETTERMAN, JOHN K", "OZ, MEHMET C", "GERHARDT, ERIK", "WEISS, RICHARD L", "WASSMER, DANIEL"]

get_state_wide(pa_gov)
get_state_wide(pa_senate)

get_county_wide(pa_gov)
get_county_wide(pa_senate)



