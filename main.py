import pandas as pd
import requests
from xml.etree import ElementTree as ET
import gspread
import datetime

columns = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths", 
           "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
          "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
           "Estimates of number of homicides",
           "Crude suicide rates (per 100 000 population)",
           "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
           "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
           "Estimated road traffic death rate (per 100 000 population)",
           "Estimated number of road traffic deaths",
           "Mean BMI (kg/m&#xb2;) (crude estimate)",
           "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
           "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)",
           "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
           "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
           "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
           "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
           "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
           "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
           "Estimate of daily cigarette smoking prevalence (%)",
           "Estimate of daily tobacco smoking prevalence (%)",
           "Estimate of current cigarette smoking prevalence (%)",
           "Estimate of current tobacco smoking prevalence (%)",
           "Mean systolic blood pressure (crude estimate)",
           "Mean fasting blood glucose (mmol/l) (crude estimate)",
           "Mean Total Cholesterol (crude estimate)"
          ]

countries = {'CHL': 'Chile', 'BOL': 'Bolivia', 'ARG': 'Argentina', 'URY': 'Uruguay', 'PER': 'Peru', 'COL': 'Colombia'}

def normalize(gho, numeric):
    if "(per 100 000 population)" in gho:
        return numeric * 100000
    return numeric

def generate_dataframe(root, country):
    tags ={'GHO': [], 'COUNTRY': [], 'SEX': [], 'YEAR': [], 'GHECAUSES': [], 'AGEGROUP': [], 'Display': [], 'Numeric': [], 'Low': [], 'High': []}  
    for child in root:
        gho = child.find(f'./GHO')
        if gho.text in columns:
            for tag in tags.keys():
                child2 = child.find(f'./{tag}')
                if child2 is None:
                    tags[tag].append(None)
                else:
                    tags[child2.tag].append(child2.text)

    df =  pd.DataFrame(tags)
    df.COUNTRY = df.COUNTRY.replace({None: country})
    df.Numeric = df.apply(lambda x: normalize(x['GHO'], float(x['Numeric'])), axis=1)
    df.YEAR = df.YEAR.apply(lambda x: str(datetime.date(int(x), 1, 1)))
    return df



if __name__ == '__main__':
    gc = gspread.service_account(filename='/Users/jptorres/Documents/Otros/taller_integracion/T4/taller-tarea-4-316102-f1b84cc11a16.json')
    sh = gc.open_by_key('1IDDpfDDsJ-xDtqwTi4JXUWKomEouM_6zRWeix9Mbg0o')
    worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc. 

    df = None
    for index, country_ISO in enumerate(countries.keys()):
        country = requests.get(f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{country_ISO}.xml')
        root = ET.fromstring(country.content)
        if df is None:
            df = generate_dataframe(root, countries[country_ISO])
        else:
            df = df.append(generate_dataframe(root, countries[country_ISO]))
    
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    
