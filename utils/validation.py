# wraps CT-specific validation around ERMIN validators
from ermin import validation as ev
import pandas as pd
import datetime


# Function to check CT-specific requirements,
# Such as which years need to be included.
# These are hard-coded for now and subject to change.
def check_ct_requirements(input_df, sector,
                          max_start_date=datetime.date(2015, 1, 1),
                          min_end_date=datetime.date(2021,12,31),
                          emissions_columns = ['CO2_emissions_tonnes', 'CH4_emissions_tonnes', 'N2O_emissions_tonnes', 'total_CO2e_100yrGWP','total_CO2e_20yrGWP']
                          ):
    """Check entire input data frame against spec file
              
       Data for a country must include all years between max_start_year
       and min_end_year, inclusive.

       Full requirements per internal communication:
       - UI is expecting the year ranges to be within the same year
         (start_date & end_date). So, if start_date = "2015-01-01"
         and end_date = "2016-01-01". Reject data. Date ranges must
         fall within the same year. 
       - For all countries the data range must include 2015 to 2021.
         If we have data with 2022 data should not be a problem. 
       - Data should have all the countries in the Climate TRACE country
         dictionary
       - (TO DO) For CO2_emissions_tonnes, CH4_emissions_tonnes,N2O_emissions_tonnes,
         total_CO2e_100yrGWP,total_CO2e_20yrGWP for all sectors
         except "forest-sink" and "net-forest-emissions" the values should either 
         be an empty object "NULL,none,nan" or "positive float numbers". We 
         should only accept "negative float numbers" for "forest-sink" and 
         "net-forest-emissions".

       Parameters:
       input_df (DataFrame): Emissions report DataFrame
       sector (str): sector name (e.g. "forest-sink")
       max_start_date (datetime): data for a country must begin no later than this year
       min_end_date (datetime): data for a country must end no earlier than this year
       emissions_columns (list): list of names of columns containing emissions values
                                 (i.e. columns to be checked for negative values)

       Returns:
       warnings (list): a list of warnings encountered
       errors (list): a list of errors encountered
    """
    warnings = []
    errors = []

    # For each country, ensure aggregate dates span correct minimum rate
    for country in input_df['iso3_country'].unique():
        start_dates = input_df.loc[input_df['iso3_country'] == country, 'start_date']
        start_dates = [datetime.datetime.fromisoformat(date).date() for date in start_dates]
        end_dates = input_df.loc[input_df['iso3_country'] == country, 'end_date']
        end_dates = [datetime.datetime.fromisoformat(date).date() for date in end_dates]
        if min(start_dates) > max_start_date:
            errors.append('Error: Data for country ' + country + ' starts on ' + str(min(start_dates)) + ', requirement is on or before ' + str(max_start_date))
        if max(end_dates) < min_end_date:
            errors.append('Error: Data for country ' + country + ' ends on ' + str(max(end_dates)) + ', requirement is on or after ' + str(min_end_date))

    # For each entry, ensure time starts and ends in same year
    for i in range(len(input_df)):
        start_year = datetime.datetime.fromisoformat(input_df.at[i,'start_date']).year
        end_year = datetime.datetime.fromisoformat(input_df.at[i,'end_date']).year
        if start_year != end_year:
            errors.append('Error: Entry spans more than one year: ' + str('\t'.join(input_df.loc[i,['start_date','end_date','iso3_country']].tolist())))

    # Ensure all countries present
    countrylist = input_df['iso3_country'].unique()
    for country in COUNTRIES_DICT:
        if not country in countrylist:
            errors.append('Error: country ' + country + ' missing from input table.')

    # Ensure nan or positive float for all sectors and all emissions quantities
    # except for "forest-sink" and "net-forest-emissions"
    if sector not in ['forest-sink','net-forest-emissions']:
        for i in range(len(input_df)):
            for emission_column in emissions_columns:
                emissions_val = input_df.at[i,emission_column]
                year = str(datetime.datetime.fromisoformat(input_df.at[i,'end_date']).year)
                country = input_df.at[i,'iso3_country']
                if emissions_val != '' and emissions_val != 'NULL':
                    try:
                        emissions_val = float(emissions_val)
                        if emissions_val < 0:
                            errors.append('Error: Negative ' + emission_column + ' emissions ' + str(emissions_val) + ' reported in ' + year + ' for country ' + country)
                    except ValueError:
                        errors.append('Could not check >=0 status of ' + emission_column + ' value ' + emissions_val + ' reported in ' + year + ' for country ' + 'country because could not convert to float.')

    return warnings, errors

# Wrapper function for using ERMIN module to validate data
# But using climate_trace specification.
# This means there is at least one additional field type, 
# namely {iso3_country} that needs special checking.
def check_input_dataframe(input_df,
                          spec_file=None,
                          repair=False,
                          output_file=None,
                          allow_unknown_stringtypes=False):
    """Check entire input data frame against spec file
              
       Optionally repairs missing data according to specification.

       Parameters:
       input_df (DataFrame): Emissions report DataFrame
       spec_file (str): Path to specification CSV file or None.
       repair (bool): If True, repair missing/invalid and return new DataFrame
       output_file (str): If not None and repair, write repaired data to output file.
       allow_unknown_stringyptes (bool): if False, raise error on unknown stringtypes

       Returns:
       warnings (list): a list of warnings encountered
       errors (list): a list of errors encountered
       newdf (DataFrame): only returned if repair
    """

    errors = []

    # Check ct-specific stringtype syntax first
    ct_stringtypes = ['iso3_country']
    spec = ev.load_spec(spec_file)

    # check for any CT-specific types
    for row in spec:
        syntax = row['Value syntax']
        fieldname = row['Structured name']
        if syntax in ct_stringtypes and fieldname in df:
            # There is a field with CT-specific syntax,
            # see if it is contained in the inputs
            for value in df[fieldname]:
                errors += check_syntax(value, syntax)

    # Now check remaining fields with ERMIN checker
    if repair:
        warnings, newerrors, newdf = ev.check_input_dataframe(input_df, spec_file = spec_file,
                                                repair = repair,
                                                allow_unknown_stringtypes=allow_unknown_stringtypes)
        errors += newerrors
        return warnings, errors, newdf
    else:
        warnings, newerrors = ev.check_input_dataframe(input_df, spec_file = spec_file,
                                                repair = repair,
                                                allow_unknown_stringtypes=allow_unknown_stringtypes)
        errors += newerrors
        return warnings, errors


def check_syntax(value, syntax):
    """CT-specific syntax checker, e.g. for {iso3_country} stringtype
       
       Checks whether value matches stringtype.

       Currently, the only accepted string type is {iso3_country}

       Parameters:
       value (str): input value to be checked
       stringtype (str): acceptable syntax description

       Returns:
       list: list of errors, empty if no errors
    """
    error_list = []

    # First check non-string options (bool, int, float, timestamp)
    if type(value) is not str:
        raise ValueError('Syntax is ' + stringtype + ', but this type was provided: ' + str(type(value)))
    else:      
        # Now we know the value is a string

        # Test each syntax type
        if stringtype == "{iso3_country}":
            if value not in COUNTRIES_DICT:
                error_list.append('The value ' + value + ' was not a valid iso3_country code.')
        else:
            raise ValueError('Error: unknown climatetrace stringtype "' + stringtype + '"')

    return error_list

COUNTRIES_DICT = {
    "SCG": "deleted",
    "ANT": "deleted",
    "ABW": "Aruba",
    "AFG": "Afghanistan",
    "AGO": "Angola",
    "AIA": "Anguilla",
    "ALA": "Åland Islands",
    "ALB": "Albania",
    "AND": "Andorra",
    "ARE": "United Arab Emirates",
    "ARG": "Argentina",
    "ARM": "Armenia",
    "ASM": "American Samoa",
    "ATA": "Antarctica",
    "ATF": "French Southern Territories",
    "ATG": "Antigua and Barbuda",
    "AUS": "Australia",
    "AUT": "Austria",
    "AZE": "Azerbaijan",
    "BDI": "Burundi",
    "BEL": "Belgium",
    "BEN": "Benin",
    "BES": "Bonaire, Sint Eustatius and Saba",
    "BFA": "Burkina Faso",
    "BGD": "Bangladesh",
    "BGR": "Bulgaria",
    "BHR": "Bahrain",
    "BHS": "Bahamas",
    "BIH": "Bosnia and Herzegovina",
    "BLM": "Saint Barthélemy",
    "BLR": "Belarus",
    "BLZ": "Belize",
    "BMU": "Bermuda",
    "BOL": "Bolivia (Plurinational State of)",
    "BRA": "Brazil",
    "BRB": "Barbados",
    "BRN": "Brunei Darussalam",
    "BTN": "Bhutan",
    "BVT": "Bouvet Island",
    "BWA": "Botswana",
    "CAF": "Central African Republic",
    "CAN": "Canada",
    "CCK": "Cocos (Keeling) Islands",
    "CHE": "Switzerland",
    "CHL": "Chile",
    "CHN": "China",
    "CIV": "Côte d'Ivoire",
    "CMR": "Cameroon",
    "COD": "Democratic Republic of the Congo",
    "COG": "Congo",
    "COK": "Cook Islands",
    "COL": "Colombia",
    "COM": "Comoros",
    "CPV": "Cabo Verde",
    "CRI": "Costa Rica",
    "CUB": "Cuba",
    "CUW": "Curaçao",
    "CXR": "Christmas Island",
    "CYM": "Cayman Islands",
    "CYP": "Cyprus",
    "CZE": "Czechia",
    "DEU": "Germany",
    "DJI": "Djibouti",
    "DMA": "Dominica",
    "DNK": "Denmark",
    "DOM": "Dominican Republic",
    "DZA": "Algeria",
    "ECU": "Ecuador",
    "EGY": "Egypt",
    "ERI": "Eritrea",
    "ESH": "Western Sahara",
    "ESP": "Spain",
    "EST": "Estonia",
    "ETH": "Ethiopia",
    "FIN": "Finland",
    "FJI": "Fiji",
    "FLK": "Falkland Islands (Malvinas)",
    "FRA": "France",
    "FRO": "Faroe Islands",
    "FSM": "Micronesia (Federated States of)",
    "GAB": "Gabon",
    "GBR": "United Kingdom of Great Britain and Northern Ireland",
    "GEO": "Georgia",
    "GGY": "Guernsey",
    "GHA": "Ghana",
    "GIB": "Gibraltar",
    "GIN": "Guinea",
    "GLP": "Guadeloupe",
    "GMB": "Gambia",
    "GNB": "Guinea-Bissau",
    "GNQ": "Equatorial Guinea",
    "GRC": "Greece",
    "GRD": "Grenada",
    "GRL": "Greenland",
    "GTM": "Guatemala",
    "GUF": "French Guiana",
    "GUM": "Guam",
    "GUY": "Guyana",
    "HKG": "China, Hong Kong Special Administrative Region",
    "HMD": "Heard Island and McDonald Islands",
    "HND": "Honduras",
    "HRV": "Croatia",
    "HTI": "Haiti",
    "HUN": "Hungary",
    "IDN": "Indonesia",
    "IMN": "Isle of Man",
    "IND": "India",
    "IOT": "British Indian Ocean Territory",
    "IRL": "Ireland",
    "IRN": "Iran (Islamic Republic of)",
    "IRQ": "Iraq",
    "ISL": "Iceland",
    "ISR": "Israel",
    "ITA": "Italy",
    "JAM": "Jamaica",
    "JEY": "Jersey",
    "JOR": "Jordan",
    "JPN": "Japan",
    "KAZ": "Kazakhstan",
    "KEN": "Kenya",
    "KGZ": "Kyrgyzstan",
    "KHM": "Cambodia",
    "KIR": "Kiribati",
    "KNA": "Saint Kitts and Nevis",
    "KOR": "Republic of Korea",
    "XKX": "Kosovo",
    "KWT": "Kuwait",
    "LAO": "Lao People's Democratic Republic",
    "LBN": "Lebanon",
    "LBR": "Liberia",
    "LBY": "Libya",
    "LCA": "Saint Lucia",
    "LIE": "Liechtenstein",
    "LKA": "Sri Lanka",
    "LSO": "Lesotho",
    "LTU": "Lithuania",
    "LUX": "Luxembourg",
    "LVA": "Latvia",
    "MAC": "China, Macao Special Administrative Region",
    "MAF": "Saint Martin (French Part)",
    "MAR": "Morocco",
    "MCO": "Monaco",
    "MDA": "Republic of Moldova",
    "MDG": "Madagascar",
    "MDV": "Maldives",
    "MEX": "Mexico",
    "MHL": "Marshall Islands",
    "MKD": "The former Yugoslav Republic of Macedonia",
    "MLI": "Mali",
    "MLT": "Malta",
    "MMR": "Myanmar",
    "MNE": "Montenegro",
    "MNG": "Mongolia",
    "MNP": "Northern Mariana Islands",
    "MOZ": "Mozambique",
    "MRT": "Mauritania",
    "MSR": "Montserrat",
    "MTQ": "Martinique",
    "MUS": "Mauritius",
    "MWI": "Malawi",
    "MYS": "Malaysia",
    "MYT": "Mayotte",
    "NAM": "Namibia",
    "NCL": "New Caledonia",
    "NER": "Niger",
    "NFK": "Norfolk Island",
    "NGA": "Nigeria",
    "NIC": "Nicaragua",
    "NIU": "Niue",
    "NLD": "Netherlands",
    "NOR": "Norway",
    "NPL": "Nepal",
    "NRU": "Nauru",
    "NZL": "New Zealand",
    "OMN": "Oman",
    "PAK": "Pakistan",
    "PAN": "Panama",
    "PCN": "Pitcairn",
    "PER": "Peru",
    "PHL": "Philippines",
    "PLW": "Palau",
    "PNG": "Papua New Guinea",
    "POL": "Poland",
    "PRI": "Puerto Rico",
    "PRK": "Democratic People's Republic of Korea",
    "PRT": "Portugal",
    "PRY": "Paraguay",
    "PSE": "State of Palestine",
    "PYF": "French Polynesia",
    "QAT": "Qatar",
    "REU": "Réunion",
    "ROU": "Romania",
    "RUS": "Russian Federation",
    "RWA": "Rwanda",
    "SAU": "Saudi Arabia",
    "SDN": "Sudan",
    "SEN": "Senegal",
    "SGP": "Singapore",
    "SGS": "South Georgia and the South Sandwich Islands",
    "SHN": "Saint Helena",
    "SJM": "Svalbard and Jan Mayen Islands",
    "SLB": "Solomon Islands",
    "SLE": "Sierra Leone",
    "SLV": "El Salvador",
    "SMR": "San Marino",
    "SOM": "Somalia",
    "SPM": "Saint Pierre and Miquelon",
    "SRB": "Serbia",
    "SSD": "South Sudan",
    "STP": "Sao Tome and Principe",
    "SUR": "Suriname",
    "SVK": "Slovakia",
    "SVN": "Slovenia",
    "SWE": "Sweden",
    "SWZ": "Eswatini",
    "SXM": "Sint Maarten (Dutch part)",
    "SYC": "Seychelles",
    "SYR": "Syrian Arab Republic",
    "TCA": "Turks and Caicos Islands",
    "TCD": "Chad",
    "TGO": "Togo",
    "THA": "Thailand",
    "TJK": "Tajikistan",
    "TKL": "Tokelau",
    "TKM": "Turkmenistan",
    "TLS": "Timor-Leste",
    "TON": "Tonga",
    "TTO": "Trinidad and Tobago",
    "TUN": "Tunisia",
    "TUR": "Turkey",
    "TUV": "Tuvalu",
    "TWN": "Taiwan",
    "TZA": "United Republic of Tanzania",
    "UGA": "Uganda",
    "UKR": "Ukraine",
    "UMI": "United States Minor Outlying Islands",
    "URY": "Uruguay",
    "USA": "United States of America",
    "UZB": "Uzbekistan",
    "VAT": "Holy See",
    "VCT": "Saint Vincent and the Grenadines",
    "VEN": "Venezuela (Bolivarian Republic of)",
    "VGB": "British Virgin Islands",
    "VIR": "United States Virgin Islands",
    "VNM": "Viet Nam",
    "VUT": "Vanuatu",
    "WLF": "Wallis and Futuna Islands",
    "WSM": "Samoa",
    "YEM": "Yemen",
    "ZAF": "South Africa",
    "ZMB": "Zambia",
    "ZWE": "Zimbabwe",
}
