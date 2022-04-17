# wraps CT-specific validation around ERMIN validators
from ermin import validation as ev
import pandas as pd
import datetime


# Function to check CT-specific requirements,
# Such as which years need to be included.
# These are hard-coded for now and subject to change.
def check_ct_requirements(input_df,
						  max_start_date=datetime.date(2015, 1, 1),
						  min_end_date=datetime.date(2021,12,31)):
	"""Check entire input data frame against spec file
	          
	   Data for a country must include all years between max_start_year
	   and min_end_year, inclusive.

	   Parameters:
	   input_df (DataFrame): Emissions report DataFrame
	   max_start_date (datetime): data for a country must begin no later than this year
	   min_end_date (datetime): data for a country must end no earlier than this year

	   Returns:
	   warnings (list): a list of warnings encountered
	   errors (list): a list of errors encountered
	   newdf (DataFrame): only returned if repair
	"""

	# For each country
	for country in input_df['iso3_country'].unique():
		print(country)
		dates = input_df.loc[input_df['iso3_country'] == country, 'start_date']
		dates = [datetime.datetime.fromisoformat(date).date() for date in dates]
		print(min(dates))
		print(max(dates))
		print(max_start_date)
		print(min_end_date)
		print(min(dates) <= max_start_date and max(dates) >= min_end_date)

# Wrapper function for using ERMIN module to validate data
# But using climate_trace specification.
# This means there is at least one additional field type, 
# namely {iso3_country} that needs special checking.
def check_input_dataframe(input_df,
                          spec_file=None,
                          repair=True,
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
    warnings, newerrors, newdf = ev.check_input_dataframe(input_df, spec_file = spec_file,
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
