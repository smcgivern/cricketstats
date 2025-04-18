format_lookup = {'men_test': 1, 'men_odi': 2, 'men_t20i': 3, 'women_test': 8, 'women_odi': 9, 'women_t20i':10}
# format_lookup = {'women_odi': 9, 'women_t20i':10}

format_length = {'men_test_batting': 11,
                'men_test_bowling': 11,
                'men_odi_bowling': 11,
                'men_odi_batting': 11,
                'men_t20i_batting': 11,
                'men_t20i_bowling': 10,
                'women_test_batting': 11,
                'women_test_bowling': 11,
                'women_odi_batting': 11,
                'women_odi_bowling': 10,
                'women_t20i_batting': 11,
                'women_t20i_bowling': 10,
                'men_test_team': 10,
                'men_odi_team': 9,
                'men_t20i_team': 9,
                'women_test_team': 10,
                'women_odi_team': 9,
                'women_t20i_team': 9,
                }

team_lookup = {'AUS': 'Australia',
            'ENG': 'England',
            'SA': 'South Africa',
            'WI': 'West Indies',
            'NZ': 'New Zealand',
            'INDIA': 'India',
            'IND': 'India',
            'PAK': 'Pakistan',
            'SL': 'Sri Lanka',
            'ZIM': 'Zimbabwe',
            'BDESH': 'Bangladesh',
            'BAN': 'Bangladesh',
            'ICC': 'ICC World XI',
            'IRE': 'Ireland',
            'AFG': 'Afghanistan',
            'EAf': 'East Africa',
            'CAN': 'Canada',
            'USA': 'U.S.A.',
            'NEPAL': 'Nepal',
            'OMAN': 'Oman',
            'OMA': 'Oman',
            'NAM': 'Namibia',
            'UAE': 'U.A.E.',
            'SCOT': 'Scotland',
            'PNG': 'P.N.G.',
            'NL': 'Netherlands',
            'NED': 'Netherlands',
            'HKG': 'Hong Kong',
            'KENYA': 'Kenya',
            'BMUDA': 'Bermuda',
            'Asia': 'Asia XI',
            'Afr': 'Africa XI',
            'World': 'ICC World XI',
            'BHR': 'Bahrain',
            'Saudi': 'Saudi Arabia',
            'Mald': 'Maldives',
            'KUW': 'Kuwait',
            'QAT': 'Qatar',
            'PHI': 'Philippines',
            'VAN': 'Vanuatu',
            'MLT': 'Malta',
            'ESP': 'Spain',
            'Mex': 'Mexico',
            'Blz': 'Belize',
            'CRC': 'Costa Rica',
            'PNM': 'Panama',
            'GER': 'Germany',
            'Belg': 'Belgium',
            'NGA': 'Nigeria',
            'Ghana': 'Ghana',
            'UGA': 'Uganda',
            'Botsw': 'Botswana',
            'BOT': 'Botswana',
            'ITA': 'Italy',
            'JER': 'Jersey',
            'GUE': 'Guernsey',
            'NOR': 'Norway',
            'DEN': 'Denmark',
            'THAI': 'Thailand',
            'MAL': 'Malaysia',
            'Samoa': 'Samoa',
            'Fin': 'Finland',
            'SGP': 'Singapore',
            'Caym': 'Cayman Islands',
            'ROM': 'Romania',
            'Aut': 'Austria',
            'TKY': 'Turkey',
            'LUX': 'Luxembourg',
            'CZK-R': 'Czech Republic',
            'Arg': 'Argentina',
            '': 'Brazil',
            'Chile': 'Chile',
            'Peru': 'Peru',
            'Serb': 'Serbia',
            'BUL': 'Bulgaria',
            'PORT': 'Portugal',
            'GIBR': 'Gibralta',
            'Moz': 'Mozambique',
            'MOZ': 'Mozambique',
            'MWI': 'Malawi',
            'BHU': 'Bhutan',
            'Iran': 'Iran',
            'IOM': 'Isle of Man',
            'DnWmn': 'Denmk Women',
            'SAMwn': 'Samoa Women',
            'INA-W': 'Indonesia Wm',
            'MYA-W': 'Myanmar Wmn',
            'CRC-W': 'CRC-W',
            'MEX-W': 'Mexico Women',
            'NAM-W': 'NAM Women',
            'RWA-W': 'Rwanda Women',
            'SLE-W': 'Sierra L Wmn',
            'MOZ-W': 'MOZ Women',
            'ZIM-W': 'Zim Women',
            'NGA-W': 'Nigeria Wmn',
            'VAN-W': 'Vanuatu Wome',
            'UGA-W': 'Uganda Women',
            'PNG-W': 'PNG Wmn',
            'JPN-W': 'Japan Women',
            'FJI-W': 'Fiji Women',
            'KEN-W': 'Kenya Women',
            'TZN-W': 'Tanzania Wmn',
            'PAK-W': 'PAK Women',
            'CAN-W': 'Canada Women',
            'USA-W': 'USA Women',
            'SA-W': 'SA Women',
            'IRE-W': 'Ire Women',
            'WI-W': 'WI Women',
            'JEY-W': 'Jersey Women',
            'GUN-W': 'Guernsey Wmn',
            'MLI-W': 'Mali Women',
            'ENG-W': 'ENG Women',
            'SCO-W': 'Scot Women',
            'GER-W': 'GER Women',
            'NL-W': 'Neth Women',
            'AUS-W': 'AUS Women',
            'FRA-W': 'France Women',
            'NOR-W': 'Norway Women',
            'AUT-W': 'Austria Wmn',
            'THI-W': 'Thai Women',
            'BD-W': 'Bdesh Wmn',
            'MAL-W': 'Mal Women',
            'SIN-W': 'Spore Women',
            'NZ-W': 'NZ Women',
            'SL-W': 'SL Women',
            'IND-W': 'India Women',
            'KOR-W': 'SK Women',
            'HKG-W': 'HKG Women',
            'CHN-W': 'China Women',
            'ARG-W': 'Arg Women',
            'PER-W': 'Peru Women',
            'CHI-W': 'Chile Women',
            'YEWmn': 'Young ENG Women',
            'IntWn': 'International XI Women',
            'TTWmn': 'Trinidiad Tobago Women',
            'JamWn': 'Jamacia Women',
            'UAE-W': 'UAE Women',
            'BOT-W': 'Botswana Women',
            'LES-W': 'Lesotho Women',
            'MLW-W': 'Malawi Women',
            'BRA-W': 'Brazil Women',
            'NEP-W': 'Nepal Women',
            'BHU-W': 'Bhutan Women',
            'KUW-W': 'Kuwait Women',
            'MDV-W': 'Maldives Women',
            'BLZ-W': 'Belize Women',
            'PHI-W': 'Philippines Women',
            'OMA-W': 'Oman Women',
            'QAT-W': 'Qatar Women',
            'SRB': 'Serbia',
            'HUN': 'Hungary',
            'RWN': 'Rwanda',
            'SWE': 'Sweden',
            'Fran': 'France',
            'IOM': 'Isle of Man',
            'Iran': 'Iran',
            'BHU': 'Bhutan',
            'GIBR': 'Gibraltar',
            'PORT': 'Portugal',
            'GRC': 'Greece',
            'BUL': 'Bulgaria',
            'ESW-W': 'Eswatini Women',
            'SWE-W': 'Sweden Women',
            'ITA-W': 'Italy Women',
            'CMR-W': 'Cameroon Women',
            'EST': 'Estonia',
            'CYP': 'Cyprus',
            'LES': 'Lesotho',
            'SEY': 'Seychelles',
            'SLE': 'Sierra Leone',
            'SUI': 'Switzerland',
            'TAN': 'Tanzania',
            'CAM': 'Cameroon',
            'Bhm': 'Bahamas',
            'BEL-W': 'Belgium Women',
            'BHR-W': 'Belgium Women',
            'KSA-W': 'Saudi Arabia Women',
            }
