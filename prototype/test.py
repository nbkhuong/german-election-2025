import pandas as pd

# Your input dictionary
data = {
    '1000': {'Date': '2018-08-31',
             'Institute_ID': '16',
             'Method_ID': '0',
             'Parliament_ID': '7',
             'Results': {'0': 3.9,
                         '101': 31.1,
                         '2': 23.9,
                         '3': 7.1,
                         '4': 13.4,
                         '5': 7.9,
                         '7': 12.7},
             'Survey_Period': {'Date_End': '2018-08-30',
                               'Date_Start': '2018-08-14'},
             'Surveyed_Persons': '4532',
             'Tasker_ID': '76'},
    '1001': {'Date': '2018-08-31',
             'Institute_ID': '5',
             'Method_ID': '0',
             'Parliament_ID': '13',
             'Results': {'0': 4,
                         '101': 28,
                         '2': 11,
                         '3': 7,
                         '4': 7,
                         '5': 18,
                         '7': 25},
             'Survey_Period': {'Date_End': '2018-08-30',
                               'Date_Start': '2018-08-27'},
             'Surveyed_Persons': '1040',
             'Tasker_ID': '4'}
}

df = []
for key, value in data.items():
    row = {'survey_id': key}
    for k, v in value.items():
        if k == "Results":
            for party_id, result in v.items():
                row['party_id'] = party_id
                row['survey_result_by_percent'] = result
                row['survey_publish_date'] = value['Date']
                row['institute_id'] = value['Institute_ID']
                row['parliament_id'] = value['Parliament_ID']
                row['method_id'] = value['Method_ID']
                row['survey_start_date'] = value['Survey_Period']['Date_Start']
                row['survey_end_date'] = value['Survey_Period']['Date_End']
                row['total_surveyees'] = value['Surveyed_Persons']
                row['tasker_id'] = value['Tasker_ID']
                
                df.extend(row)
            break


dfs = pd.DataFrame(df)
# Show the resulting DataFrame
print(dfs)