import pandas as pd

original_frame = pd.read_csv('trustpilot-task.csv', encoding = 'unicode_escape')
new_frame = pd.read_csv('trustpilot-results.csv', encoding = 'unicode_escape')

companies_merged_results = pd.merge(original_frame, new_frame, how='outer', on='url')

companies_merged_results.to_csv('trustpilot-merged.csv')
