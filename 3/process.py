''' Libraries '''
import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm


''' Functions '''
def process_deleted_file(year):
    print(f"Process deleted {year}: Reading original data...")
    original_data_path = f"original_data/{year}_刪除無分析師預測後剩餘股吧名單.xlsx"
    original_data = pd.read_excel(original_data_path, engine='openpyxl')
    original_data_values = original_data.values

    deleted_data_values = []
    for row in tqdm(original_data_values, desc=f"Process deleted {year}: Processing data", ascii=True):
        deleted_row = []
        
        edited_post_date = row[0].strftime('%Y')
        post_date_month = row[0].strftime('%m')
        if   int(post_date_month) <= 3: edited_post_date = f'{edited_post_date}01' # Q1
        elif int(post_date_month) <= 6: edited_post_date = f'{edited_post_date}02' # Q2
        elif int(post_date_month) <= 9: edited_post_date = f'{edited_post_date}03' # Q3
        else                          : edited_post_date = f'{edited_post_date}04' # Q4

        deleted_row.append(edited_post_date)  # EditedPostDate
        deleted_row.append(row[1])            # Stkcd
        # deleted_row.append(row[2])          # TotalPosts
        # deleted_row.append(row[3])          # AvgReadings
        # deleted_row.append(row[4])          # AvgComments
        # deleted_row.append(row[5])          # AvgNetComments
        # deleted_row.append(row[6])          # AvgThumbUps
        # deleted_row.append(row[7])          # AvgUserBarAge
        deleted_row.append(row[8])            # AvgUserInfluIndex
        # deleted_row.append(row[9])          # AvgUserPosts
        # deleted_row.append(row[10])         # AvgUserComments
        # deleted_row.append(row[11])         # Indcd
        deleted_data_values.append(deleted_row)

    deleted_data_columns = ['EditedPostDate', 'Stkcd', 'AvgUserInfluIndex']
    deleted_data_output = pd.DataFrame(deleted_data_values, columns=deleted_data_columns)
    print(f"Process deleted {year}: Saving processed data...")
    deleted_data_output.to_csv(f"processed_data/deleted_{year}.csv")
    print(f"Process deleted {year}: Done.\n")


def filter_deleted_file(year):
    print(f"Filter deleted {year}: Reading processed data...")
    deleted_data_path = f"processed_data/deleted_{year}.csv"
    deleted_data = pd.read_csv(deleted_data_path, index_col=0)
    deleted_data_values = deleted_data.values

    more_data_values = []
    less_data_values = []
    more_existed_season_company = []
    less_existed_season_company = []
    for row in tqdm(deleted_data_values, desc=f"Filter deleted {year}: Filtering data", ascii=True):
        season_company = f'{row[0]}{row[1]}'
        if int(row[2]) >= 2.5 and season_company not in more_existed_season_company:
            more_existed_season_company.append(season_company)
            more_data_values.append([ int(row[1]), int(row[0]) ])
            if season_company in less_existed_season_company:
                less_existed_season_company.remove(season_company)
                less_data_values.remove([ int(row[1]), int(row[0]) ])
        elif int(row[2]) < 2.5 and season_company not in less_existed_season_company and season_company not in more_existed_season_company:
            less_existed_season_company.append(season_company)
            less_data_values.append([ int(row[1]), int(row[0]) ])

    print(f"Filter deleted {year}: Sorting filtered data...")
    more_data_values = sorted(more_data_values, key=lambda e: e[1])
    more_data_values = sorted(more_data_values, key=lambda e: e[0])
    less_data_values = sorted(less_data_values, key=lambda e: e[1])
    less_data_values = sorted(less_data_values, key=lambda e: e[0])

    filtered_data_columns = ['Stkcd', 'EditedPostDate']

    more_data_output = pd.DataFrame(more_data_values, columns=filtered_data_columns)
    print(f"Filter deleted {year}: Saving more data...")
    more_data_output.to_csv(f"processed_data/deleted_{year}_more.csv")

    less_data_output = pd.DataFrame(less_data_values, columns=filtered_data_columns)
    print(f"Filter deleted {year}: Saving less data...")
    less_data_output.to_csv(f"processed_data/deleted_{year}_less.csv")

    print(f"Filter deleted {year}: Done.\n")


def process_prediction_file(year):
    print(f"Process prediction {year}: Reading original data...")
    original_data_path = f"original_data/{year}多年期盈餘預測.xlsx"
    original_data = pd.read_excel(original_data_path, engine='openpyxl')
    original_data_columns = original_data.columns
    original_data_values = original_data.values
    if year in [ 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2020 ]: original_data_values = original_data_values[2:]

    prediction_data_values = []
    for row in tqdm(original_data_values, desc=f"Process prediction {year}: Processing data", ascii=True):

        # Stkcd (A)
        Stkcd = row[0]

        # Rptdt (B)
        if year in [ 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2020 ]: Rptdt = datetime.strptime(row[1], '%Y-%m-%d').strftime('%Y%m%d')
        else: Rptdt = row[1].strftime('%Y%m%d')

        # InstitutionID (G) + AnanmID (D)
        institution_ananm_id = [ str(row[6]) ]
        ananm_ids = str(row[3])
        if ananm_ids != 'nan':
            ananm_ids = ananm_ids.split(',')
            ananm_ids.sort()
            for ananm_id in ananm_ids: institution_ananm_id.append(ananm_id)
        institution_ananm_id = '-'.join(institution_ananm_id)
        
        # Fenddt (C)
        if year in [ 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2020 ]: EditedFenddt = datetime.strptime(row[2], '%Y-%m-%d').strftime('%Y')
        else: EditedFenddt = row[2].strftime('%Y%m%d')

        prediction_data_values.append([ Stkcd, Rptdt, institution_ananm_id, EditedFenddt ])

    prediction_data_columns = [ 'Stkcd', 'Rptdt', 'InstitutionAnanmID', 'EditedFenddt' ]
    prediction_data_output = pd.DataFrame(prediction_data_values, columns=prediction_data_columns)
    print(f"Process prediction {year}: Saving processed data...")
    prediction_data_output.to_csv(f"processed_data/prediction_{year}.csv")
    print(f"Process prediction {year}: Done.\n")


def filter_prediction_file(year):
    print(f"Filter prediction {year}: Reading processed data...")
    prediction_data_path = f"processed_data/prediction_{year}.csv"
    prediction_data = pd.read_csv(prediction_data_path, index_col=0)
    prediction_data_values = prediction_data.values

    prediction_data = []
    for row in tqdm(prediction_data_values, desc=f"Filter prediction {year}: Transforming data", ascii=True):
        if   int(str(row[1])[4:6]) <= 3: EditedRptdt = f'{str(row[1])[:4]}01'  # Q1
        elif int(str(row[1])[4:6]) <= 6: EditedRptdt = f'{str(row[1])[:4]}02'  # Q2
        elif int(str(row[1])[4:6]) <= 9: EditedRptdt = f'{str(row[1])[:4]}03'  # Q3
        else                           : EditedRptdt = f'{str(row[1])[:4]}04'  # Q4
        prediction_data.append({
            'Keep'              : True,
            'Stkcd'             : row[0],
            'Rptdt'             : row[1],
            'EditedRptdt'       : EditedRptdt,
            'InstitutionAnanmID': row[2],
            # 'EditedFenddt'      : row[3],
        })
    prediction_data = sorted(prediction_data, key=lambda e: e['InstitutionAnanmID'])

    for index in tqdm(range(len(prediction_data)-1), desc=f'Filter prediction {year}: Filtering data', ascii=True):
        if prediction_data[index]['Stkcd'] == prediction_data[index+1]['Stkcd'] and \
           prediction_data[index]['EditedRptdt'] == prediction_data[index+1]['EditedRptdt'] and \
           prediction_data[index]['InstitutionAnanmID'] == prediction_data[index+1]['InstitutionAnanmID']:  # and \
        #    prediction_data[index]['EditedFenddt'] == prediction_data[index+1]['EditedFenddt']:
            prediction_data[index]['Keep'] = False
    filtered_data = list(filter(lambda e: e['Keep'], prediction_data))
    filtered_data_values = [
        # [ d['Stkcd'], d['EditedRptdt'], d['InstitutionAnanmID'], d['EditedFenddt'] ] for d in filtered_data
        [ d['Stkcd'], d['EditedRptdt'], d['InstitutionAnanmID'] ] for d in filtered_data
    ]
    # filtered_data_values = sorted(filtered_data_values, key=lambda e: e[3])
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[2])
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[1])
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[0])

    # filtered_data_columns = [ 'Stkcd', 'EditedRptdt', 'InstitutionAnanmID', 'EditedFenddt' ]
    filtered_data_columns = [ 'Stkcd', 'EditedRptdt', 'InstitutionAnanmID' ]
    filtered_data_output = pd.DataFrame(filtered_data_values, columns=filtered_data_columns)
    print(f"Filter prediction {year}: Saving filtered data...")
    filtered_data_output.to_csv(f"processed_data/prediction_{year}_filtered.csv")
    print(f"Filter prediction {year}: Done.\n")


def integrate_all_filtered_deleted_data(years):

    all_more_deleted_data = []
    all_less_deleted_data = []

    for year in years:
        print(f"Integrate all filtered deleted: Reading more {year} deleted data...")
        deleted_data_path = f"processed_data/deleted_{year}_more.csv"
        for row in pd.read_csv(deleted_data_path, index_col=0).values:
            all_more_deleted_data.append(row)
            
        print(f"Integrate all filtered deleted: Reading less {year} deleted data...")
        deleted_data_path = f"processed_data/deleted_{year}_less.csv"
        for row in pd.read_csv(deleted_data_path, index_col=0).values:
            all_less_deleted_data.append(row)

    print(f"Integrate all filtered deleted: Sorting all more deleted data...")
    all_more_deleted_data = sorted(all_more_deleted_data, key=lambda e: e[1])
    all_more_deleted_data = sorted(all_more_deleted_data, key=lambda e: e[0])
    all_more_deleted_data = [
        [ f'{int(row[0]):06d}', row[1] ] for row in all_more_deleted_data
    ]

    print(f"Integrate all filtered deleted: Sorting all less deleted data...")
    all_less_deleted_data = sorted(all_less_deleted_data, key=lambda e: e[1])
    all_less_deleted_data = sorted(all_less_deleted_data, key=lambda e: e[0])
    all_less_deleted_data = [
        [ f'{int(row[0]):06d}', row[1] ] for row in all_less_deleted_data
    ]

    filtered_deleted_data_columns = ['Stkcd', 'EditedPostDate']
    all_more_deleted_data_output = pd.DataFrame(all_more_deleted_data, columns=filtered_deleted_data_columns)
    all_less_deleted_data_output = pd.DataFrame(all_less_deleted_data, columns=filtered_deleted_data_columns)

    print(f"Integrate all filtered deleted: Saving all more deleted data...")
    all_more_deleted_data_output.to_excel(f"processed_data/all_deleted_more.xlsx")

    print(f"Integrate all filtered deleted: Saving all less deleted data...")
    all_less_deleted_data_output.to_excel(f"processed_data/all_deleted_less.xlsx")

    print(f"Integrate all filtered deleted: Done.\n")


def integrate_all_filtered_prediction_data(years):

    all_filtered_prediction_data = []
    for year in years:
        print(f"Integrate all filtered prediction: Reading filtered {year} prediction data...")
        filtered_prediction_data_path = f"processed_data/prediction_{year}_filtered.csv"
        for row in pd.read_csv(filtered_prediction_data_path, index_col=0).values:
            all_filtered_prediction_data.append(row)
    
    print(f"Integrate all filtered prediction: Sorting all filtered prediction data...")
    # all_filtered_prediction_data = sorted(all_filtered_prediction_data, key=lambda e: e[3])
    all_filtered_prediction_data = sorted(all_filtered_prediction_data, key=lambda e: e[2])
    all_filtered_prediction_data = sorted(all_filtered_prediction_data, key=lambda e: e[1])
    all_filtered_prediction_data = sorted(all_filtered_prediction_data, key=lambda e: e[0])

    all_filtered_prediction_data = [
        # [ f'{int(row[0]):06d}', row[1], row[2], row[3] ] for row in all_filtered_prediction_data
        [ f'{int(row[0]):06d}', row[1], row[2] ] for row in all_filtered_prediction_data
    ]

    # all_filtered_prediction_data_columns = [ 'Stkcd', 'EditedRptdt', 'InstitutionAnanmID', 'Fenddt' ]
    all_filtered_prediction_data_columns = [ 'Stkcd', 'EditedRptdt', 'InstitutionAnanmID' ]
    all_filtered_prediction_data_output = pd.DataFrame(all_filtered_prediction_data, columns=all_filtered_prediction_data_columns)
    print(f"Integrate all filtered prediction: Saving all filtered prediction data...")
    all_filtered_prediction_data_output.to_excel(f"processed_data/all_prediction_filtered.xlsx")
    print(f"Integrate all filtered prediction: Done.\n")


def __previous_season(season):
    if str(season)[-2:] == '01': return f'{(int(str(season)[:4])-1)}04'
    else: return str(int(str(season))-1)


def __next_season(season):
    if str(season)[-2:] == '04': return f'{(int(str(season)[:4])+1)}01'
    else: return str(int(str(season))+1)


def filter_significance(more_or_less='more', longlasting=True):

    print(f"Filter significance: Reading all deleted data - {more_or_less}...")
    all_deleted_data_path = f"processed_data/all_deleted_{more_or_less}.xlsx"
    all_deleted_data = pd.read_excel(all_deleted_data_path, index_col=0, engine='openpyxl').values

    print(f"Filter significance: Reading all filtered prediction data...")
    all_filtered_prediction_data_path = f"processed_data/all_prediction_filtered.xlsx"
    all_filtered_prediction_data = pd.read_excel(all_filtered_prediction_data_path, index_col=0, engine='openpyxl').values

    all_prediction_dictionaries = {}
    for row in tqdm(all_filtered_prediction_data, desc=f'Filter significance: Transforming data - all filtered prediction dictionaries', ascii=True):
        Stkcd              = row[0]
        EditedRptdt        = row[1]
        InstitutionAnanmID = row[2]
        if Stkcd not in all_prediction_dictionaries.keys():
            all_prediction_dictionaries[Stkcd] = {}
        if InstitutionAnanmID not in all_prediction_dictionaries[Stkcd].keys():
            all_prediction_dictionaries[Stkcd][InstitutionAnanmID] = []
        all_prediction_dictionaries[Stkcd][InstitutionAnanmID].append({
            'Keep'        : False,
            'EditedRptdt' : EditedRptdt,
        })

    for row in tqdm(all_deleted_data, desc=f'Filter significance: Filtering data (1) - {more_or_less}', ascii=True):
        Stkcd               = int(row[0])
        thisEditedRptdt     = int(row[1])
        previousEditedRptdt = int(__previous_season(thisEditedRptdt))
        nextEditedRptdt     = int(__next_season(thisEditedRptdt))
        if Stkcd in all_prediction_dictionaries.keys():
            for InstitutionAnanmID in all_prediction_dictionaries[Stkcd].keys():
                if longlasting:
                    this_season_index     = None
                    previous_season_index = None
                    next_season_index     = None
                    for index, d in enumerate(all_prediction_dictionaries[Stkcd][InstitutionAnanmID]):
                        if   d['EditedRptdt'] == thisEditedRptdt:     this_season_index     = index
                        elif d['EditedRptdt'] == previousEditedRptdt: previous_season_index = index
                        elif d['EditedRptdt'] == nextEditedRptdt:     next_season_index     = index
                    # if previous_season_index != None and this_season_index != None and next_season_index != None:
                    #     all_prediction_dictionaries[Stkcd][InstitutionAnanmID][this_season_index]['Keep'] = True
                    # if previous_season_index != None and this_season_index != None:
                    #     all_prediction_dictionaries[Stkcd][InstitutionAnanmID][this_season_index]['Keep'] = True
                    if previous_season_index != None and next_season_index != None:
                        all_prediction_dictionaries[Stkcd][InstitutionAnanmID][next_season_index]['Keep'] = True
                else:
                    this_index = None
                    previous_exist = False
                    for index, d in enumerate(all_prediction_dictionaries[Stkcd][InstitutionAnanmID]):
                        # if   d['EditedRptdt'] == thisEditedRptdt:
                        if   d['EditedRptdt'] == nextEditedRptdt:
                            this_index = index
                        elif d['EditedRptdt'] < thisEditedRptdt:
                            previous_exist = True
                            break
                    if this_index != None and previous_exist == False:
                        all_prediction_dictionaries[Stkcd][InstitutionAnanmID][this_index]['Keep'] = True

    for Stkcd in tqdm(all_prediction_dictionaries.keys(), desc=f'Filter significance: Filtering data (2) - {more_or_less}', ascii=True):
        for InstitutionAnanmID in all_prediction_dictionaries[Stkcd].keys():
            all_prediction_dictionaries[Stkcd][InstitutionAnanmID] = list(filter(lambda d: d['Keep'], all_prediction_dictionaries[Stkcd][InstitutionAnanmID]))

    print(f"Filter significance: Sorting all prediction data - {more_or_less}...")
    all_prediction_data = []
    for Stkcd in all_prediction_dictionaries.keys():
        for InstitutionAnanmID in all_prediction_dictionaries[Stkcd].keys():
            for d in all_prediction_dictionaries[Stkcd][InstitutionAnanmID]:
                all_prediction_data.append([ Stkcd, d['EditedRptdt'], InstitutionAnanmID ])
    all_prediction_data = sorted(all_prediction_data, key=lambda e: e[2])
    all_prediction_data = sorted(all_prediction_data, key=lambda e: e[1])
    all_prediction_data = sorted(all_prediction_data, key=lambda e: e[0])

    if longlasting: xlsx_name = f"significance_{more_or_less}"
    else          : xlsx_name = f"significance_appear_{more_or_less}"

    all_prediction_data_columns = [ 'Stkcd', 'EditedRptdt', 'InstitutionAnanmID' ]
    all_prediction_data_output = pd.DataFrame(all_prediction_data, columns=all_prediction_data_columns)
    print(f"Filter significance: Saving all {more_or_less} prediction data...")
    all_prediction_data_output.to_excel(f"processed_data/{xlsx_name}.xlsx")

    print(f"Filter significance: Done.\n")
    return


def count_significance_amount():

    print(f"Get significance amount: Reading data - more significance data...")
    more_significance_data_path = f"processed_data/significance_more.xlsx"
    more_significance_data = pd.read_excel(more_significance_data_path, index_col=0, engine='openpyxl').values

    print(f"Get significance amount: Reading data - more appeared significance data...")
    more_appear_significance_data_path = f"processed_data/significance_appear_more.xlsx"
    more_appear_significance_data = pd.read_excel(more_appear_significance_data_path, index_col=0, engine='openpyxl').values

    print(f"Get significance amount: Reading data - less significance data...")
    less_significance_data_path = f"processed_data/significance_less.xlsx"
    less_significance_data = pd.read_excel(less_significance_data_path, index_col=0, engine='openpyxl').values

    print(f"Get significance amount: Reading data - less appeared significance data...")
    less_appear_significance_data_path = f"processed_data/significance_appear_less.xlsx"
    less_appear_significance_data = pd.read_excel(less_appear_significance_data_path, index_col=0, engine='openpyxl').values

    significance_amount = {}
    data_dict = {
        'more significance data'       : more_significance_data,
        'less significance data'       : less_significance_data,
        'more appear significance data': more_appear_significance_data,
        'less appear significance data': less_appear_significance_data,
    }
    for data_name, data in data_dict.items():
        significance_amount_tmp = {}
        for row in tqdm(data, desc=f"Get significance amount: Transforming data - {data_name}", ascii=True):
            Stkcd              = f"{row[0]:06d}"
            EditedRptdt        = row[1]
            InstitutionAnanmID = row[2]
            if Stkcd not in significance_amount_tmp.keys():
                significance_amount_tmp[Stkcd] = {}
            if EditedRptdt not in significance_amount_tmp[Stkcd].keys():
                significance_amount_tmp[Stkcd][EditedRptdt] = []
            if InstitutionAnanmID not in significance_amount_tmp[Stkcd][EditedRptdt]:
                significance_amount_tmp[Stkcd][EditedRptdt].append(InstitutionAnanmID)
        for Stkcd in significance_amount_tmp.keys():
            for EditedRptdt in significance_amount_tmp[Stkcd]:
                significance_amount_tmp[Stkcd][EditedRptdt] = len(significance_amount_tmp[Stkcd][EditedRptdt])
            significance_amount_tmp[Stkcd] = dict(sorted(significance_amount_tmp[Stkcd].items(), key=lambda i: i[0]))
        significance_amount[data_name] = dict(sorted(significance_amount_tmp.items(), key=lambda i: i[0]))

    significance_amount_data = {}
    for data_name in significance_amount.keys():
        significance_amount_data[data_name] = []
        for Stkcd in significance_amount[data_name].keys():
            for EditedRptdt in significance_amount[data_name][Stkcd]:
                significance_amount_data[data_name].append([ Stkcd, EditedRptdt, significance_amount[data_name][Stkcd][EditedRptdt] ])
    
    for data_name in significance_amount_data.keys():
        if   data_name == 'more significance data'       : xlsx_name = "significance_amount_more"
        elif data_name == 'less significance data'       : xlsx_name = "significance_amount_less"
        elif data_name == 'more appear significance data': xlsx_name = "significance_amount_appear_more"
        elif data_name == 'less appear significance data': xlsx_name = "significance_amount_appear_less"
        significance_amount_data_columns = [ 'Stkcd', 'EditedRptdt', 'SignificanceAmount' ]
        significance_amount_data_output = pd.DataFrame(significance_amount_data[data_name], columns=significance_amount_data_columns)
        print(f"Get significance amount: Saving significance amount - {data_name}...")
        significance_amount_data_output.to_excel(f"processed_data/{xlsx_name}.xlsx")

    return


''' Execution '''
if __name__ == "__main__":

    # os.system('clear')
    if not os.path.exists('processed_data'): os.makedirs('processed_data')

    deleted_files_years = [ 2017, 2018, 2019 ]
    prediction_files_years = [ 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020 ]

    for year in deleted_files_years:
        # process_deleted_file(year)
        # filter_deleted_file(year)
        pass
    # integrate_all_filtered_deleted_data(deleted_files_years)

    for year in prediction_files_years:
        # process_prediction_file(year)
        # filter_prediction_file(year)
        pass
    # integrate_all_filtered_prediction_data(prediction_files_years)

    filter_significance(more_or_less='more', longlasting=True)
    filter_significance(more_or_less='less', longlasting=True)
    # filter_significance(more_or_less='more', longlasting=False)
    # filter_significance(more_or_less='less', longlasting=False)
    count_significance_amount()