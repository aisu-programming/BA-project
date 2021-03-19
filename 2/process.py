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

    filtered_data_values = []
    existed_season_company = []
    for row in tqdm(deleted_data_values, desc=f"Filter deleted {year}: Filtering data", ascii=True):
        season_company = f'{row[0]}{row[1]}'
        if int(row[2]) >= 2.5 and season_company not in existed_season_company:
            existed_season_company.append(season_company)
            filtered_data_values.append([ row[1], row[0] ])

    print(f"Filter deleted {year}: Sorting filtered data...")
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[1])
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[0])

    filtered_data_columns = ['Stkcd', 'EditedPostDate']
    filtered_data_output = pd.DataFrame(filtered_data_values, columns=filtered_data_columns)
    print(f"Filter deleted {year}: Saving filtered data...")
    filtered_data_output.to_csv(f"processed_data/deleted_{year}_filtered.csv")
    print(f"Filter deleted {year}: Done.\n")


def process_recommended_file(year):
    print(f"Process recommended {year}: Reading original data...")
    original_data_path = f"original_data/分析師股票推薦{year}.xlsx"
    original_data = pd.read_excel(original_data_path, engine='openpyxl')
    original_data_columns = original_data.columns
    original_data_values = original_data.values

    deleted_row_amount = 0
    recommended_data_values = []
    for row in tqdm(original_data_values, desc=f"Process recommended {year}: Processing data", ascii=True):
        
        # E - Fenddt
        if row[4] != f'{year}-12-31': continue

        # J - Stdrank
        if   row[9] == '': 
            deleted_row_amount += 1
            continue
        elif row[9] == '卖出': Stdrank = 1
        elif row[9] == '减持': Stdrank = 2
        elif row[9] == '中性': Stdrank = 3
        elif row[9] == '买入': Stdrank = 4
        elif row[9] == '增持': Stdrank = 5

        # InstitutionID + AnanmID
        if original_data_columns[5] == 'AnanmID': ananm_ids = str(row[5])
        else: ananm_ids = str(row[6])
        if ananm_ids == 'nan':
            deleted_row_amount += 1
            continue
        ananm_ids = ananm_ids.split(',')
        ananm_ids.sort()
        institution_ananm_id = [ str(row[7]) ]
        for ananm_id in ananm_ids: institution_ananm_id.append(ananm_id)
        institution_ananm_id = '-'.join(institution_ananm_id)

        # D - Rptdt
        Rptdt = datetime.strptime(row[3], '%Y-%m-%d').strftime('%Y%m%d')

        recommended_data_values.append([ row[0], Rptdt, institution_ananm_id, Stdrank ])

    recommended_data_columns = ['Stkcd', 'Rptdt', 'InstitutionAnanmID', 'Stdrank']
    recommended_data_output = pd.DataFrame(recommended_data_values, columns=recommended_data_columns)
    print(f"Process recommended {year}: Saving processed data...")
    recommended_data_output.to_csv(f"processed_data/recommended_{year}.csv")
    print(f"Process recommended {year}: Done. Deleted row amount = {deleted_row_amount}.\n")


def filter_recommended_file(year):
    print(f"Filter recommended {year}: Reading processed data...")
    recommended_data_path = f"processed_data/recommended_{year}.csv"
    recommended_data = pd.read_csv(recommended_data_path, index_col=0)
    recommended_data_values = recommended_data.values

    recommended_data_dictionaries = []
    for row in tqdm(recommended_data_values, desc=f"Filter recommended {year}: Transforming data", ascii=True):
        if   int(str(row[1])[4:6]) <= 3: EditedRptdt = f'{str(row[1])[:4]}01'  # Q1
        elif int(str(row[1])[4:6]) <= 6: EditedRptdt = f'{str(row[1])[:4]}02'  # Q2
        elif int(str(row[1])[4:6]) <= 9: EditedRptdt = f'{str(row[1])[:4]}03'  # Q3
        else                           : EditedRptdt = f'{str(row[1])[:4]}04'  # Q4
        recommended_data_dictionaries.append({
            'Keep'              : True,
            'Stkcd'             : row[0],
            'Rptdt'             : row[1],
            'EditedRptdt'       : EditedRptdt,
            'InstitutionAnanmID': row[2],
            'Stdrank'           : row[3],
        })
    recommended_data_dictionaries = sorted(recommended_data_dictionaries, key=lambda e: e['InstitutionAnanmID'])

    for index in tqdm(range(len(recommended_data_dictionaries)-1), desc=f'Filter recommended {year}: Filtering data', ascii=True):
        if recommended_data_dictionaries[index]['Stkcd'] == recommended_data_dictionaries[index+1]['Stkcd'] and \
           recommended_data_dictionaries[index]['EditedRptdt'] == recommended_data_dictionaries[index+1]['EditedRptdt'] and \
           recommended_data_dictionaries[index]['InstitutionAnanmID'] == recommended_data_dictionaries[index+1]['InstitutionAnanmID']:
            recommended_data_dictionaries[index]['Keep'] = False
    filtered_data_dictionaries = list(filter(lambda e: e['Keep'], recommended_data_dictionaries))
    filtered_data_values = [
        [ d['Stkcd'], d['EditedRptdt'], d['InstitutionAnanmID'], d['Stdrank'] ] for d in filtered_data_dictionaries
    ]
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[1])
    filtered_data_values = sorted(filtered_data_values, key=lambda e: e[0])

    filtered_data_columns = ['Stkcd', 'EditedRptdt', 'InstitutionAnanmID', 'Stdrank']
    filtered_data_output = pd.DataFrame(filtered_data_values, columns=filtered_data_columns)
    print(f"Filter recommended {year}: Saving filtered data...")
    filtered_data_output.to_csv(f"processed_data/recommended_{year}_filtered.csv")
    print(f"Filter recommended {year}: Done.\n")


def integrate_all_filtered_deleted_data(years):

    all_filtered_deleted_data = []
    for year in years:
        print(f"Integrate all filtered deleted: Reading filtered {year} deleted data...")
        filtered_deleted_data_path = f"processed_data/deleted_{year}_filtered.csv"
        for row in pd.read_csv(filtered_deleted_data_path, index_col=0).values:
            all_filtered_deleted_data.append(row)

    print(f"Integrate all filtered deleted: Sorting all filtered deleted data...")
    all_filtered_deleted_data = sorted(all_filtered_deleted_data, key=lambda e: e[1])
    all_filtered_deleted_data = sorted(all_filtered_deleted_data, key=lambda e: e[0])

    all_filtered_deleted_data = [
        [ f'{int(row[0]):06d}', row[1] ] for row in all_filtered_deleted_data
    ]

    filtered_deleted_data_columns = ['Stkcd', 'EditedPostDate']
    all_filtered_deleted_data_output = pd.DataFrame(all_filtered_deleted_data, columns=filtered_deleted_data_columns)
    print(f"Integrate all filtered deleted: Saving all filtered deleted data...")
    all_filtered_deleted_data_output.to_excel(f"processed_data/all_filtered_deleted.xlsx")
    print(f"Integrate all filtered deleted: Done.\n")


def integrate_all_filtered_recommended_data(years):

    all_filtered_recommended_data = []
    for year in years:
        print(f"Integrate all filtered recommended: Reading filtered {year} recommended data...")
        filtered_recommended_data_path = f"processed_data/recommended_{year}_filtered.csv"
        for row in pd.read_csv(filtered_recommended_data_path, index_col=0).values:
            all_filtered_recommended_data.append(row)
    
    print(f"Integrate all filtered recommended: Sorting all filtered recommended data...")
    all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda e: e[2])
    all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda e: e[1])
    all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda e: e[0])

    all_filtered_recommended_data = [
        [ f'{int(row[0]):06d}', row[1], row[2], row[3] ] for row in all_filtered_recommended_data
    ]

    all_filtered_recommended_data_columns = ['Stkcd', 'EditedRptdt', 'InstitutionAnanmID', 'Stdrank']
    all_filtered_recommended_data_output = pd.DataFrame(all_filtered_recommended_data, columns=all_filtered_recommended_data_columns)
    print(f"Integrate all filtered recommended: Saving all filtered recommended data...")
    all_filtered_recommended_data_output.to_excel(f"processed_data/all_filtered_recommended.xlsx")
    print(f"Integrate all filtered recommended: Done.\n")


def previous_season(season):
    if str(season)[-2:] == '01': return f'{(int(str(season)[:4])-1)}04'
    else: return str(int(str(season))-1)


def next_season(season):
    if str(season)[-2:] == '04': return f'{(int(str(season)[:4])+1)}01'
    else: return str(int(str(season))+1)


def summarize_significance_and_calculate_difference():

    print(f"Summarize significance: Reading all filtered deleted data...")
    all_filtered_deleted_data_path = f"processed_data/all_filtered_deleted.xlsx"
    all_filtered_deleted_data = pd.read_excel(all_filtered_deleted_data_path, index_col=0).values

    print(f"Summarize significance: Reading all filtered recommended data...")
    all_filtered_recommended_data_path = f"processed_data/all_filtered_recommended.xlsx"
    all_filtered_recommended_data = pd.read_excel(all_filtered_recommended_data_path, index_col=0).values

    all_filtered_recommended_dictionaries = {}
    for row in tqdm(all_filtered_recommended_data, desc=f'Summarize significance: Transforming data', ascii=True):

        Stkcd              = row[0]
        EditedRptdt        = row[1]
        InstitutionAnanmID = row[2]
        Stdrank            = row[3]

        if Stkcd not in all_filtered_recommended_dictionaries.keys():
            all_filtered_recommended_dictionaries[Stkcd] = {}
        if InstitutionAnanmID not in all_filtered_recommended_dictionaries[Stkcd].keys():
            all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID] = []
        all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID].append({
            'Keep'              : False,
            'EditedRptdt'       : EditedRptdt,
            'Stdrank'           : Stdrank,
        })

    for row in tqdm(all_filtered_deleted_data, desc=f'Summarize significance: Filtering data', ascii=True):

        Stkcd               = int(row[0])
        thisEditedRptdt     = int(row[1])
        previousEditedRptdt = int(previous_season(thisEditedRptdt))
        nextEditedRptdt     = int(next_season(thisEditedRptdt))

        if Stkcd in all_filtered_recommended_dictionaries.keys():
            for InstitutionAnanmID in all_filtered_recommended_dictionaries[Stkcd].keys():
                related_season_indexs = []
                for index, d in enumerate(all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID]):
                    if   d['EditedRptdt'] == thisEditedRptdt:     related_season_indexs.append(index)
                    elif d['EditedRptdt'] == previousEditedRptdt: related_season_indexs.append(index)
                    elif d['EditedRptdt'] == nextEditedRptdt:     related_season_indexs.append(index)
                if len(related_season_indexs) == 3:
                    for index in related_season_indexs:
                        all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID][index]['Keep'] = True

    for Stkcd in tqdm(all_filtered_recommended_dictionaries.keys(), desc=f'Summarize significance: Averaging data', ascii=True):
        for InstitutionAnanmID in all_filtered_recommended_dictionaries[Stkcd].keys():
            all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID] = list(filter(lambda d: d['Keep'], all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID]))


    # # 企管系教授額外要求的「第 5 步驟結果」
    # all_filtered_recommended_data = []
    # for Stkcd in tqdm(all_filtered_recommended_dictionaries.keys(), desc=f'Summarize significance: Transforming data for Step.5 answer', ascii=True):
    #     for InstitutionAnanmID in all_filtered_recommended_dictionaries[Stkcd].keys():
    #         for d in all_filtered_recommended_dictionaries[Stkcd][InstitutionAnanmID]:
    #             all_filtered_recommended_data.append([
    #                 f'{Stkcd:06d}', d['EditedRptdt'], InstitutionAnanmID, d['Stdrank']
    #             ])
    # all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda r: r[2])
    # all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda r: r[1])
    # all_filtered_recommended_data = sorted(all_filtered_recommended_data, key=lambda r: r[0])
    # all_filtered_recommended_data_columns = ['Stkcd', 'EditedRptdt', 'InstitutionAnanmID', 'Stdrank']
    # all_filtered_recommended_data_output = pd.DataFrame(all_filtered_recommended_data, columns=all_filtered_recommended_data_columns)
    # print(f"Summarize significance: Saving remaining InstitutionAnanmID data...")
    # all_filtered_recommended_data_output.to_excel(f"processed_data/remaining_InstitutionAnanmID.xlsx")
    

    all_filtered_recommended_data = []
    for Stkcd in tqdm(all_filtered_recommended_dictionaries.keys(), desc=f'Summarize significance: Averaging & transforming data', ascii=True):
        Stkcd_dictionary = {}
        for value in all_filtered_recommended_dictionaries[Stkcd].values():
            for d in value:
                if d['EditedRptdt'] not in Stkcd_dictionary.keys(): Stkcd_dictionary[d['EditedRptdt']] = []
                Stkcd_dictionary[d['EditedRptdt']].append(d['Stdrank'])
        for EditedRptdt in Stkcd_dictionary.keys():
            Stkcd_dictionary[EditedRptdt] = sum(Stkcd_dictionary[EditedRptdt]) / len(Stkcd_dictionary[EditedRptdt])
        all_filtered_recommended_dictionaries[Stkcd] = { t[0]: t[1] for t in sorted(Stkcd_dictionary.items(), key=lambda e: e[0]) }

        for EditedRptdt in all_filtered_recommended_dictionaries[Stkcd].keys():
            all_filtered_recommended_data.append([ f'{Stkcd:06d}', EditedRptdt, all_filtered_recommended_dictionaries[Stkcd][EditedRptdt] ])

    all_filtered_recommended_data_columns = ['Stkcd', 'EditedRptdt', 'Stdrank']
    all_filtered_recommended_data_output = pd.DataFrame(all_filtered_recommended_data, columns=all_filtered_recommended_data_columns)
    print(f"Summarize significance: Saving all significant average recommended data...")
    all_filtered_recommended_data_output.to_excel(f"processed_data/all_significant_average.xlsx")


    not_existed_company_season = []
    for Stkcd in all_filtered_recommended_dictionaries.keys():
        for EditedRptdt in all_filtered_recommended_dictionaries[Stkcd].keys():
            all_filtered_recommended_dictionaries[Stkcd][EditedRptdt] = [ False, all_filtered_recommended_dictionaries[Stkcd][EditedRptdt] ]
    for row in tqdm(all_filtered_deleted_data, desc=f'Summarize significance: Filtering last data', ascii=True):
        Stkcd       = int(row[0])
        EditedRptdt = int(row[1])
        if Stkcd in all_filtered_recommended_dictionaries.keys():
            if EditedRptdt in all_filtered_recommended_dictionaries[Stkcd].keys():
                try:
                    all_filtered_recommended_dictionaries[Stkcd][EditedRptdt].append(
                        all_filtered_recommended_dictionaries[Stkcd][int(next_season(EditedRptdt))][1] - \
                            all_filtered_recommended_dictionaries[Stkcd][int(previous_season(EditedRptdt))][1]
                    )
                    all_filtered_recommended_dictionaries[Stkcd][EditedRptdt][0] = True
                except:
                    not_existed_company_season.append((f'{Stkcd:06d}', EditedRptdt))
    all_filtered_recommended_data = []
    for Stkcd in all_filtered_recommended_dictionaries.keys():
        all_filtered_recommended_dictionaries[Stkcd] = {
            t[0]: [ t[1][1], t[1][2] ] for t in list(filter(lambda e: e[1][0], all_filtered_recommended_dictionaries[Stkcd].items()))
        }
        for EditedRptdt in all_filtered_recommended_dictionaries[Stkcd].keys():
            all_filtered_recommended_data.append([
                f'{Stkcd:06d}',
                EditedRptdt,
                all_filtered_recommended_dictionaries[Stkcd][EditedRptdt][0],
                all_filtered_recommended_dictionaries[Stkcd][EditedRptdt][1]
            ])
    all_filtered_recommended_data_columns = ['Stkcd', 'EditedRptdt', 'Stdrank', 'Difference (Next-Previous)']
    all_filtered_recommended_data_output = pd.DataFrame(all_filtered_recommended_data, columns=all_filtered_recommended_data_columns)
    print(f"Summarize significance: Saving filtered significant average recommended data...")
    all_filtered_recommended_data_output.to_excel(f"processed_data/filtered_significant_average.xlsx")
    print(f"Summarize significance: Done. Amount of not_existed_company_season: {len(not_existed_company_season)}\n")


''' Execution '''
if __name__ == "__main__":

    os.system('cls')

    deleted_files_years = [ 2017, 2018, 2019 ]
    recommended_files_years = [ 2016, 2017, 2018, 2019, 2020 ]

    for year in deleted_files_years:
        # process_deleted_file(year)
        # filter_deleted_file(year)
        pass

    for year in recommended_files_years:
        # process_recommended_file(year)
        # filter_recommended_file(year)
        pass

    # integrate_all_filtered_deleted_data(deleted_files_years)
    # integrate_all_filtered_recommended_data(recommended_files_years)

    summarize_significance_and_calculate_difference()