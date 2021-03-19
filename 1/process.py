# Processing Data
import os
import sys
import json
from datetime import datetime, timedelta

# Linear Regression
import numpy as np
from sklearn.linear_model import LinearRegression


TEST = True

def read_data1():
    step1_files = [
        '1 - 年、中、季報預披露日期/2009.txt',
        '1 - 年、中、季報預披露日期/2010-2019.txt',
    ]

    # season_dict = {}
    # for year in range(11):
    #     for date in ['0331', '0630', '0930', '1231']:
    #         season_dict[f'20{year+9:02d}{date}'] = []

    output_dict = {}

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start reading Data 1.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for file in step1_files:
        now_file = open(file, mode='r', encoding="utf-16-le")
        contents = now_file.read()
        contents = contents.split('\n')
        contents = contents[1:]
        for row in contents:
            items = row.split('\t')
            if TEST and items[0] != '000002':
                continue
            if items[-1] == '':
                continue
            actudt = datetime.strptime(items[-1], '%Y-%m-%d').strftime('%Y%m%d')
            if items[0] not in output_dict.keys():
                output_dict[items[0]] = {
                    'Stkcd': items[0],
                    'Stknme': items[1],
                    'Seasons': { actudt: [] },
                }
            else:
                output_dict[items[0]]['Seasons'][actudt] = []

        # toolbar
        sys.stdout.write("-" * int(toolbar_width/int(len(step1_files))))
        sys.stdout.flush()

        now_file.close()

    sys.stdout.write("]\n")
    return output_dict

def read_data2():
    step2_files = [
        '2 - 日個股回報率文件/2009.txt',
        '2 - 日個股回報率文件/2010.txt',
        '2 - 日個股回報率文件/2011.txt',
        '2 - 日個股回報率文件/2012.txt',
        '2 - 日個股回報率文件/2013.txt',
        '2 - 日個股回報率文件/2014.txt',
        '2 - 日個股回報率文件/2015.txt',
        '2 - 日個股回報率文件/2016.txt',
        '2 - 日個股回報率文件/2017.txt',
        '2 - 日個股回報率文件/2018.txt',
        '2 - 日個股回報率文件/2019.txt',
    ]
    
    output_array = []
    
    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start reading Data 2.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for file in step2_files:
        now_file = open(file, mode='r', encoding="utf-16-le")
        contents = now_file.read()
        contents = contents.split('\n')
        contents = contents[1:]
        for row in contents:
            if row != '':
                items = row.split('\t')
                # if TEST and items[0] != '000002':
                #     continue
                market_type = int(items[3])
                trdsta = int(items[4])
                if market_type not in [2, 8, 16, 32] and trdsta == 1:
                    output_array.append({
                        'Stkcd': items[0],
                        'Trddt': datetime.strptime(items[1], '%Y-%m-%d').strftime('%Y%m%d'),
                        'Dretwd': float(items[2]),
                        'Markettype': market_type,
                        'Trdsta': trdsta,
                    })
        # toolbar
        sys.stdout.write("-" * int(toolbar_width/int(len(step2_files))))
        sys.stdout.flush()

        now_file.close()
    
    sys.stdout.write("]\n")
    return output_array

def read_data3():
    step3_file = [
        '3 - 無風險利率文件/2009.txt',
        '3 - 無風險利率文件/2010-2019.txt',
    ]

    output_dict = {}

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start reading Data 3.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for file in step3_file:
        now_file = open(file, mode='r', encoding="utf-16-le")
        contents = now_file.read()
        contents = contents.split('\n')
        contents = contents[1:]
        for row in contents:
            if row != '':
                items = row.split('\t')
                nrrdaydt = float(items[2]) if len(items) == 3 else float(items[3])
                clsdt = datetime.strptime(items[1], '%Y-%m-%d').strftime('%Y%m%d')
                output_dict[clsdt] = {
                    # 'Nrr1': items[0],
                    # 'Clsdt': clsdt,
                    # 'Nrrdata': float(items[2]),
                    'Nrrdaydt': nrrdaydt,
                }

        # toolbar
        sys.stdout.write("-" * int(toolbar_width/int(len(step3_file))))
        sys.stdout.flush()

        now_file.close()

    sys.stdout.write("]\n")
    return output_dict

def read_data4():
    step4_files = [
        '4 - 綜合市場日回報率/2009.txt',
        '4 - 綜合市場日回報率/2010.txt',
        '4 - 綜合市場日回報率/2011.txt',
        '4 - 綜合市場日回報率/2012.txt',
        '4 - 綜合市場日回報率/2013.txt',
        '4 - 綜合市場日回報率/2014.txt',
        '4 - 綜合市場日回報率/2015.txt',
        '4 - 綜合市場日回報率/2016.txt',
        '4 - 綜合市場日回報率/2017.txt',
        '4 - 綜合市場日回報率/2018.txt',
        '4 - 綜合市場日回報率/2019.txt',
    ]

    output_dict = {}

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start reading Data 4.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for file in step4_files:
        now_file = open(file, mode='r', encoding="utf-16-le")
        contents = now_file.read()
        contents = contents.split('\n')
        contents = contents[1:]
        for row in contents:
            if row != '':
                items = row.split('\t')
                if items[0] == '5':
                    trddt = datetime.strptime(items[1], '%Y-%m-%d').strftime('%Y%m%d')
                    output_dict[trddt] = {
                        # 'Markettype': items[0],
                        # 'Trddt': trddt,
                        'Cdretwdeq': float(items[2]),
                        'Cdretwdos': float(items[3]),
                        'Cdretwdtl': float(items[4]),
                    }

        # toolbar
        sys.stdout.write("-" * int(toolbar_width/int(len(step4_files))))
        sys.stdout.flush()
        
        now_file.close()

    sys.stdout.write("]\n")
    return output_dict

def read_data5():
    step5_files = [
        '5 - 相對價值指標/2009.txt',
        '5 - 相對價值指標/2010.txt',
        '5 - 相對價值指標/2011.txt',
        '5 - 相對價值指標/2012.txt',
        '5 - 相對價值指標/2013.txt',
        '5 - 相對價值指標/2014.txt',
        '5 - 相對價值指標/2015.txt',
        '5 - 相對價值指標/2016.txt',
        '5 - 相對價值指標/2017.txt',
        '5 - 相對價值指標/2018.txt',
        '5 - 相對價值指標/2019.txt',
    ]
    
    output_dict = {}

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start reading Data 5.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for file in step5_files:
        now_file = open(file, mode='r', encoding="utf-16-le")
        contents = now_file.read()
        contents = contents.split('\n')
        contents = contents[1:]
        for row in contents:
            if row == '':
                continue
            items = row.split('\t')
            if items[2][0] == 'J':
                continue
            stkcd = items[0]
            accper = datetime.strptime(items[1], '%Y-%m-%d').strftime('%Y%m%d')
            if stkcd not in output_dict.keys():
                output_dict[stkcd] = {
                    accper: {
                        'F100802A': float(items[4]),
                        'F101002A': float(items[6]),
                    }
                }
            else:
                output_dict[stkcd][accper] = {
                    'F100802A': float(items[4]),
                    'F101002A': float(items[6]),
                }

        # toolbar
        sys.stdout.write("-" * int(toolbar_width/int(len(step5_files))))
        sys.stdout.flush()

        now_file.close()

    sys.stdout.write("]\n")
    return output_dict

def merge_data2_into_data1(data1, data2):

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start merging Data 2 into Data 1.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for index_i, data in enumerate(data2):
        if TEST and data['Stkcd'] != '000002':
            continue
        for index_j, key in enumerate(data1[data['Stkcd']]['Seasons'].keys()):
            if int(data['Trddt']) < int(key):
                if index_j != 0:
                    last_key = list(data1[data['Stkcd']]['Seasons'].keys())[index_j-1]
                    kill_date = datetime.strptime(last_key, '%Y%m%d') + timedelta(days=2)
                    if int(data['Trddt']) <= int(kill_date.strftime('%Y%m%d')):
                        continue
                data1[data['Stkcd']]['Seasons'][key].append({
                    'Trddt': data['Trddt'],
                    'Dretwd': data['Dretwd'],
                    'Markettype': data['Markettype'],
                    'Trdsta': data['Trdsta'],
                })
                break
        # toolbar
        # if (index_i+1) % (int(len(data2) / toolbar_width)) == 0:
    sys.stdout.write("-" * 88)
    sys.stdout.flush()

    sys.stdout.write("]\n")
    return data1

def merge_data3_into_data1(data1, data3):

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start merging Data 3 into Data 1.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for index, company_data in enumerate(data1.values()):
        for season_data in company_data['Seasons'].values():
            for data in season_data:
                data['Nrrdaydt'] = data3[data['Trddt']]['Nrrdaydt']
        # toolbar
        # if (index+1) % (int(len(data1) / toolbar_width)) == 0:
    sys.stdout.write("-" * 88)
    sys.stdout.flush()

    sys.stdout.write("]\n")
    return data1

def merge_data4_into_data1(data1, data4):

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start merging Data 4 into Data 1.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    for index, company_data in enumerate(data1.values()):
        for season_data in company_data['Seasons'].values():
            for data in season_data:
                data['Cdretwdeq'] = data4[data['Trddt']]['Cdretwdeq']
                data['Cdretwdos'] = data4[data['Trddt']]['Cdretwdos']
                data['Cdretwdtl'] = data4[data['Trddt']]['Cdretwdtl']
        # toolbar
        # if (index+1) % (int(len(data1) / toolbar_width)) == 0:
    sys.stdout.write("-" * 88)
    sys.stdout.flush()

    sys.stdout.write("]\n")
    return data1

def get_season_dict_from_data2_and_data5(data2, data5):

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start getting season dictionary from Data 2 and Data 5.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    season_dict = {}

    for index, data in enumerate(data2):
        if data['Stkcd'] not in data5.keys():
            continue
        for key in data5[data['Stkcd']].keys():
            if int(data['Trddt']) <= int(key):
                if key not in season_dict.keys():
                    season_dict[key] = []
                season_dict[key].append({
                    'Stkcd': data['Stkcd'],
                    'Trddt': data['Trddt'],
                    'Dretwd': data['Dretwd'],
                    'F100802A': data5[data['Stkcd']][key]['F100802A'],
                    'F101002A': data5[data['Stkcd']][key]['F101002A'],
                })
                break
        # toolbar
        if (index+1) % (int(len(data2) / toolbar_width)) == 0:
            sys.stdout.write("-")
            sys.stdout.flush()

    sys.stdout.write("]\n")
    return season_dict

def get_SMB_and_HML(season_dict):

    # toolbar
    toolbar_width = 88
    sys.stdout.write("Start getting SMB from season dictionary.\n[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1))

    SMB_dict = {}

    for key in season_dict.keys():

        season = season_dict[key]
        
        SMB_data = sorted(season, key=lambda k: k['F100802A'])
        SMB_sm = SMB_data[:int(len(SMB_data)*0.5)]
        SMB_lg = SMB_data[int(len(SMB_data)*0.5)+1:]

        SMB_sm = sorted(SMB_sm, key=lambda k: k['F101002A'])
        SMB_sm_HML_sm = SMB_sm[:int(len(SMB_sm)*0.3)]
        SMB_sm_HML_md = SMB_sm[int(len(SMB_sm)*0.3)+1:int(len(SMB_sm)*0.7)]
        SMB_sm_HML_lg = SMB_sm[int(len(SMB_sm)*0.7)+1:]
        
        SMB_lg = sorted(SMB_lg, key=lambda k: k['F101002A'])
        SMB_lg_HML_sm = SMB_lg[:int(len(SMB_lg)*0.3)]
        SMB_lg_HML_md = SMB_lg[int(len(SMB_lg)*0.3)+1:int(len(SMB_lg)*0.7)]
        SMB_lg_HML_lg = SMB_lg[int(len(SMB_lg)*0.7)+1:]

        SMB = 0
        for data in [SMB_sm_HML_sm, SMB_sm_HML_md, SMB_sm_HML_lg]:
            SMB_temp = 0
            for dictionary in data:
                SMB_temp += dictionary['Dretwd']
            SMB += SMB_temp / len(data)
        for data in [SMB_lg_HML_sm, SMB_lg_HML_md, SMB_lg_HML_lg]:
            SMB_temp = 0
            for dictionary in data:
                SMB_temp += dictionary['Dretwd']
            SMB -= SMB_temp / len(data)
        SMB /= 3

        HML = 0
        for data in [SMB_sm_HML_lg, SMB_lg_HML_lg]:
            HML_temp = 0
            for dictionary in data:
                HML_temp += dictionary['Dretwd']
            HML_temp /= len(data)
            HML += HML_temp
        for data in [SMB_sm_HML_sm, SMB_lg_HML_sm]:
            HML_temp = 0
            for dictionary in data:
                HML_temp += dictionary['Dretwd']
            HML_temp /= len(data)
            HML -= HML_temp
        HML /= 2

        SMB_dict[key] = {
            'SMB': SMB,
            'HML': HML,
        }

        # toolbar
        sys.stdout.write("--")
        sys.stdout.flush()

    sys.stdout.write("]\n")

    SMB_dict = {k: v for k, v in sorted(SMB_dict.items(), key=lambda item: item[0])}
    return SMB_dict


def main():

    os.system('cls')

    data1 = read_data1()
    data2 = read_data2()
    data3 = read_data3()
    data4 = read_data4()
    data5 = read_data5()

    data1 = merge_data2_into_data1(data1=data1, data2=data2)
    data1 = merge_data3_into_data1(data1=data1, data3=data3)
    data1 = merge_data4_into_data1(data1=data1, data4=data4)

    season_dict = get_season_dict_from_data2_and_data5(data2=data2, data5=data5)
    SMB_and_HML = get_SMB_and_HML(season_dict=season_dict)
    
    print('Start writing ouput.')
    # with open(file='season_dict.txt', mode='w', encoding="utf-8") as f:
    #     f.write('Stkcd\tTrddt\tDretwd\tF100802A\tF101002A\n')
    #     for index, array in enumerate(season_dict.values()):
    #         if index == 4:
    #             break
    #         for data in array:
    #             f.write(f"{data['Stkcd']}\t{data['Trddt']}\t{data['Dretwd']}\t{data['F100802A']}\t{data['F101002A']}\n")
    # with open(file='SMB_and_HML.txt', mode='w', encoding="utf-8") as f:
    #     json.dump(SMB_and_HML, f, indent=4, ensure_ascii=False)

    
    # Linear Regression
    Y_array = []
    X_array = []
    for index, data in enumerate(data1['000002']['Seasons'].values()):
        if index == 4:
            break
        for dictionary in data:
            Y_array.append(dictionary['Dretwd'] - dictionary['Nrrdaydt'])   # R_it - R_ft
            for key in SMB_and_HML.keys():
                if int(dictionary['Trddt']) <= int(key):
                    X_array.append([
                        dictionary['Cdretwdeq'] - dictionary['Nrrdaydt'],   # RM_t - R_ft
                        SMB_and_HML[key]['SMB'],                            # SMB_t
                        SMB_and_HML[key]['HML'],                            # HML_t
                    ])
                    break
    Y_array = np.array(Y_array)
    X_array = np.array(X_array)

    lm = LinearRegression()
    lm.fit(X_array, Y_array)

    print(lm.coef_)
    print(lm.intercept_)

    return


if __name__ == "__main__":
    main()
    print('Finished.')