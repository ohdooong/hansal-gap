

import pandas as pd
print("======================CSV변환 시작==========================")
for x in range(1,8) :
    
    filePath = f"D:/DF/보안데이터/MTS-MTA_탐지현황_7~8월/탐지현황{x}.xlsx"
    savePath = f"../processed_csv/mds_202408_{x}.csv"

    df1 = pd.read_excel(filePath, engine='openpyxl')
    df1.to_csv(savePath, index=False)


print("======================CSV변환 끝==========================")

