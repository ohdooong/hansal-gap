

import pandas as pd
print("======================CSV변환 시작==========================")
for x in [2,3,4,5,6] :
    
    filePath = f"/opt/bitnami/spark/data/mds_security_logs_202403_0{x}.xlsx"
    savePath = f"/opt/bitnami/spark/data/mds_security_logs_202403_0{x}.csv"

    df1 = pd.read_excel(filePath, engine='openpyxl')
    df1.to_csv(savePath, index=False)

print("======================CSV변환 끝==========================")

