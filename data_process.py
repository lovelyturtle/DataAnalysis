import os
import pyarrow.parquet as pq
import pandas as pd
import time
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def parse_history(history_str):
    history = eval(history_str)
    avg_price = history['avg_price']
    item_count = len(history['items'])
    purchase_date=history['purchase_date']
    return avg_price, item_count, purchase_date

start_time = time.time()  # 记录开始时间

folder_path = '/home/ylqiu/30G_data_new/'
parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

# 初始化空的DataFrame用于存储数据
data = pd.DataFrame()
# 逐个读取Parquet文件中的数据并进行去重处理
id=0
now = datetime.now()
for file in parquet_files:
    file_path = os.path.join(folder_path, file)
    df = pd.read_parquet(file_path, engine='pyarrow', columns=['id', 'income', 'age', 'is_active', 'gender', 'country', 'purchase_history'])
    print(df.info())
    # 检查并统计重复行
    duplicates = df.duplicated(keep='first')  # 标记重复行，首次出现的保留
    num_duplicates = duplicates.sum()
    if num_duplicates > 0:
        print(f"文件 {file} 发现 {num_duplicates} 个完全重复的行")
        # 删除重复行，只保留第一个出现的行
        clean_pd = df.drop_duplicates(keep='first')
        print(f"\n删除重复行后，DataFrame形状从 {df.shape} 变为 {clean_pd.shape}")
        # 可视化文件中的重复行数据，颜色：1 -> 蓝色，0 -> 黄色
        heat_data = duplicates.astype(int).to_frame().T  # 转置后变成 1xN 的 DataFrame
        cmap = sns.color_palette(["yellow", "blue"])
        plt.figure(figsize=(10, 1.5))
        sns.heatmap(
            heat_data,
            cmap=cmap,
            cbar=False,
        )
        plt.title(f"文件 {file} 条状热力图（蓝=缺失值）")
        plt.show()
    else:
        print("没有发现完全重复的行")
        clean_pd=df
    # 检查缺失值
    clean_pd=df
    absent=clean_pd.isnull().sum().sum()
    if absent == None:
        print("没有发现缺失值")
    else:
        print("缺失值数量为：", absent)
        clean_pd=clean_pd.dropna()
    # 检查年龄的异常值：
    num0=len(clean_pd)
    clean_pd = clean_pd.loc[(clean_pd['age'] <= 100) & (clean_pd['age'] >= 18)]
    print("年龄异常值数量：", len(clean_pd)-num0)
    # 对列信息进行提取和更改
    clean_pd=df
    clean_pd[['avg_price', 'item_count', 'purchase_date']] = clean_pd['purchase_history'].apply(lambda x: pd.Series(parse_history(x)))
    clean_pd['sum_amount'] = clean_pd['item_count'] * clean_pd['avg_price']
    clean_pd['purchase_date'] = pd.to_datetime(clean_pd['purchase_date'])
    clean_pd['recency'] = (now - clean_pd['purchase_date']).dt.days
    clean_pd = clean_pd.drop(columns=['purchase_history', 'purchase_date'])
    clean_pd.to_parquet("/home/ylqiu/30G_data_new2/"+"filter"+str(id)+".parquet")
    id+=1
print(time.time()-start_time)