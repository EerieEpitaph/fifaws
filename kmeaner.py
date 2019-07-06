import pandas as pd
from sklearn.cluster import KMeans

column_filter = ['sum', 'pop', 'terrain']
datasets_path = "./datasets/test.csv"

csv_dataframe = pd.read_csv(filepath_or_buffer=datasets_path, sep=';', encoding='utf-8-sig')
filtered_csv = csv_dataframe[column_filter]

# print(filtered_csv)
kmeans = KMeans(n_clusters=20).fit(filtered_csv)
# print(kmeans.cluster_centers_)

final_map = csv_dataframe
final_map['cluster'] = kmeans.labels_
print(final_map[final_map.cluster == 0])
