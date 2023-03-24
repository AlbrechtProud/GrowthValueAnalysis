import pandas as pd
import urllib.request
import zipfile
from scipy.stats import jarque_bera

# from pandas.compat import StringIO


def download_file(source_url, filename):
    urllib.request.urlretrieve(source_url, filename)


def transform_and_save(source_file):
    df_source = pd.read_csv(source_file, 
                 skiprows=4, sep='\s+', nrows=576, 
                 names=['YYYYMM', 'BE/ME Mkt', 'BE/ME High', 'BE/ME Low', 'E/P High', 
                        'E/P Low', 'CE/P High', 'CE/P Low', 'Yld High', 'Yld Low', 'Yld Zero'])
    
    df_source['Source'] = source_file.split('\\')[-1]

    return df_source


def unzip_files():
    with zipfile.ZipFile(r'C:\Users\leand\Documents\Source_Data\indeces.zip', 'r') as zip_ref:
        zip_ref.extractall(r'C:\Users\leand\Documents\Source_Data\indeces_extr')

pd.options.display.float_format = '{:.4f}'.format

# download_url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_International_Indices.zip'
# filename = r'C:\Users\leand\Documents\Source_Data\indeces.zip'

# download_file(source_url=download_url, filename=filename)
# unzip_files()

source_EUWOUK = r'C:\Users\leand\Documents\Source_Data\indeces_extr\Ind_Eur_WOut_UK.dat'
source_Asia_Pac = r'C:\Users\leand\Documents\Source_Data\indeces_extr\Ind_Asia_Pacific.Dat'
source_EUWUK = r'C:\Users\leand\Documents\Source_Data\indeces_extr\Ind_Eur_With_UK.Dat'
source_Scan = r'C:\Users\leand\Documents\Source_Data\indeces_extr\Ind_Scandanavia.Dat'
source_UK = r'C:\Users\leand\Documents\Source_Data\indeces_extr\Ind_UK.Dat'

df_EuWoUK = transform_and_save(source_file=source_EUWOUK)
df_Asia_Pac = transform_and_save(source_file=source_Asia_Pac)
df_EUWUK = transform_and_save(source_file=source_EUWUK)
df_Scan = transform_and_save(source_file=source_Scan)
df_UK = transform_and_save(source_file=source_UK)

df_list = [df_EuWoUK, df_EUWUK, df_Scan, df_UK]

for dataframe in df_list:
    dataframe = dataframe.query('200806 < YYYYMM <= 202112')
    country = dataframe['Source'].values[0].split('.')[0]
    dataframe = dataframe[['BE/ME High', 'BE/ME Low', 'E/P High', 'E/P Low', 'CE/P High', 'CE/P Low', 'Yld High', 'Yld Low']]
    dataframe = dataframe.rename(columns={'BE/ME High': 'B/P Value', 'BE/ME Low': 'B/P Growth', 
                                            'E/P High': 'E/P Value', 'E/P Low': 'E/P Growth', 'CE/P High': 'CE/P Value', 
                                            'CE/P Low': 'CE/P Growth', 'Yld High': 'D/P Value', 'Yld Low': 'D/P Growth'})
    stats_df = dataframe.describe().transpose()
    stats_df['Skewness'] = dataframe.skew()
    stats_df['Kurtosis'] = dataframe.kurtosis()
    stats_df['Median'] = dataframe.median()
    stats_df['JB'] = dataframe.apply(lambda x: jarque_bera(x)[0]) 
    stats_df['P_Val'] = dataframe.apply(lambda x: jarque_bera(x)[1])
    stats_df['Normal at 0.05?'] = stats_df['P_Val'].apply(lambda p: 'Reject' if p < 0.05 else 'Fail to reject')

    stats_pivot = stats_df.reset_index()
    stats_pivot = stats_pivot[['index', 'mean', 'std', 'Skewness', 'Kurtosis', 'min', 'Median', 'max', 'JB', 'P_Val', 'Normal at 0.05?']]
    stats_pivot['Source'] = country
    print(stats_pivot)
    print('-'*130)
    stats_pivot.to_csv(f'output_{country}.csv', index=False, sep=';', decimal=',')
