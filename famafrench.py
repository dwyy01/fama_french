import urllib
import zipfile
from bs4 import BeautifulSoup
import io
from datetime import datetime
import pandas as pd
import regex as re
from pandas.tseries.offsets import MonthEnd

def ff_parse_dates(date):
    if len(date) <=6:
        dtdate = pd.to_datetime(date, format="%Y%m") + MonthEnd(0)
    else:
        dtdate = pd.to_datetime(date)
    return dtdate

class FFData():
    def __init__(self, start = None, end = None):
        self.start = start
        self.end = end

        # initialise dates
        if end is None:
            self.end = datetime.today().strftime('%Y-%m')
        if start is None:
            now = datetime.today()
            self.start = now.replace(now.year - 5).strftime('%Y-%m')

        # self._proxies = proxies
        # handle proxies
        # proxy = urllib.request.ProxyHandler(proxies)
        # opener = urllib.request.build_opener(proxy)
        # urllib.request.install_opener(opener)
        fflink = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html'

        # open URL
        html_object = urllib.request.urlopen(fflink)
        soup = BeautifulSoup(html_object, "html.parser")
        # Create dataset list
        self.datasets = []
        for link in soup.find_all('a', href=True):
            # Find all csv files
            filename = link['href']
            if 'CSV.zip' in filename:
                data_name = filename.replace('_CSV.zip', '').replace('ftp/', '').replace('_', ' ')
                self.datasets.append(data_name)

    def get_available_data(self):
        return self.datasets

    def key_factor_names(self):
        key_datas = ['F-F Research Data Factors', 'F-F Research Data Factors weekly', 'F-F Research Data Factors daily',
                     'F-F Research Data 5 Factors 2x3', 'F-F Research Data 5 Factors 2x3 daily']
        for i in key_datas:
            print(i)

    def setDates(self, start:str, end:str):
        '''
        Set date range
        :param start: '2021-10-22'
        :param end: '2022-11-22'
        :return: None
        '''
        self.start = start
        self.end = end

    def _loadData(self, name = 'F-F Research Data 5 Factors 2x3'):
        """
        Extract dataset from Fama French library, this is used in loadData function
        :param name: name of the data set, please get list of names through get_available_data() function
        :return: outputs dictionary of datasets in the format:
                output = {
                            'FILE DESCRIPTION' : 'Description'
                            0 : {
                                ' Annual Factors - 3 factors': pd.DataFrame
                                 }
                        }
                note dataset numbering starts from 0
        """
        filename = name.replace(' ', '_') + '_CSV.zip'

        datalink = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/" + filename

        data_saved = urllib.request.urlopen(datalink)
        zip_file = zipfile.ZipFile(io.BytesIO(data_saved.read()), 'r')
        # csv_name = zip_file.namelist()[0]
        files = zip_file.open(zip_file.namelist()[0]).read().decode(errors='ignore')

        splitfile = files.split("\r\n\r\n")

        main_desc = ''
        csv_str_sets = []
        # First filter out descriptions from the data table
        for stringdata in splitfile:
            # Unfortunately can only differentiate them by character length
            if len(stringdata) < 1500:
                main_desc += stringdata
            else:
                csv_str_sets.append(stringdata)

        output_dict = {}
        for i, data_table in enumerate(csv_str_sets):

            data_set_dict = {}
            # Make sure first line of data is not data table description
            match = re.search(r"^\s*,", data_table, re.M) # Identifies description string
            if match:
                str_start = match.start() # Start of table data
                data_desc = data_table[:str_start].replace("\r\n", "")
                data_str = data_table[str_start:]
            else:
                data_desc = 'No data description'
                data_str = data_table

            ff_csv_str = io.StringIO('Date' + data_str) # Use StringIO to read_csv
            ff_data = pd.read_csv(ff_csv_str, parse_dates=[0], date_parser=ff_parse_dates)
            ff_data.set_index('Date', inplace=True)
            ff_data = ff_data.loc[self.start:self.end]
            ff_data = ff_data.apply(pd.to_numeric, errors='coerce')
            data_set_dict[data_desc] = ff_data
            output_dict[i] = data_set_dict

        output_dict['FILE DESCRIPTION'] = main_desc
        print('Dataset for {0} obtained, description: {1}'.format(name, main_desc))
        print('Number of datasets: {0}'.format(len(output_dict) - 1))
        print('Output format is nested dictionary file')

        return output_dict

    def loadData(self, name='F-F Research Data 5 Factors 2x3'):
        try:
            output = self._loadData(name)
            return output
        except:
            print('Could not load data')




