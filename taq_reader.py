import pandas as pd

class DailyTaqData:
    def __init__(self, data_type):
        if data_type == "nasdaq":
            self.read_data = self.read_nyse_nbbo_data
            self.resample_data = self.resample_nbbo_data
        elif data_type == "trades":
            self.read_data = self.read_nyse_trades_data
            self.resample_data = self.resample_trades_data
        else:
            print("{}: unknown data type".format(data_type))
    
    def read_nyse_trades_data(self,path, symbol = None, compression = "gzip"):
        """ 
        Method for reading NYSE TAQ data
        It is assumed that the data is compressed
        """
        chunks = pd.read_csv(path, sep="|", compression=compression, usecols = ["Time","Symbol", "Trade Price", "Trade Volume", "Exchange"], chunksize=10000000)
        self.data = pd.concat([chunk[chunk["Symbol"] == symbol] for chunk in chunks])
        self.data["Dollar Volume"] = self.data["Trade Volume"] * self.data["Trade Price"]
        self.date_str = self.get_date(path)
        self.convert_time_stamps()

    def read_nyse_nbbo_data(self,path, symbol = None, compression = "gzip"):
        """ 
        Method for reading NYSE TAQ data
        It is assumed that the data is compressed    
        """
        data = pd.read_csv(path, sep="|", compression=compression, usecols = ["Time", "Symbol","Bid_Price", "Bid_Size", "Offer_Price", "Offer_Size", "Exchange"])
        self.data = data[(data["Symbol"] == symbol) & (data["Exchange"] == "T")] # Select only column matching symbol
        self.date_str = self.get_date(path)
        self.convert_time_stamps()

    def convert_time_stamps(self):
        #Convert timestamp to datetime and index
        self.data.index = pd.DatetimeIndex(
            pd.to_datetime(
                self.data.Time.apply(lambda x : self.date_str + " " + str(x)),
                 format = "%Y%m%d %H%M%S%f"),
                 name = "DateTime"
                 )

        one_sec_time_delta = pd.Timedelta(1,"s")
        day_time_stamp = pd.to_datetime(self.date_str)

        self.data.Time = ((self.data.index-day_time_stamp)/one_sec_time_delta).values
    
    def get_date(self, path_string):
        return path_string.split("_")[-1].split(".")[0]
    
    def select_between_open_and_close(self,start,end):
        self.data = self.data.between_time(start,end)
    def resample_nbbo_data(self, freq):
        resampled_data = self.data.resample(freq).last().ffill()
        one_sec_time_delta = pd.Timedelta(1,"s")
        day_time_stamp = pd.to_datetime(self.date_str)

        resampled_data.Time = ((resampled_data.index-day_time_stamp)/one_sec_time_delta).values
        return resampled_data
    def resample_trades_data(self,freq):
        resampled = self.data.resample(freq)
        resampled_data =resampled.agg({
            "Time" : "last"})
          #   "Symbol": "last",
        resampled_data["Trade Price"] = resampled.agg({
            "Trade Price": "mean"})
        resampled_data["High"] = resampled.agg({
            "Trade Price": lambda x : x.max()})
        resampled_data["Low"]= resampled.agg({
            "Trade Price": lambda x: x.min()})
        resampled_data["Open"] = resampled.agg({
            "Trade Price": "first"})
        resampled_data["Close"] = resampled.agg({
            "Trade Price": "last"})
        resampled_data["Trade Volume"] =  resampled.agg({"Trade Volume": "sum"})
        resampled_data["Trades"] = resampled.agg({
            "Trade Price": "count"})
        resampled_data["Dollar Volume"] = resampled.agg({"Dollar Volume": "sum"})

        one_sec_time_delta = pd.Timedelta(1,"s")
        day_time_stamp = pd.to_datetime(self.date_str)

        resampled_data.Time = ((resampled_data.index-day_time_stamp)/one_sec_time_delta).values

        return resampled_data

def convert_taq(data_type,path,symbol):
    taq_data = DailyTaqData(data_type)
    taq_data.read_data(path, symbol)
    taq_data.select_between_open_and_close("9:30:00", "16:00:00")
    taq_data.data.to_csv("test_out/" + taq_data.date_str + "_"+ symbol + "_" + data_type + ".csv")
    
    resample_freqs = {"1min" : "1T", "1sec": "1s"}
    
    for time, freq in resample_freqs.items():
        taq_data.resample_data(freq).to_csv("test_out/" + taq_data.date_str + "_"+ symbol + "_" + data_type + "_" + time +".csv")
