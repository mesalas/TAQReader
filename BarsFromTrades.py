
import sys
import taq_reader as taq_reader


def make_bars_from_trades(in_path, symbol, freq, out_path):
    taq_data = taq_reader.DailyTaqData("trades")
    taq_data.read_data(in_path, symbol)
    taq_data.select_between_open_and_close("9:30:00", "16:00:00")
    taq_data.resample_trades_data(freq).to_csv(out_path)

if __name__ == "__main__":
    in_path, out_path, symbol, freq = sys.argv[1:]
    make_bars_from_trades(in_path, out_path, symbol, freq)