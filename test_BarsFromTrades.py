import unittest
import BarsFromTrades

class MyTestCase(unittest.TestCase):
    def test_something(self):
        BarsFromTrades.make_bars_from_trades("testing/testdata/EQY_US_PFE_ABBV_TRADE_20170103.gz", "PFE", "1T","testing/testdata/PFE_1T_TRADE_20170103_partial.csv" )

if __name__ == '__main__':
    unittest.main()
