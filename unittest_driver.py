from unittest import TestLoader,TextTestRunner
from resources.HTMLTestRunner import HTMLTestRunner
if __name__ == "__main__":
    loader = TestLoader()
    start_directory ="D://DataEngineering_Projects/HTMLTestRunner/test_cases/"
    suite =loader.discover(start_directory,pattern='*_test.py')
    runner =HTMLTestRunner(open("D://DataEngineering_Projects/HTMLTestRunner/result/Unittest_report.html","w"),title="Unit Test Reports",description="Testing Reports")
    runner.run(suite)
    # To check the result on console comment above line 8 and uncomment line 10.
    #print(TextTestRunner(verbosity=2).run(suite))
 
