import traceback
import sys
#from src.logger import get_logger

## 
class CustomException(Exception):
    
    def __init__(self, error_message, error_detail:sys):
        super().__init__(error_message)
        self.error_message = self.get_detailed_error_message(error_message, error_detail)
    
    @staticmethod
    def get_detailed_error_message(error_message, error_detail:sys):
        ## Use this when using local Script
        ## This will not be able to handle credentials error
        #_, _, exc_tb = error_detail.exc_info()
        
        ## Use this for CI/CD
        ## This will be able to handle credentials error
        _, _, exc_tb = traceback.sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno

        return f"Error in {file_name}, line {line_number}: {error_message}"

## Error
#def __str__(self):
    #return self.error_message
    
    ## Solution
    def __str__(self):
        return self.error_message