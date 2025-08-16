

class Report:
    """ Reporter class that can be logged to and tracks what to be displayed to user at the end of operation. """
    
    def __init__(self):
        self.report = ""

    def log_report(self, message: str):
        self.report += message + '\n'

    def print_report(self):
        print(self.report)
    