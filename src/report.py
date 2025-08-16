from config import OutputCfg

class Report:
    """ Reporter class that can be logged to and tracks what to be displayed to user at the end of operation. """
    
    def __init__(self, out: OutputCfg):
        self.report = ""
        self.report_file = out.report_md

    def log_report(self, message: str):
        self.report += message + '\n'

    def print_report(self):
        file = open(self.report_file, "w+")
        file.write(self.report)

    