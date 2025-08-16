from config import OutputCfg

class Report:
    """ Reporter class that can be logged to and tracks what to be displayed to user at the end of operation. """
    
    def __init__(self, out: OutputCfg):
        self.report = out.report_md
        file = open(self.report, "w")
        file.close()

    def log_report(self, message: str):
        with open(self.report, "a+") as file:
            file.write(message)
            file.write("\n")

    