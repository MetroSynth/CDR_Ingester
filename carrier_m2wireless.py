class M2wirelessCDR(CallDetailRecord):
    def __init__(self,filename):
        #the name of the .SCHEMA file this class will search for
        self.cdr_type = 'm2_wireless_cdr'
        self.filename = filename
        
    def sourceToDataframe(self):
        """
        Ingests source CDR and turns it to a DF with the correct header-placement  
        """
        self.df = pd.read_excel(self.filename)
        self.insertODN()
        display(self.df.head())