class TMobileCDR(CallDetailRecord):
    def __init__(self,filename):
        self.filename = filename
        #the name of this value **must** match the .SCHEMA file this class will search for
        self.cdr_type = 'tmobile_standard'

    def sourceToDataframe(self):
        """
        Each CDR type's sourceToDataframe() subroutine is where the unique operations
        for that type are stored      
        """
        df = pd.read_excel(self.filename)
        df.columns = df.iloc[10]
        df = df.drop(df.index[:11])
        self.df = df #makes this df accessible to the whole class now
        self.insertODN()
        display(df.head())