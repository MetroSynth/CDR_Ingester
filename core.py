import pandas as pd
from IPython.display import display,HTML
import os

class CDRInventory():
    def __init__(self,source_dir,target_dir):
        self.source_dir = source_dir #sets directory the raw CDR files will be found in
        self.target_dir = target_dir #sets dir processed CDRs and CFRs will be sent to
        #dictionary of functions that ingests a source CDR and returns the headspace-string
        self.headspace_handlers = {
            '.xlsx':headspaceXls,
            '.xls':headspaceXls,
            #'.csv':headspaceCsv,
            '.txt':headspaceTxt
        }
        
    def loadDefinitions(self):
        filename = './app_data/definitions.data' #fixed location of definition strings
        self.definitions = {} #empty dict for each to be read into
        with open(filename) as f:
            for line in f:
                line = line.rstrip('\n') #strips line-break chars from each line
                ##splits on pipe char, front is cdr-type, back is definition string
                self.definitions[line.split('|')[0]] = line.split('|')[1] 

    def headspaceTxt(self,filename):
        """
        Function to create headspace object from any txt files in source dir
        """
        lines = [] #blank list each row will be read into
        with open(filename) as f:
            for line in f:
                line = line.rstrip('\n') #strips line-break chars
                lines.append(line) #adds each line in the file to list
            headspace = lines[0:20] #only grabs the first 20
            return headspace #returns object out into class

    def headspaceXls(self,filename):
        """
        Creates headspace str object from all excel files in source dir
        """
        df = pd.read_excel(filename).astype(str) #reads all cells into a df as strings
        headspace = dfToHeadspace(df) #passes this df into a sub-function
        return headspace

    def dfToHeadspace(self,df):
        """
        Creates headspace str object from a pandas df. Is needed since excel files
        are not easily read in through python, it needs pandas as an intermediary
        """
        header_rows = []
        columns = [i for i in df.columns] #makes sure text of header row is not ignored
        columns = ' '.join(columns) #all cells in header combined into one str
        header_rows.append(columns) #placed first into empty list
        if len(df) >= 19: #makes sure func doesn't choke when file is less than 20 lines
            headspace_depth = 19 #how deep into the file this function will look
        else:
            headspace_depth = len(df) #to handle files shorter than 20 lines
        for i in range(headspace_depth):
            #makes a list of all cells in the current row being processed
            row_cells = list(df.ix[i])
            #creates a list from only the cells that are strings. Drops NaNs and Ints
            string_cells = [j for j in row_cells if isinstance(j,str)]
            #makes a single string from each cell in the entire row
            final_string = ' '.join(string_cells)
            #drops it into our master list of of rows
            header_rows.append(final_string)
        return header_rows

    def buildEnv(self):
        if not os.path.exists(self.target_dir): #if target dir has not been created, 
            os.makedirs(self.target_dir) #creates it...
        self.hsh = self.headspace_handlers #re-inits dict to a shorter variable name   
        self.cdr_listing = [f for f in os.listdir(source_dir) if os.path.splitext(f)[1] in hsh.keys()]

    def detectTypes(self):
        definitions = loadDefinitions()
        cdr_inventory = {}
        for file in self.cdr_listing:
            headspace = hsh[os.path.splitext(file)[1]]('{}/{}'.format(source_dir,file))
            headspace = ' '.join(headspace)
            supported = False
            for d in definitions.keys():
                if definitions[d] in headspace:
                    #print('CDR is of type: {}'.format(d))
                    cdr_inventory[file] = d
                    supported = True
            if not supported:
                cdr_inventory[file] = 'unsupported'
        return cdr_inventory
    
    def scan(self):
        self.buildEnv()
        self.loadDefinitions()
        cdr_inventory = self.detectTypes()
        return cdr_inventory
        
parser = CDRInventory("./CDRs","./CDRs/Processed CDRs")
cdr_types = parser.scan()
display(pd.DataFrame.from_dict(cdr_inventory, orient='index'))

class_schema_associations = {
    'm2_wireless_cdr':M2wirelessCDR,
    'tmobile_cdr':TMobileCDR
}

list_of_cdr_objects = []
for record in cdr_types.keys():
    if cdr_types[record] in class_schema_associations.keys():
        filename = '{}/{}'.format(parser.source_dir,record)
        odn = (os.path.splitext(record)[0]).split('_')[0]
        current_cdr = class_schema_associations[cdr_types[record]](filename)
        current_cdr.setODN(odn)
        list_of_cdr_objects.append(current_cdr)
    
for cdr in list_of_cdr_objects:
    cdr.sourceToDataframe()
    