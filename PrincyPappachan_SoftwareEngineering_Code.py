# Create a Class for Fetching the Survery Data 
class FetchSurveryData():
    def __init__(self):
        #Setting the modules to be installed 
        self.modules_to_install = {'pyodbc': 'pyodbc','pandas': 'pd','numpy': 'np','pyyaml':'yaml' }
    
    #Setting the installation process
    def install(self,package):
        import subprocess #Useful for runtime installation of packages 
        import sys #Useful for runtime installation of packages
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    #Checks if the the given packges are installed and installs if not . Imports if yes . 
    def import_packages(self):
        
        global pd, np, pyodbc, yaml, sys, os , shutil, datetime
        print("Initiating process for importing the essentials packages and installing them if unavailable ")
        
        for key, value in self.modules_to_install.items():
            try:
                #These packages are already installed and available by default if the system has python installed . We dont need to install them .
                import sys
                import os
                from datetime import datetime
                import shutil
                #These Packages need to be installed 
                if key == 'pandas':
                    pd = __import__(key) 
                elif key == 'numpy':
                    np = __import__(key) 
                elif key == 'pyyaml':
                    yaml = __import__(value) 
                else:
                    pyodbc = __import__(key)
                print ('{} Module imported as {}  '.format(key, value) )
            except ImportError as e:
                self.install(e.name)
                print ('Module installed : ' + e.name )
                if e.name == 'pandas':
                    pd = __import__(e.name) 
                elif e.name == 'numpy':
                    py = __import__(e.name)
                elif e.name == 'pyyaml':
                    yaml = __import__(value)
                else:
                    pyodbc = __import__(e.name)
                print ('{} Module imported as {}  '.format(e.name, value) )

        print("Congratulations ! All the packages have been imported . Now lets start connecting to the Database.")


    #This function connects to the DB mentioned in the YAML file
    def connectdb(self):
        try :
            with open("config.yml", "r") as self.ymlfile:
                self.cfg = yaml.full_load(self.ymlfile)


            for section, value in self.cfg.items():
                server = value["server"]
                database = value["database"]
                username = value["username"]
                password = value["password"]

            self.ymlfile.close()
            cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            self.cursor = cnxn.cursor()
            print("We have successfully connected to the database !")
            return server,database
        except FileNotFoundError:
            print("ERROR : Kindly ensure that the 'config.yml' available in the zip file is placed in your current working directory ")
        except pyodbc.Error as ex:
            print("CONNECTION ERROR : Kindly verify the DataBase details provided in 'config.yml' file and ensure the USER has proper access")
            print(ex)
        except:
            print("ERROR :Something  went wrong : ", sys.exc_info()[0])
            

    #This Function Builds the query by replacing the strings with the dynamic input provided 
    def querybuilder(self,query,string_to_replace,replaced_string): 
        new_query = query.replace(string_to_replace,replaced_string)
        return new_query

    #Since we have to call the DB to fetch the data by querying , this function does the needful
    def fetchQueryData(self,query):
        self.cursor.execute(query) 
        rows = self.cursor.fetchall()
        return rows
        
    
    def fetchdata(self):
        #Query TO get a resultset of SurveyId, QuestionId, flag InSurvey indicating whether the question is in the survey structure
        query_for_resultset = "SELECT * FROM (	SELECT SurveyId,QuestionId, 1 as InSurvey FROM SurveyStructure\
                                WHERE SurveyId = id UNION SELECT id as SurveyId,Q.QuestionId,0 as InSurvey\
                                FROM Question as Q\
                                WHERE NOT EXISTS\
                                (SELECT * FROM SurveyStructure as S WHERE S.SurveyId = id AND S.QuestionId = Q.QuestionId\
                                )\
                            ) as t\
                            ORDER BY QuestionId "

        #Query for fetching the Survery Structure which defines which question is present in the survery
        query_for_SurveryStructure = 'SELECT * from SurveyStructure'


        #Set the Initial statement
        strQueryTemplateForAnswerColumn = 'COALESCE((SELECT a.Answer_Value FROM Answer as a WHERE a.UserId = u.UserId AND a.SurveyId = <SURVEY_ID> AND a.QuestionId = <QUESTION_ID>), -1) AS ANS_Q<QUESTION_ID> '

        strQueryTemplateForNullColumnn = ' NULL AS ANS_Q<QUESTION_ID> '

        strQueryTemplateOuterUnionQuery = 'SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u WHERE EXISTS (SELECT * FROM Answer as a WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>)'


        #Query to create the view
        strViewTemplate = 'CREATE OR ALTER VIEW vw_AllSurveyData AS '


        #Query to fetch survery ID from Survery structure 
        query_for_SurveryID = 'SELECT SurveyId FROM Survey ORDER BY SurveyId'


        #This function checks if the question ID is present in the current Survery 
        #Calls the query builder function  to replace the question id in the give query template based on the if it is present in the Current Survery
        def strcolumnsquerybuilder(curr_InSurvery,question_id) : 
            if curr_InSurvery == 0:
                    strColumnsQuery = self.querybuilder(strQueryTemplateForNullColumnn,'<QUESTION_ID>',str(question_id))
                            

            else  :
                    strColumnsQuery = self.querybuilder(strQueryTemplateForAnswerColumn,'<QUESTION_ID>',str(question_id))
            return strColumnsQuery

        #Created a list of Suvery ID which will be later used for iteration 
        self.cursor.execute("SELECT SurveyId FROM Survey ORDER BY SurveyId;") 
        row = self.cursor.fetchone() 
        survery_id_list = []
        while row: 
            survery_id_list.append(row[0]) #Append the survery id in a list 
            row = self.cursor.fetchone()
        

        #Created a Data frame of the In survery List
        resultset_list = []
        for survey_id in survery_id_list:
            self.cursor.execute(self.querybuilder(query_for_resultset,'id',str(survey_id)))
            rows = self.cursor.fetchone() 
            
            while rows: 
                row_list = [rows.SurveyId, rows.QuestionId, rows.InSurvey]
                resultset_list.append(row_list)
                rows = self.cursor.fetchone()
                    
        df_resultset_list = pd.DataFrame(resultset_list, columns=['SurveyId', 'QuestionId', 'InSurvey'])
        

        question_id_list = df_resultset_list['QuestionId'].unique()
        survey_id_list=df_resultset_list['SurveyId'].unique()
        

        #Iterated over Survery ID as outer loop and Question ID as inner loop 
        #Based on the avaialability of the Question ID in the survery, create the Final Query 
        strOuterQueryPart = []
        for survey_id in survey_id_list:
            strColumnsQueryPart = []
            for question_id in question_id_list:
                #Below line checkes if the Question id is present in the Survery
                curr_InSurvery = df_resultset_list[(df_resultset_list['SurveyId']==survey_id)&(df_resultset_list['QuestionId']==question_id)].iloc[:,2].values
                strColumnsQuery = strcolumnsquerybuilder(curr_InSurvery,question_id) 
                strColumnsQueryPart.append(strColumnsQuery)
                #Below line appends a comma between the queries
                dynamicQuestion = ','.join(strColumnsQueryPart)
            
            #This Replaces the <DYNAMIC_QUESTION_ANSWERS> and <SURVEY_ID> tag with the query created in the inner loop and Survery ID of outer loop respctively
            outerquery = self.querybuilder(strQueryTemplateOuterUnionQuery,'<DYNAMIC_QUESTION_ANSWERS>',str(dynamicQuestion))
            outerquery = self.querybuilder(outerquery,'<SURVEY_ID>',str(survey_id))
            strOuterQueryPart.append(outerquery)
            #Below line appends UNION between the iteration of query generated for each Survery 
            finalquery = ' UNION '.join(strOuterQueryPart)
        
        #Create the data frame for the output of the final query
        rows = self.fetchQueryData(finalquery)
        df = pd.DataFrame(
            [[row.UserId, row.SurveyId,row.ANS_Q1,row.ANS_Q2,row.ANS_Q3,row.ANS_Q4 ] for row in rows]\
            ,columns=['UserId', 'SurveyId', 'ANS_Q1','ANS_Q2','ANS_Q3','ANS_Q4']
            ,dtype=np.int16)
        #Filled the NA values with Null to maintain uniformity (Some were displayed as None and Null )
        df.fillna("Null", inplace = True) 
        


        rows = self.fetchQueryData(query_for_SurveryStructure)
        
        df_survery_structure = pd.DataFrame(
            [[ row.SurveyId,row.QuestionId,row.OrdinalValue ] for row in rows]\
            ,columns=['SurveyId', 'QuestionId', 'OrdinalValue']
            #,dtype=np.int16\
            )
        cwd = os.getcwd()

        #Declaring the variables for file name for the ease of reusability
        file_ext = '.csv'
        file_name = 'SurveryStructure'
        full_file_name = file_name + file_ext
        file_path = os.path.join(cwd, full_file_name)
        alldata_file_name = 'AllSurveryData'
        alldata_full_file_name = alldata_file_name + file_ext
        alldata_file_path = os.path.join(cwd, alldata_full_file_name)

        

        #Function to create the new/updated file and archive the preivous one
        def createfile(file_path,archive_file_path,full_archive_file_path,stringtoprint,Datatype):
            try:
                shutil.move(file_path, full_archive_file_path)
                print("Previous" + stringtoprint + "has been Archived to :  " , archive_file_path )
            except PermissionError:
                print("ERROR While archiving the file . Please ensure the file is not being used by another process.")
            except:
                print("ERROR : Something went wrong while archiving the file.")

            
            if Datatype == 'S':
                df_survery_structure.to_csv(file_path, index = False)
            else :
                df.to_csv(file_path, index = False)

            print ("Updated" + stringtoprint + "has been created on your local : " + file_path )

        
                    

        #Function to create view
        def createview(strViewTemplate,finalquery,decision):
            try:
                strViewTemplate = strViewTemplate + finalquery
                self.cursor.execute(strViewTemplate)
                if decision == 'create':
                    print('A view name vw_AllSurveyData has been created in your database') 
                else :
                    print('View  vw_AllSurveyData has been updated in your database')
            except:
                print("ERROR: Error while creating a view . Please ensure the Database User provided in the YAML File has the CREATE/ALTER ")           
                    
        if os.path.isfile(file_path):
            print ("Survery Structure File exists at : ",file_path)
            old_df = pd.read_csv(file_path)
            
            if old_df.equals(df_survery_structure)==False:
                print("New data has been added to Survery Structure ")
                curr_timestamp = datetime.utcnow().strftime('%d%m%Y%H%M%S%f')[:-3]
                new_full_file_name = file_name + '_' + curr_timestamp + file_ext
                alldata_new_full_file_name = alldata_file_name + '_' + curr_timestamp + file_ext
                archive_file_path =  os.path.join(cwd,'archive') 
                full_archive_file_path = os.path.join(archive_file_path,new_full_file_name)
                alldata_full_archive_file_path = os.path.join(archive_file_path,alldata_new_full_file_name)

                
                if not os.path.exists(archive_file_path):
                    os.makedirs(archive_file_path)
                    createfile(file_path,archive_file_path,full_archive_file_path,' Survery Structure File ','S')
                    createfile(alldata_file_path,archive_file_path,alldata_full_archive_file_path,' All Survery File ','D')
                    createview(strViewTemplate,finalquery,'update')
                    
                else:
                    createfile(file_path,archive_file_path,full_archive_file_path,' Survery Structure File ','S')
                    createfile(alldata_file_path,archive_file_path,alldata_full_archive_file_path,' All Survery File ','D')
                    createview(strViewTemplate,finalquery,'update')
                    
                
            else:
                print("Survery Structure remains unchanged. Latest SurveryStructure.csv and AllSurveryData.csv is available in your current working directory ")
            
        else:
            df_survery_structure.to_csv(file_path, index = False)
            print ('Survery strucure File has been created on your local : ' + file_path )
            df.to_csv(alldata_file_path, index = False)
            print ('All Survery File has been created on your local : ' + alldata_file_path )
            createview(strViewTemplate,finalquery,'create')
    

# This is the main funtion which initiates the script

def main():
    import os 
    print("Welcome.This Code fetches the Survery Data of the Database metioned in the YAML File")
    cwd = os.getcwd()
    print("Please ensure the following : \n 1.Database User should have CREATE/ALTER access \n 2.Proper User credentials are updated in the YAML File available in the zip file \n 3.The YAML File  is placed in your current working directory: " + cwd)
    d = input("Would you like to continue (Y/N) :")
    if d.upper() == "Y":
        surveyobj = FetchSurveryData()
        try:
            surveyobj.import_packages()
        except:
            print("ERROR while importing the packages")
        try:
            serv,db = surveyobj.connectdb()
            print("Connected to server  {} and database {}". format(serv,db))
            
        except:
            print("ERROR while connecting to the database")
        try:
            surveyobj.fetchdata()
        except:
            print("ERROR while fetching the data")
        #finalquery = surveyobj.fetchdata()
        #print ("Final Query to be execulted is : \n" + finalquery)
    else:
        print("Exiting the application")



if __name__ == "__main__":
    main()
    