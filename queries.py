#queries.py
#Script by Forum Modi
#
#Function: Takes DB filled by XML_scraping and outputs an excel document with articles based upon user input
#Allows for selection by conferences, years, and keywords


import pyodbc
import pandas as pd
from tqdm import tqdm
import sqlite3 as sql
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import os


#parses through textfile
def read_file(file):
    array = []
    with open(file) as f:
        content = f.read()
        array = [word.strip() for word in content.split(",")]        
    return array
                              

#user menu
def terminal_prompt():
    
    #prompts user
    print("<---Welcome to the database parsing script!--->\n")
    database = input("What is the database name (do not include .db)?\n")

    print("Trying to enter database...")
          
    #create db
    connection = sql.connect(database+".db")
    
    
    #cursor, communicates SQL statements to db
    cursor = connection.cursor()
    
    #checks if database is actually filled
    try: cursor.execute("SELECT title FROM " + database)
    except: 
        print("...Sorry, that database does not exist! Try again! \n")
        cursor.execute("DROP TABLE IF EXISTS " + database)
        terminal_prompt()
    
    print("...Connected to database!\n")
    
    #user prompting again
    while (True):
        file = input("What would you like to call the excel file?\n")
        if len(file) > 1: break
        else: print("Please enter a valid filename\n")

    
    #set variables
    isYear = False
    isKeyword = False
    isConference = False
    conferences = []
    keywords = []
    data = pd.DataFrame()
    first = True
    
    #user menu loop
    while (True):
        
        if (isYear): print("Year Range Set!")
        if (isKeyword): print("Keyword Search Set!")
        if (isConference): print("Conference Search Set!")
        
        print("\n<--Query Menu--->")
        if (first == False): print("Enter 4 to start search or set another menu option.")
        else: print("Please enter the corresponding menu option!")
        
        first = False
        

        val = input("1- Keyword Search \n2- Year Range \n3- Conference Search \n4- Start Search\n")
        if (len(val) < 1 or len(val) > 2):
            print("Please enter a valid menu option!")
            continue
        
        #Keyword Search
        if (val == "1"):
            print ("<---Search by Keywords--->") 
            isKeyword = True
            textfile = input("Please enter a text file with each keyword seperated by a comma.\nEx: hci, binary trees, algorithm\n\nWhat is the textfile name?\n") + ".txt"
            keys = read_file(textfile)
            
        
        #Year Search
        elif (val == "2"):
            print("<--- Search by Year -->\n")
            isYear = True
            startYear = input("What is the starting year?\n")
            endYear = input("What is the ending year?\n")
           
        #Conference Search
        elif (val == "3"):
            print("<--- Search by Conference -->\n")
            isConference = True
            journal_type = input("Are you searching for proceedings or periodicals?\nEnter the corresponding menu option\n0- Proceedings\n1- Periodicals\n")
            
            textfile = ""
            if (journal_type == "0"): textfile = input("\nPlease enter the name of text file with each conference acronym seperated by a comma.\nEx: CHI, C&C, DIS\n\nWhat is the textfile name?\n") + ".txt"
            else: textfile = input("\nPlease enter the name of text file with each conference venue seperated by a comma.\nEx: ACM Transactions on Mathematical Software, ACM Transactions on Computer-Human Interaction \n\nWhat is the textfile name?\n") + ".txt"
            
            conferences = read_file(textfile)
            #read in textfile

        #breaks out of user menu after else statement, looping stops here

        else:
            
            x = input("Press Enter to start the search!")
            
            print("Please be patient... starting the search")
            #if keywords search
            if (isKeyword): 
                
                #pulls data from db
                cursor.execute("SELECT title FROM " + database)
                titles = cursor.fetchall()
                cursor.execute("SELECT full_text FROM " + database)
                fulltexts = cursor.fetchall()
                cursor.execute("SELECT abstracts FROM " + database)
                abstracts = cursor.fetchall()
                cursor.execute("SELECT keywords FROM " + database)
                keywords = cursor.fetchall()
                cursor.execute("SELECT article_ids FROM " + database)
                ids = cursor.fetchall()
                
                #variables to put into new table
                count = 0
                include_ids = []
                scores = []
                include_keywords = []
                empty_fields = []
                fields_with_key = []
                
                
                #loops through all articles
                for article in ids:

                    empty_field = ""
                    included_keys = ""
                    field_with_key = ""
                    inTitle = False
                    inFullText = False
                    inAbstract = False
                    inKeywords = False
                    inPaper = False
                    
                    #loops through all the keywords
                    for k in keys:
                        
                        #easier variable referencing
                        t = titles[count][0]
                        f = fulltexts[count][0]
                        a = abstracts[count][0]
                        keyword = keywords[count][0]
                        inPaper = True
                        
                        #check string equiv with lower so it is case insensitive
                        if k.lower() in t.lower():
                            inTitle = True
                            inPaper = True
                            field_with_key = field_with_key + k + ": title, "
                            
                        if k.lower() in f.lower(): 
                            inFullText = True
                            inPaper = True
                            field_with_key = field_with_key + k + ": full_text, "
                            
                        if k.lower() in a.lower(): 
                            inAbstract = True
                            inPaper = True
                            field_with_key = field_with_key + k + ": abstract, "
                            
                        if k.lower() in keyword.lower(): 
                            inKeywords = True
                            inPaper = True
                            field_with_key = field_with_key + k + ": keywords, "
                        
                        #score only for including keys
                        if (inPaper): included_keys = included_keys + k + ", "
                        
                        first = False
                        
                    #checking if empty feild, then add to empty feilds
                    if len(a) < 5: 
                        empty_field = empty_field + "abstract, "
                    if len(f) < 5: 
                        empty_field = empty_field + "full text, "
                    if len(keyword) < 2: 
                        empty_field = empty_field + "keywords"
                            
                    #if it has a score, add article to list
                    #add data for new columns in arrays
                    if (inPaper):
                        
                        #calculate score
                        score = 0
                        if (inTitle): score = score + 2
                        if (inFullText): score = score + 1.5
                        if (inAbstract): score = score + 1
                        if (inKeywords): score = score + .5
                        
                        
    
                        #add article id to array
                        include_ids.append(article[0])
                        
                        #add variables to arrays (will become future columns)
                        scores.append(score)
                        
                        if (len(field_with_key) < 2): field_with_key = "N/A"
                        fields_with_key.append(field_with_key)
                        
                        if (len(included_keys) < 2): empty_field = "N/A"
                        include_keywords.append(included_keys)
                        
                        if (len(empty_field ) < 2): empty_field = "N/A"
                        empty_fields.append(empty_field)

                        
                    count = count + 1
            
                #turn array into tuple for referencing
                tuple_ids = tuple(include_ids)


                #drop table if it already exists
                cursor.execute("DROP TABLE IF EXISTS " + database + "_keys")
                
                #copy to new table, taking articles with a score in array
                cursor.execute("CREATE TABLE " + database + "_keys AS SELECT * FROM " + database + " WHERE article_ids IN {}".format(tuple_ids))

                #rename database
                database = database + "_keys"
              
                #add new colums
                cursor.execute("ALTER TABLE " + database + " ADD score TEXT")
                cursor.execute("ALTER TABLE " + database + " ADD included_keywords TEXT")
                cursor.execute("ALTER TABLE " + database + " ADD fields_with_key TEXT")
                cursor.execute("ALTER TABLE " + database + " ADD empty_fields TEXT")

                
                count = 0
                
                #add values for new columns
                for i in include_ids:
                    cursor.execute("UPDATE " + database + " SET score=?, included_keywords=?, fields_with_key=?, empty_fields=? WHERE article_ids=?", (str(scores[count]), include_keywords[count], fields_with_key[count], empty_fields[count], str(i)))
                    count = count + 1

                
                #get data from database
                if (isYear): data = pd.read_sql(("SELECT * FROM " + database + " WHERE CAST(publication_year AS INT) BETWEEN " + startYear + " AND " + endYear + " AND article_ids IN {}".format(tuple_ids)), connection)
                if (not isConference): data = pd.read_sql(("SELECT * FROM " + database + " WHERE article_ids IN {}".format(tuple_ids)), connection)
                
            #conference search
            if (isConference):
                
                #get all publication acronyms or venues
                if journal_type == "1": cursor.execute("SELECT publication_acronym FROM " + database)
                else: cursor.execute("SELECT publication_venue FROM " + database)
                
                acronyms = cursor.fetchall()
                
                #select all article ids
                cursor.execute("SELECT article_ids FROM " + database)
                ids = cursor.fetchall()
                
                acrm = ""
                count = 0
                include_ids = []
                
                #loop through all acronyms
                for a in acronyms:
                    
                    #loops through all conferences to search for
                    for conf in conferences:

                        c = len(conf)
                        
                        #if shorter than conference, break
                        if (len(a[0]) <= c): continue
                    
                        #if journal, check if string contains another string
                        # journal names do not have acronym metadata, so must search entire
                        # journal title
                        if (journal_type == "2"):
                            if conf in a[0]:
                                include_ids.append(ids[count][0])
                                continue
                       
                        #if proceedings 
                        else:
                    
                            #check if empty string
                            if not(a[0][c] == " " or a[0][c] == "'"): continue
                            acrm = a[0][0:c]
    
                            #checks if matches conference acronym and prevent extended abstracts and other conferences by checking length
                            if (acrm == conf and len(a[0]) < len(acrm) + 5):
                                include_ids.append(ids[count][0])
                                continue
                            
                            
                    count = count + 1
                            
                #change to tuple for sql
                tuple_ids = tuple(include_ids)
                
                if (isYear): data = pd.read_sql(("SELECT * FROM " + database + " WHERE CAST(publication_year AS INT) BETWEEN " + startYear + " AND " + endYear + " AND article_ids IN {}".format(tuple_ids)), connection)
                
                #create dataframe for ids put in excel
                else: data = pd.read_sql(("SELECT * FROM " + database + " WHERE article_ids IN {}".format(tuple_ids)), connection)
    
            #search by year
            elif (isYear):
                data = pd.read_sql("SELECT * FROM " + database + " WHERE CAST(publication_year AS INT) BETWEEN " + startYear + " AND " + endYear, connection)
            
            else:
                data = pd.read_sql("SELECT * FROM " + database, connection)
            
            print("Creating " + file + "...")
            break   

    #write document
    with pd.ExcelWriter(file+".xlsx", engine='xlsxwriter', engine_kwargs={'options':{'strings_to_formulas': False, 'strings_to_urls': False}}) as writer:
        writer.book.use_zip64()
        data.to_excel(writer, sheet_name='bar')
        
    print("...finished creating " + file )

 


#calls user menu
terminal_prompt()