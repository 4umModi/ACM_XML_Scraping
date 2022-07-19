#XML_Scraping.py
#Script by Forum Modi, started by Alexander Hayes
#
#Function: Takes XML data from the ACM DL and inserts into a db file
#Works with sister python file, queries.py which will take db file
#and insert into Excel Sheet with selection options
#



#import libraries
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import os
from tqdm import tqdm
import sqlite3 as sql
clean = re.compile('<.*?>')


#function to remove html tags and replace with string
#also replace new lines and quotes with empty string
def remove_tags(text):
    """Remove html tags from a string"""
    return re.sub(clean, '', text).replace("\n", "").replace('"',"") #replace new lines


#function for XML parsing
def XML_parsing(folder, isProceedings, isPeriodicals, isNestedFolder, searchFilename, counterValue):

    #create db
    connection = sql.connect(folder+".db")
   
    #cursor, communicates SQL statements to db
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + folder)
    
    
    #runs SQL statement, creates table
    cursor.execute("CREATE TABLE IF NOT EXISTS " + folder + " (article_ids TEXT, title TEXT, dois TEXT, urls TEXT, publication_year TEXT, publication_acronym TEXT, publication_venue TEXT, article_type TEXT, keywords TEXT, abstracts TEXT, authors TEXT, author_instituions TEXT, author_ids TEXT, page_nums TEXT, page_starts TEXT, page_ends TEXT, full_text TEXT, publishers TEXT, sponsors TEXT)")    
    
    #fixes random bug where it doesn't loop correctly
    error_fix = True
    
    #loop through all the xml files in the data folder
    for file in os.listdir(folder):
        
        # counter
        counterValue = counterValue - 1
        #print("count: " + str(counterValue))
        
        
        #finds directory based on how XML files are stored
        directory = folder + "/" + file
        if (isNestedFolder): directory = folder + "/" + file + "/" + file + ".xml"    
        #open and read files
        try:
            with open(directory, "r", encoding='utf-8', errors='ignore') as fh:
                xml_doc = fh.read()
                #debugging to print XML file
                #print(xml_doc[0:300000])
                #print(directory)
              

        except: 
            #random error fix (proceedings never correctly terminate)
            if error_fix == False:
                connection.commit()
                connection.close()  
                return
                
            print("Did you enter the correct settings?\n Please try again.\n")
            terminal_prompt()
            return
        
        error_fix = False
            
        #xml document
        soup = BeautifulSoup(xml_doc, 'xml')
        
        #checks if proceeding or journal, then gets the publication venue/year
        article_id = "N/A"
        title = "N/A"
        doi = "N/A"
        url = "N/A"
        year = "N/A"
        acronym = "N/A"
        publication_venue = "N/A"
        article_type = "N/A"
        keyword = ""
        abstract = "N/A"
        author = "N/A"
        author_instituion = ""
        author_id = ""
        page_num = "N/A"
        page_start = "N/A"
        page_end = "N/A"
        full_text = "N/A"
        publisher = "N/A"
        sponsor = "N/A"


        #Periodicals and Journals parse differently 
        
        #parsing for proceedings
        #proceeding name, acronym, year, sponsors
        if (isProceedings):
            for procs in tqdm(soup.find_all("proceeding_rec")):  
               publication_venue = procs.proc_desc.text 
               if procs.proc_title is not None and len(procs.proc_title.text) > 1: publication_venue = publication_venue + " on " + procs.proc_title.text
               elif procs.proc_subtitle is not None and len(procs.proc_subtitle.text) > 1: publication_venue = publication_venue + " on " + procs.proc_subtitle.text
               if procs.acronym is not None and len(procs.acronym.text) > 1: acronym = procs.acronym.text
               if procs.copyright_year is not None: year = procs.copyright_year.text
               if procs.publisher.publisher_name is not None: publisher = procs.publisher.publisher_name.text   
               if procs.sponsor_rec is not None: 
                   sponsor = procs.sponsor_rec.sponsor_name.text

        #periodicals name, acronym, year, sponsors   
        else:
            for jours in tqdm(soup.find_all("journal_rec")):      
               publication_venue = jours.journal_name.text
               if jours.journal_abbr is not None and len(jours.journal_abbr) > 1: acronym = jours.journal_abbr.text
               if jours.publisher.publisher_name is not None: publisher = jours.publisher.publisher_name.text
               
            for issues in tqdm(soup.find_all("ccc")):      
               if issues.copyright_holder.copyright_holder_year is not None: year = issues.copyright_holder.copyright_holder_year.text
               
            if jours.sponsor_rec is not None: 
                sponsor = procs.sponsor_rec.sponsor_name.text
           
        
        #No difference Between periodicals/proceedings onward

        #find all tags with article_rec on the xml files 
        for article in tqdm(soup.find_all("article_rec")):
            
            #reset variables for each loop
            author = ""
            author_instituion = ""
            author_id = ""
            

            
            #get article information from metadata
            title = article.title.text
            article_id = article.article_id.text
            url = article.url.text
            doi = article.doi_number.text
            

            
            #if article has subtitle, then include it to the title of the article
            if article.subtitle is not None: title = title + ": " + article.subtitle.text
                
            #finds starting/ending page
            if article.page_from is not None: page_start = article.page_from.text
            if article.page_to is not None: page_end = article.page_to.text
            
            #finds number of pages, or calculates number of pages
            if article.pages is not None: page_num = article.pages.text
            elif(article.page_from is not None and article.page_to is not None): 
            
                try: page_num = (str(int(article.page_to.text) - int(article.page_from.text)))
                except: page_num = "N/A"
    
            first = True
            #get author metadata
            for auths in tqdm(article.authors.find_all("au")):
                 #checks for author name(checks for middle name/blank author names)
                 
                 if(len(auths.first_name.text.strip()) > 0) or (len(auths.last_name.text.strip()) > 0):
                     if (first == False): author = author + "; "
                     author = author + auths.first_name.text + " " + auths.last_name.text
                     first = False
                 else: author = author + "N/A"
                 
                     
                 #gets author ids 
                 if auths.person_id is not None: author_id = author_id + auths.person_id.text + "; "
                 else: author_id = "N/A"
                 if auths.affiliation is not None and len(auths.affiliation.text) > 2: author_instituion = author_instituion + auths.affiliation.text +"; "
                 else: author_instituion = "N/A"
                
        
            #gets keywords
            first = True
            keyword = ""

            for kws in tqdm(article.find_all("kw")):
                if kws is not None:
                    if (not first): keyword = keyword + "; "
                    keyword = keyword + str(kws.text)
                    first = False
                    
            if len(keyword) < 1: keyword = "N/A"
            
    
            #Awkward fix to error, can't reference article_type.art_type, have to manually parse through metadata to find value
            article_type = "N/A"
            for art in tqdm(article.find_all("article_type")):
                article_type = str(art)
                article_type = article_type[(article_type.index("=\"") + 2): article_type.index(">") - 2]
            
            #get length of abstract and title
            abst_len = 0
            title_len = len(title)
            
            #finds abstract
            try:
                abstract = ((article.abstract.text))
                abst_len = len(abstract)
            except AttributeError:
                abstract = "N/A"
                
            #article full text
            if article.fulltext is not None:
                full_text = ((article.fulltext.ft_body.text).strip())
                
                
                #makes it so there isn't repeat text- in fulltext
                try:
                    startIndex = full_text.index("INTRODUCTION")
                except:
                    startIndex = abst_len + title_len 
                if len(full_text) > startIndex:
                    #print("MATCH!")
                    full_text = full_text[startIndex:len(full_text)]

                
            if len(full_text) < 2: full_text = "N/A"
            if len(abstract) < 2: abstract = "N/A"
            
                
                
            cursor.execute("INSERT INTO " + folder + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(article_id, title, doi, url, year, acronym, publication_venue, article_type, keyword, abstract, author, author_instituion, author_id, page_num, page_start, page_end, full_text, publisher, sponsor))
            connection.commit()
            
        if (counterValue == 0):
            break 
    
    connection.close()  
        
           
        
                
#user menu
def terminal_prompt():
    
    #varibles for terminal prompting
    #folder name variable
    folder_name = ""
    isProceedings = True;
    isPeriodicals = False;
    isNestedFolder = False;
    search_type = "proceedings"
    searchFilename= ""
    nested_folder = " without nested folders"
    
    #prompts user 
    print("<---Hello, welcome to the ACM database XML parsing script!--->\n")
    
    #directory for files
    var = input("<---Please enter the folder name that includes your XML files--->\n")
    folder_name = var
    
    #proceedings or periodicals
    var = input("\n<---Do you have proceedings or periodicals?--->\nSelection Menu\n0 - proceedings\n1 - periodicals\n" )
    if (var == "1"):
        isProceedings = False
        isPeriodicals = True;
        search_type = "periodicals"
        
    #Files are stored differently (Zaidat has in one folder (0), Forum has in nested folders (1))
    var = input("\n<---Are your XML files in nested folders?--->\nSelection Menu\n0 - No(default)\n1 - Yes\n")
    if (var == "1"): 
        isNestedFolder = True;
        nested_folder = " with nested folders"      
        
    
    #For debugging
    #number of files to go through
    #counterValue = input("\n<---Enter the amount of proceedings/periodicals you would like to parse through--->\nEnter 0 or all for all proceedings\n")
    #if (counterValue == "0"): counterValue = "all"

    
    
   
    #if (counterValue == "all"): counterValue = 0
    
    counterValue = 0
    
    var = input("Press Enter to Start!\n")
    
    #calls parsing function
    XML_parsing(folder_name, isProceedings, isPeriodicals, isNestedFolder, searchFilename, int(counterValue))
    
    
#calls terminal
terminal_prompt()


            
