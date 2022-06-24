#import libraries
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from tqdm import tqdm
clean = re.compile('<.*?>')

#Left to implement

#put into SQL

#discluding duplicates
#searching by year
#   -start/end year range
#searching by venue
#   - disclude certain keywords (extneded abstracts)
#   - generate csv of certain venue(s)
#search by article type
#   - option to disclude certain types
#search by author
#   - authors who have published numerous papers in field
#   - search by author


#function to remove html tags and replace with string
#also replace new lines and quotes with empty string
def remove_tags(text):
    """Remove html tags from a string"""
    return re.sub(clean, '', text).replace("\n", "").replace('"',"") #replace new lines

#create list to hold titles, articles_ids, urls, abstracts, etc.
titles = []
article_ids = []
dois = []
urls = []
abstracts = []
page_nums = []
page_starts = []
page_ends = []
authors = []
author_instituions = []
author_ids = []
publishers = []
sponsors = []
keywords = []
publication_venues = []
publication_years = []
article_types = []
    
outputFilename = "metadata.csv"

#function for XML parsing
def XML_parsing(folder, isProceedings, isPeriodicals, isNestedFolder, outputFilename, counterValue, isYearRange, yearStart, yearEnd):
    
    yearStart = int(yearStart)
    yearEnd = int(yearEnd) 

    filename = outputFilename
    #for loop to loop through all the xml files in the data folder
    for file in os.listdir(folder):
        
        
        counterValue = counterValue - 1
        print("count: " + str(counterValue))
        
        
        #finds directory based on how XML files are stored
        directory = folder + "/" + file
        if (isNestedFolder): directory = folder + "/" + file + "/" + file + ".xml"    
    
    
       #open and read files
        try:
            with open(directory, "r", encoding='utf-8', errors='ignore') as fh:
                xml_doc = fh.read()
                #debugging to print XML file
                #print(xml_doc)
                print(directory)
            
        except: return
            

            
        #xml document
        soup = BeautifulSoup(xml_doc, 'xml')
        
        #checks if proceeding or journal, then gets the publication venue/year
        proc_jour_name = "N/A"
        publish_year = "N/A"
        if (isProceedings):
            for procs in tqdm(soup.find_all("proceeding_rec")):      
               if procs.copyright_year is not None: publish_year = procs.copyright_year.text
               proc_jour_name = procs.proc_desc.text
               
        else:
            for jours in tqdm(soup.find_all("journal_rec")):      
               proc_jour_name = jours.journal_name.text
            for issues in tqdm(soup.find_all("ccc")):      
               if issues.copyright_holder.copyright_holder_year is not None: publish_year = issues.copyright_holder.copyright_holder_year.text
           
        if (isYearRange):
            print("\nPaper from" + publish_year + " within" + str(yearStart) + " and " + str(yearEnd))
            if (publish_year == "N/A" or int(publish_year) < int(yearStart) or int(publish_year) > int(yearEnd)): 
                print("not within")
                continue
                
        
        #loops through finding author information for each author
        for article_auths in tqdm(soup.find_all("authors")):  
            
            #variables for holding each authors information for article
            author_list = ""
            author_list_ids = ""
            author_list_affil = ""
            
            for auths in tqdm(article_auths.find_all("au")):
                
                #checks for author name(checks for middle name/blank author names)
                if(len(auths.first_name.text.strip()) > 0) or (len(auths.last_name.text.strip()) > 0):
                    if (auths.middle_name.text is not None): author_list = author_list + " " + auths.first_name.text + " " + auths.middle_name.text + auths.last_name.text + "; "
                    else: author_list= author_list + auths.first_name.text + " " + auths.last_name.text + "; "
                else: author_list = author_list + "N/A"
                
                    
                #gets author ids 
                if auths.person_id is not None: author_list_ids = author_list_ids + auths.person_id.text + "; "
                else: author_list_ids = "N/A"
                if auths.affiliation is not None and len(auths.affiliation.text) > 2: author_list_affil = author_list_affil + auths.affiliation.text +"; "
                else: author_list_affil = "N/A"
                
            authors.append(author_list)
            author_ids.append(author_list_ids)
            author_instituions.append(author_list_affil)
        
            
        #gets publisher name    
        publish = "N/A"
        for pubs in tqdm(soup.find_all("publisher")):
            publish = pubs.publisher_name.text
    
        #gets sponsor name
        sponsor = "N/A"
        for spons in tqdm(soup.find_all("sponsor_rec")):
            sponsor = spons.sponsor_name.text
        
        #find all tags with article_rec on the xml files 
        for article in tqdm(soup.find_all("article_rec")):
            title = article.title.text
       
            kw_list = "N/A"
            for kws in tqdm(article.find_all("keywords")):
                kw_list = ""
                kw_list = str(kws.kw.text) + "; "
            keywords.append(kw_list)
            
    
            #Awkward fix to error, can't reference article_type.art_type, have to manually parse through metadata to find value
            article_type = "N/A"
            for art in tqdm(article.find_all("article_type")):
                article_type = str(art)
                article_type = article_type[(article_type.index("=\"") + 2): article_type.index(">") - 2]
    
            #if article has subtitle, then include it to the title of the article
            if article.subtitle is not None:
                title = title + ": " + article.subtitle.text
                
            titles.append(title)  #append title to titles list
            article_ids.append(article.article_id.text) #append articles_ids to the article_ids list
            urls.append(article.url.text) #append url to url lists
            dois.append(article.doi_number.text) #append doi to doi lists
    
            #finds starting page, checks if present
            if article.page_from is not None: page_starts.append(article.page_from.text)
            else: page_starts.append("N/A")
            
            #finds ending page, checks if present
            if article.page_to is not None: page_ends.append(article.page_to.text)
            else: page_ends.append("N/A")
            
            #finds number of pages, or calculates number of pages
            page_num = "N/A"
            if article.pages is not None: page_nums.append(article.pages.text)
            elif(article.page_from is not None and article.page_to is not None): 
            
                try: page_nums.append((str(int(article.page_to.text) - int(article.page_from.text))))
                except: page_nums.append("N/A")
    
            else: page_nums.append(page_num)
                    
            publishers.append(publish)
            sponsors.append(sponsor)
            publication_venues.append(proc_jour_name)
            publication_years.append(publish_year)
            article_types.append(article_type)
            
            
            #finds abstract
            try:
                abstracts.append(remove_tags(article.abstract.text))
            except AttributeError:
                abstracts.append("")
                
        #for debugging mode
        if (counterValue == 0):
            break
                
    #creates CSV dataframe
    df = pd.DataFrame(
        {
    
            "article_ids": article_ids,
            "titles": titles,
            "urls": urls,
            "DOIs": dois,
                
            "abstract": abstracts,
            "keywords": keywords,
            "authors": authors,
            "author_ids": author_ids,
            "authors_instituion": author_instituions,
                
            "number of pages": page_nums,
            "start page": page_starts,
            "end page": page_ends,
                
            "publication venue": publication_venues,
            "publication year": publication_years,
            "publisher": publishers,
            "article type": article_types,
            "sponsor": sponsors,
        }
    )
    
    #creates CSV file
    df.to_csv(filename,index=False)
        
                
#user menu
def terminal_prompt():
    
    #varibles for terminal prompting
    #folder name variable
    folder_name = ""
    isProceedings = True;
    isPeriodicals = False;
    isNestedFolder = False;
    isYearRange = False;
    yearStart = "0"
    yearEnd = "0"
    search_type = "proceedings"
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
        
    #number of files to go through
    counterValue = input("\n<---Enter the amount of proceedings/periodicals you would like to parse through--->\nEnter 0 or all for all proceedings\n")
    if (counterValue == "0"): counterValue = "all"

    #selection menu
    while (True):
        
        var = input("\n<---Parsing Critera--->\nSelection Menu\n1 - Year Range\n2 - Publication Venues\n3- Article Type\n4- Search by Author\n5- Finished, generate csv file--->\n")
        if (var == "1"):
            print("<---Searching by Year--->")
            isYearRange = True
            yearStart = input("Enter Year Starting Range\n")
            yearEnd = input("Enter Year Ending Range\n")
        else:
            break;
    
    #output file name 
    outputFilename = input("<---What would you like to name the CSV output file?--->\n") + ".csv"
    
    
    print("<---Parsing through " + search_type + " files in " + folder_name + "/ for " + counterValue + " files. Outputting to " + outputFilename + "-->\n")
    if (counterValue == "all"): counterValue = "0"
    
    var = input("Press Enter to Start!\n")
    #calls parsing function
    XML_parsing(folder_name, isProceedings, isPeriodicals, isNestedFolder, outputFilename, int(counterValue), isYearRange, yearStart, yearEnd)
    
#calls terminal
terminal_prompt()

#More debugging
'''print("article ids " + str(len(article_ids)))
print("titles " + str(len(titles)))
print("urls " + str(len(urls)))
print("doi " + str(len(dois)))
print("abstracts " + str(len(abstracts)))
print("authors " + str(len(authors)))
print("author ids " + str(len(author_ids)))
print("author insti " + str(len(author_instituions)))
print("page num " + str(len(page_nums)))
print("start " + str(len(page_starts)))
print("end " + str(len(page_ends)))
print("publishers " + str(len(publishers)))
print("sponsors " + str(len(sponsors)))
print("keywords " + str(len(keywords)))
'''
            
