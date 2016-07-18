import logging              # logging module
import logging.handlers     # handler module - handlers write log messages
import multiprocessing      # multiprocessing module
import requests 		     # web interface for Python module
import json     		     # json parser module
from random import randrange, randint, choice # random module
import time 		     # time module
import mysql.connector      # connect to MySQL module
import re                   # regular expressions module
#import pprint              # pretty printing module for debugging
import sys                  # sys module - system specific parameters and functions
#import unittest            # module to test strings (e.g., assertEqual())
import smtplib              # sending emails with python, so gangster

import config               # the config file for the scraper 

# Logging variables
# make sure the requests module only sends warning or higher
# to not clutter the log, we do not want to record everytime it makes a connection etc. 
logging.getLogger("requests").setLevel(logging.WARNING) 

LEVEL = logging.INFO               # the level of the messages we send
LOGGER = 'Steam Crawler'           # the name of the logger

####################################################

def send_email(message): #Keren, added try/ catch in here; I dont raise this (it's not important)
    try:
        server = smtplib.SMTP(config.server, config.port)
        
        server.ehlo()
        server.starttls()
        server.ehlo()
        
        server.login(config.username, config.password)
        
        msg = '\n'+message
        server.sendmail(config.username, config.recipient, msg)
        
        server.close()
    except Exception as e:
        print('send_email raised an exception with message: %s' % str(e))

def check_if_table_exists(schemaname,tablename): #Keren, added try & catch in here; I decided to raise it (not very clean) but it's not too important, if there's something wrong we will find it early in the main function
    check = 'SELECT table_name \n FROM information_schema.tables \n WHERE table_schema =\'' + schemaname +'\'\n AND table_name =\'' +tablename + '\';' 
    
    try:
        cnx = mysql.connector.connect(**config.sqlconnectionoptions)
        cursor = cnx.cursor()
        try:     
            cursor.execute(check)
            result = cursor.fetchone()
            if result is not None:
                if str(result[0]) == str(tablename):
                    return True
                else:
                    return False
            else:
                return False
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.CR_SERVER_LOST_EXTENDED:
                print("check_if_table_exists - tablename: %s - Lost connection" % tablename)
            else:
                print("check_if_table_exists - tablename: "+tablename+" - Failed query somehow; {0}".format(str(e)))
            cnx.rollback()        
            raise
        finally:        
            cursor.close()
            cnx.close()
    except mysql.connector.Error as e:
        if e.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("check_if_table_exists - tablename: %s - Something is wrong with your user name or password" % tablename)
        elif e.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("check_if_table_exists - tablename: %s - Database does not exist" % tablename)
        elif e.errno == mysql.connector.errorcode.CR_CONN_HOST_ERROR:
            print("check_if_table_exists - tablename: %s - Server is offline" % tablename)
        else:
            print("check_if_table_exists - tablename: "+tablename+" - Failed connecting db somehow; {0}".format(str(e)))
        raise

def createtable(schemaname,tablename,primarykey): #Keren: This is an important function and should be handled better (not sure yet, for now I raise)
    if primarykey == True:  
        create = ('''
        CREATE TABLE %s (
        steamid bigint(17),
        profile_name varchar(100),
        avatar_url varchar(100),
        community_visibility_state tinyint(1),
        profile_community_state tinyint(1),
        lastlogoff int(10),
        permission_to_comment tinyint(1),
        realname varchar(100),
        primaryclanid bigint(18),
        time_created int(10),
        countrycode char(2),
        statecode varchar(3),
        cityid mediumint(5),
        time_scraped int(10),
        PRIMARY KEY (`steamid`)
        ) ENGINE = MYISAM;
        ''' % (schemaname+'.'+tablename))
   
    if primarykey == False:
        create = ('''
        CREATE TABLE %s (
        steamid bigint(17),
        profile_name varchar(100),
        avatar_url varchar(100),
        community_visibility_state tinyint(1),
        profile_community_state tinyint(1),
        lastlogoff int(10),
        permission_to_comment tinyint(1),
        realname varchar(100),
        primaryclanid bigint(18),
        time_created int(10),
        countrycode char(2),
        statecode varchar(3),
        cityid mediumint(5),
        time_scraped int(10)
        ) ENGINE = MYISAM;
        ''' % (schemaname+'.'+tablename))        
    try:
        cnx = mysql.connector.connect(**config.sqlconnectionoptions)
        cursor = cnx.cursor()
        try:        
            cursor.execute(create)
            cnx.commit()
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.CR_SERVER_LOST_EXTENDED:
                print("createtable - tablename: %s - Lost connection" % tablename)
            else:
                print("createtable - Failed creating table:"+tablename+" somehow; {0}".format(str(e)))
            cnx.rollback()        
            raise
        finally:
            cursor.close()
            cnx.close()
    except mysql.connector.Error as e:
        if e.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("createtable - tablename: %s - Something is wrong with your user name or password" % tablename)
        elif e.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("createtable - tablename: %s - Database does not exist" % tablename)
        elif e.errno == mysql.connector.errorcode.CR_CONN_HOST_ERROR:
            print("createtable - tablename: %s - Server is offline" % tablename)
        else:
            print("createtable - tablename: "+tablename+" - Failed connecting db somehow; {0}".format(str(e)))
        raise
        
def check_table_size(schemaname,tableprefix): #Keren: because this is such an important function, I let it handle errors just like the insert function
                                                # I am a bit worried what will happen if createtable fails or raises, we need a except Exception block for this... right?
    retrievetables = """
    SELECT table_name, table_rows , round(((data_length + index_length)/1024/1024),2) "table_size"
    FROM information_schema.TABLES WHERE table_schema = %s AND table_name like %s ORDER by CREATE_TIME ASC;
    """ % ('\'' + schemaname +'\'', '\''+ tableprefix +'%' +'\'')  
 
    try:        
        cnx = mysql.connector.connect(**config.sqlconnectionoptions)
        cursor = cnx.cursor()
        try:
            cursor.execute(retrievetables)
            all_tables = cursor.fetchall()
            number_of_tables = len(all_tables)

            if number_of_tables == 0: 
               enter_str = "no_tables"
               exception_message = ""
               return_data = tableprefix+'1'

            else:
                size_focal_table = float(all_tables[-1][-1])
                if size_focal_table > config.max_table_size:     
                    enter_str = "new_table"
                    exception_message = ""
                    return_data = tableprefix+str(number_of_tables+1)
                else:
                    enter_str = "select_success"
                    exception_message = ''
                    return_data = tableprefix+str(number_of_tables)
        except mysql.connector.Error, e: 
            if e.errno == mysql.connector.errorcode.CR_SERVER_LOST_EXTENDED:
                print("check_table_size - Lost connection")
            else:
                print("check_table_size - Failed retrieving table info; {0}".format(str(e)))
            cnx.rollback()
            enter_str = "select_error"
            exception_message = str(e)
            return_data = ''
        finally:
            cursor.close()
            cnx.close()
    except mysql.connector.Error, e:
        if e.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("check_table_size - Something is wrong with your user name or password")
        elif e.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("check_table_size - Database does not exist")
        elif e.errno == mysql.connector.errorcode.CR_CONN_HOST_ERROR:
            print("check_table_size - Server is offline")
        else:
            print("check_table_size - Failed connecting db somehow; {0}".format(str(e)))
        enter_str = "connection_error" 
        exception_message = str(e)
        return_data = ''
    return([enter_str, exception_message, return_data])         
        
def random_steamid(N):
    """
    Summary: Sample N random steamids
    Input: an integer number
    Function: random number generator (note that upper bound is dynamic in practice)
    Output: a list of steamids, every element is a steamid class long
    """
    x = [randrange(config.firstid_ever, config.lastid_ever, 1) for p in range(1,N+1)]
    x = set(x) # remove duplicates
    while len(x) < N: # add until N
        x.add(randint(config.firstid_ever, config.lastid_ever))
    return(list(x)) # return as a list

def grabber_PlayerSummaries(steamids): #Keren: I didn't touch this, but it could be a good idea to put a try block around the get to properly catch the weird error (HTTPpool or something)
    """
    Summary: Calls the PlayerSummaries API and parses it
    Input: a list of 1 to 100 steamids
    Function: Construct the URL and call the server for the json data
        # then parse the json data
    Output: a list of steamids and their data in key-value pairs (dictionary)
    """
    if isinstance(steamids,long):
        steamids = [steamids] # if there is only a single steamid, change it to a list
    
    steamids100 = ','.join(str(x) for x in steamids) # stacks the steamids
    url = (config.getplayersummariesurl+"?key=%s&steamids=%s") % (choice(config.keylist),steamids100) # construct the URL
    
    # Enter some code here that repeats below until it has a satisfactory status_code
    attempts = 0
    while attempts < config.number_of_attempts: # we are going to repeat this until we have a satisfactory status_code, but up to a point!   
        attempts = attempts + 1
        timestamp = int(round(time.time())) # record a timestamp of when the api is called -> enter later
        grabbed = requests.get(url, headers=config.headers) # get request on the url
        status_code = grabbed.status_code # get the status code that was returned
        if 500 <= status_code <= 599: 
            time.sleep(config.sleep_time)            
        else: # 5XX are server errors, so wait & try again, maybe the server is down for a second
            break
   
    if status_code == 200:               # 200 = OK
        # Extract the text
        jsontext = grabbed.text
        # Remove emojis and other crap not in mysql's utf-8
        jsontext_corrected = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]').sub(u'', jsontext)
        # Remove first part of avatar links
        jsontext_corrected = re.sub(r'https://steamcdn-a.akamaihd.net/steamcommunity/public/images/avatars/', "", jsontext_corrected)
        # Now parse the data and dig a little in the dictionary to get the list of ids
        datalist = json.loads(jsontext_corrected, encoding = "unicode") # parse the text input to a nested dictionary
        datalist = datalist['response']['players'] # dig a little in the dictionary to get the relevant list of players
        # Add a timestamp for all the ids
        for customer in range(0,len(datalist)):
            datalist[customer][u'time_scraped'] = timestamp
        # Return the data
        return(status_code, datalist)

    else:  # 400 = bad request, 401 = unauthorized, 403 = forbidden, 404 = not found, 5XX are server errors, 429 = too many requests 
        return(status_code, [])  
       
def insert_PlayerSummaries(list_input, tablename): #Keren: whole function rewritten ; important function ; I added the "if" statements here but are they really that important

    insert_str = ("INSERT INTO {0} "
        "(steamid, profile_name, avatar_url, community_visibility_state, "
        "profile_community_state, lastlogoff, permission_to_comment, realname, primaryclanid, "
        "time_created, countrycode, statecode, cityid, time_scraped)"
        "VALUES (%(steamid)s, %(personaname)s, "
        "%(avatarfull)s, %(communityvisibilitystate)s, %(profilestate)s, "
        "%(lastlogoff)s, %(commentpermission)s, %(realname)s, "
        "%(primaryclanid)s, %(timecreated)s, %(loccountrycode)s, "
        "%(locstatecode)s, %(loccityid)s, %(time_scraped)s)".format(tablename))

    defaults = {'steamid': None,
        'personaname': None,
        'profileurl': None,
        'avatar': None,
        'avatarmedium': None,
        'avatarfull': None,
        'personastate': None,
        'communityvisibilitystate': 0,
        'profilestate': 0,
        'lastlogoff': None,
        'commentpermission': None,
        'realname': None,
        'primaryclanid': None,
        'timecreated': None,
        'gameid': None,
        'gameserverip': None,
        'gameextrainfo': None,
        'cityid': None,
        'loccountrycode': None,
        'locstatecode': None,
        'loccityid': None,
        'personastateflag': None,
        'time_scraped': None
    }
    
    try:
        cnx = mysql.connector.connect(**config.sqlconnectionoptions)
        cursor=cnx.cursor()
        try:    
            cursor.executemany(insert_str, ({k: d.get(k, defaults[k]) for k in defaults} for d in list_input))	
            enter_str = "insert_success"
            exception_message = ''
        except mysql.connector.Error, e:
            if e.errno == mysql.connector.errorcode.CR_SERVER_LOST_EXTENDED:
                print("insert_PlayerSummaries - tablename: %s - Lost connection while inserting" % tablename)
            else:
                print("insert_PlayerSummaries - tablename: "+tablename+" - Failed inserting data; {0}".format(str(e)))
            cnx.rollback() #we need to test this, because I dont think it will rollback
            enter_str = "insert_error"
            exception_message = str(e)
        finally:
            cursor.close()
            cnx.close()
    except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print("insert_PlayerSummaries - tablename: %s - Something is wrong with your user name or password" % tablename)
            elif e.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print("insert_PlayerSummaries - tablename: %s - Database does not exist" % tablename)
            elif e.errno == mysql.connector.errorcode.CR_CONN_HOST_ERROR:
                print("insert_PlayerSummaries - tablename: %s - Server is offline" % tablename)
            else:
                print("insert_PlayerSummaries - tablename: "+tablename+" - Failed connecting db somehow; {0}".format(str(e)))          
            enter_str = "connection_error" 
            exception_message = str(e)
    return([enter_str, exception_message])
   
def chunks(l, n):
    """
    Summary: Divide a vector l (of steamids) in chunks of length n
    Input: a vector (l) and chunk size (n)
    Function: Make chunks of the vector
    Output: A list of lists
    """
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]
   
def Populate_QueueChunks(idsChunks_Queue,first_id,last_id,chunkSize):
    steamids = range(first_id, last_id)
    list_of_chunks = chunks(steamids, chunkSize)   
    for chunk in list_of_chunks:
        idsChunks_Queue.put(chunk)
        
def Populate_QueueChunks_random(idsChunks_Queue,chunkSize): # for debug & testing purposes
    steamids = random_steamid(config.batchsize)
    list_of_chunks = chunks(steamids, chunkSize)   
    for chunk in list_of_chunks:
        idsChunks_Queue.put(chunk)
        
def write_to_disk(list_to_disk): #Keren: new function; I actually fixed a bug here! it's a for append instead of w for write:)
    try:
        errorfile = open(config.errorfile, 'a')
        errorfile.write("##################\n")
        for item in list_to_disk:
            errorfile.write("%s\n" % item)
        errorfile.close() 
    except Exception as e:
        print(str(e))
        raise
        
#==============================================================================

class QueueHandler(logging.Handler):
    """
    This is a logging handler which sends events to a multiprocessing queue.
    
    The plan is to add it to Python 3.2, but this can be copy pasted into
    user code for use with earlier Python versions.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue
        
    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue.
        """
        try:
            ei = record.exc_info
            if ei:
                dummy = self.format(record) # just to get traceback text into record.exc_text
                record.exc_info = None  # not needed any more
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def listener_configurer():
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler(config.log_file, 'a', 3000000, 0) 
    f = logging.Formatter('%(asctime)s;%(processName)-10s;%(message)s')
    h.setFormatter(f)
    root.addHandler(h)

def listener_process(queue, configurer):
    configurer()
    
    while True:
        try:
            record = queue.get()
            if record is None: # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record) # No level or filter logic applied - just do it!
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            import traceback
            print >> sys.stderr, 'Whoops! Problem:'
            traceback.print_exc(file=sys.stderr)
            
def worker_configurer(queue):
    h = QueueHandler(queue) 
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG) # send all messages, for demo; no other level or filter logic applied.

def worker_process(queue, configurer, idsChunks_q, results_q):  # idsChunks_q,results_q
    configurer(queue)
    name = multiprocessing.current_process().name
    print('worker started: %s' % name)
    try:
        for chunk in iter(idsChunks_q.get, 'STOP'):
            logger = logging.getLogger(LOGGER) 
            
            try:
                status_returned, data_list = grabber_PlayerSummaries(chunk) # add if list is not empty
                
                if status_returned == 200:
                    if len(data_list) > 0:
                        results_q.put(data_list)
                    message = 'gw;1;%s;%s;200' % (min(chunk), len(data_list))
                    
                elif status_returned == 429:  
                    idsChunks_q.put(chunk)
                    message = 'gw;0;%s;%s, return to queue & sleep %s' % (min(chunk), status_returned, config.sleeptime429)
                    send_email(message)
                    print(message)
                    time.sleep(config.sleeptime429)
                    
                else:           
                    idsChunks_q.put(chunk)
                    message = 'gw;0;%s;%s, ids back to queue' % (min(chunk), status_returned)
                    send_email(message)
                    print(message)
               
            except Exception, e:
                idsChunks_q.put(chunk)
                message = 'gw;0;%s;e when grabbing: %s, ids back to queue & sleep %s' % (min(chunk), str(e), config.sleeptime_exception)
                send_email(message)
                print(message)
                time.sleep(config.sleeptime_exception) #Keren: this try block now captures the HTTPpool or whatever error, I don't really like it but it works ; 
                                                            # I didn't touch this function - except for some error messages - but we may want to check it later
            finally:
                logger.log(LEVEL, message)
                
    except Exception, e:
        message_worker2 = 'Something went horribly wrong in worker %s: %s' % (name, str(e))  
        send_email(message_worker2)
        print(message_worker2)
        raise
    print('worker finished: %s' % name) 
    return True
    
def db_worker(queue, configurer, results_q): #Keren: rewrote this function, start from here ; pay particular attention to the logic and control flow
    record_steamids =0    
    try:    
        configurer(queue)
        name = multiprocessing.current_process().name
        print('db_worker started: %s' % name)
        
        while True:
            logger = logging.getLogger(LOGGER) 
            
            # 1. Try to get chunks from the results queue, if success, proceed; if fail, break (maybe start another worker?)
            #Keren: breaking it is very problematic because then the db_worker stops and the others continue, this is a big problem
            # Maybe I misunderstand though
            
            try:
                record = results_q.get()
            except Exception as e:
                message1 = 'db;0;NA;NA;e getting from queue: %s, break' % str(e)
                logger.log(LEVEL, message1)
                send_email(message1)
                print(message1)
                break #Keren: should we really break here?
            
                # If it gets to the end of the queue, break
            if record is None:
                break
            
            # 2. Then, recover the table name in which the ids will be entered
                # output_table_size is a list of three elements
                # first is what happened, second is the optional error message, third is the name of the table
                # first element can be connection_error, select_error, select_success, new_table, or no_tables
            
            record_steamids = list(i['steamid'] for i in record)
            output_table_size = check_table_size(config.schemaname, config.tableprefix)
           
                # 2.1 If connection error or insert_error, write to disk ; I decided to handle both the same - I think it's a good idea
            if output_table_size[0] == "connection_error" or output_table_size[0] == "select_error":            
                try:
                    write_to_disk(record) # This is a new function
                    message2 = 'db;0;%s;%s;%s checking table size: %s, write to disk' % (min(record_steamids),len(record_steamids), output_table_size[0], output_table_size[1])
                except Exception as e:
                    message2 = 'db;0;%s;%s;%s checking table size: %s, NOT write to disk because: %s' % (min(record_steamids),len(record_steamids), output_table_size[0], output_table_size[1], str(e))
                finally: 
                    logger.log(LEVEL, message2) 
                    send_email(message2)
                    print(message2)
                    
                # 2.2 if successful, proceed (don't log)
            elif output_table_size[0] == "select_success":
                #print('1')                
                tablename = output_table_size[2]
                
                # 2.3 if need to make a new table
            elif output_table_size[0] == "new_table" or output_table_size[0] == "no_tables":
                #print('2')
                tablename = output_table_size[2]
                try:
                    #print('3')
                    createtable(config.schemaname, tablename, True)
                    #print('4')
                    message2 = 'db;1;%s;%s;successfully created table %s' % (min(record_steamids),len(record_steamids), tablename)
                except Exception as e:
                    #print('5')
                    message2 = 'db;0;%s;%s;e when creating table %s: %s, write to error table' % (min(record_steamids),len(record_steamids), tablename, str(e))
                    #print('6')                    
                    tablename = config.errortable_name
                finally: 
                    logger.log(LEVEL, message2) 
                    send_email(message2)
                    print(message2)
                     
                # 3. Then, try to insert the chunk in the database, but only if not written to disk already
                    # enter_str is a list of two elements
                    # first is what happened in the insert function, the second is the optional error message (empty if no error)
                    # first element can be connection_error, insert_error, insert_success
            #CP - I fixed here a bug in the logic of the testing condition.    
            if not (output_table_size[0]=="connection_error" or output_table_size[0]== "select_error"):
                #print(output_table_size[0])
                enter_str = insert_PlayerSummaries(record, tablename) 
                
                    # 3.1. If connection error or insert_error, write to disk ; handle both the same - I think it's a good idea
                if enter_str[0] == 'connection_error' or enter_str[0] == 'insert_error':
                    try:               
                        write_to_disk(record)
                        message3 = 'db;0;%s;%s;%s when attempting insert: %s, write to disk' % (min(record_steamids),len(record_steamids), enter_str[0], enter_str[1])
                    except Exception, e:
                        message3 = 'db;0;%s;%s;%s when attempting insert: %s, NOT write to disk because: %s' % (min(record_steamids),len(record_steamids), enter_str[0], enter_str[1], str(e))
                    finally:
                        send_email(message3)
                        print(message3)
                        logger.log(LEVEL, message3)
                  
                    # 3.2 If however insert success, good for us, proceed
                elif enter_str[0] == 'insert_success' :
                    #print('8')
                    message3 = 'db;1;%s;%s;wrote ids to %s' % (min(record_steamids),len(record_steamids), tablename) #Keren it makes sense now to also log the table name
                    logger.log(LEVEL, message3)
            
        print('db_worker finished: %s' % name)
        
    except Exception,e:
        email_message = 'db_worker got an exception while trying to insert the follwing id:%s : %s' % (record_steamids, str(e))
        send_email(email_message)
        print(email_message)
    
def main():
    try:    
        # Create an error table & temporary table
        if not check_if_table_exists(config.schemaname, config.errortable_name):
            createtable(config.schemaname, config.errortable_name, False)  
        if not check_if_table_exists(config.schemaname, config.temporaryTableName):
            createtable(config.schemaname, config.temporaryTableName, True)
    except mysql.connector.Error as e:
        print(str(e))
        
    # Logger Queue and listener worker
    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process,args=(queue, listener_configurer))
    listener.start()
    print("Started Listener")
    
    # Results Queue and db_worker
    Results_Queue = multiprocessing.Queue(-1)
    db_process = multiprocessing.Process(target=db_worker, args=(queue, worker_configurer, Results_Queue))
    db_process.start()
    print("Started Db_worker") 
        
    # Input Queue and populate queue
    idsChunks_Queue = multiprocessing.Queue(-1)
    
    Populate_QueueChunks(idsChunks_Queue,config.firstid,config.lastid, config.chunksize) 
#    Populate_QueueChunks_random(idsChunks_Queue, config.chunksize) # this is the random one, for debugging & testing purposes
    
    # First log message 
    worker_configurer(queue)
    logger = logging.getLogger(LOGGER)
    beginMsg = 'Started grabbing; %s chunks; with first id %s' % (idsChunks_Queue.qsize(),config.firstid)
    logger.log(LEVEL,beginMsg)
    
    # Start grabber workers
    workers = []
    for i in range(config.n_workers): # profile this
        worker = multiprocessing.Process(target=worker_process,
                                       args=(queue, worker_configurer, idsChunks_Queue, Results_Queue))
        print("I am created: "+str(worker.name))        
        workers.append(worker)
        worker.start()
        idsChunks_Queue.put('STOP') #Keren: is this OK now?
    print("Started Grabbers")
    
    for w in workers:
        print("I am about to be joined: "+str(w.name))
        w.join()
        print('I am joined: '+str(w.name))
   
    # Finally
    Results_Queue.put_nowait(None) #Keren: and this?
    db_process.join()
    print('db_worker is done')
 
    # Last log message
    endMsg = 'Finished everything; %s left over chunks; %s left over db_chunks' % (idsChunks_Queue.qsize(),Results_Queue.qsize())
    logger.log(LEVEL, endMsg) 
    
    queue.put_nowait(None)
    listener.join()
    print('logger is done')

##==============================================================================
if __name__ == '__main__':
    send_email('The scraper started!')
    t0 = time.time()
    main()
    t1 = time.time()
    print(t1 - t0)
    send_email('The scraper finished!')
##==============================================================================