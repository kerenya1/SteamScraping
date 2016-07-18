# Config file for Steam scraper
#==============================================================================
# API AND REQUEST SETTINGS
#==============================================================================
# API key
key1 =  "9F1BD1CEE2221DECD7E01D5015407D23" # my main private
key2 =  "B369103FB470FAFD558A5CF5AF84AB33" # my alt private account
key3 =  "D078A0DFBBC3A7E0A2E56DF965435481" # my tiu account: cpieterstiu / c.pieters@tilburguniversity.edu
key4 =  "C4644314779E2EEA5D0EE4851638E218" # kerenyaatgmaildotcom
key5 =  "5561C71C67F8801919142A0ADB2EABA4" # kerenyaatmaildothujidotacdotil
key6 =  "E0F918896AA327B7BD134B8429DA1C51" # cpieters1 / steamscraping@gmail.com
key7 =  "744BF9396FA0600FD9E2CABC10B4FF1B" # cpieters2 / steamscraping2@gmail.com
key8 =  "CFADEC5702F9F97113BA84DCD1A534BD" # cpieterstiu2 / constantpieters4@gmail.com
key9 =  "C37A58AEAD6D3636ACDCBB21A5A11C39" # cpieterstiu3 / cpieterstiu@gmail.com
key10 = "6780758071EE2396162D4421A45AD453" # cpieterstiu5 / cpieterstiu@outlook.com
key11 = "80816F9EF643E9A78DED7DC31713A577" # cpieterstiu6 / cpieterstiu1@outlook.com
key12 = "DA21CC1980E8048E2D03E70E605B7C9E" # cpieterstiu8 / cpieterstiu@hotmail.com
key13 = "2B0D6AE2AF7F849F6E2586250F40ABDE" # itleib / itzhaks work email.
key14 = "44CF5D22CA2A537A562E1302876F14B2" #itleib1 / itzhak gmail
key15 = "D112C2B46F4DCB7635CC936968C0810E" #bathad1 / batels work
key16 = "7CD4FF816F62E342E4E186D456B54D3F" #/bathad2 / batels private


# The keys that the grabber will randomize over
keylist = [key1,key2,key3,key4,key5,key6,key7,key8,key9,key10,key11,key12,key13,key14,key15,key16]
#
# API urls
getplayersummariesurl = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/" 
getfriendlisturl = "http://api.steampowered.com/ISteamUser/GetFriendList/v0001/"
getownedgamesurl = "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/" 

# The number of attempts the grabber does in case of 5XX status codes before moving on
number_of_attempts = 1 #3
# How many seconds it sleeps when 5XX status code before trying again
sleep_time = 1 #5

# User agent and contact information
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.97 Safari/537.36',
    'From': 'c.pieters@tilburguniversity.edu'
}

#==============================================================================
# SQL SETTINGS
#==============================================================================

# The name of the schema we want to insert
schemaname = "test_steam"

# SQL connection options
sqlconnectionoptions = {             
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
#    'database': 'steam_db',
    'database': schemaname,
    'raise_on_warnings': True
#    'get_warnings': True
}  

# The table prefix we want to use (tables will be named prefix1, prefix2 ...)
tableprefix = "steam_playersummaries"
tableprefix_friendlist = "steam_friendlist"
tableprefix_ownedgames = "steam_ownedgames"
steamidstable = "playersummaries_focal"
steamidschema = "steam_db"
# Maximum size of a table in MBs (before making a new one)
max_table_size = 2000
# Name of the error table
errortable_name = 'errortable'
# Name of the temporary table
temporaryTableName = "steam_temporaryplayersummaries"

#==============================================================================
# WORKER SETTINGS
#==============================================================================
# How long (in seconds) should the worker sleep when he gets a 429?
sleeptime429 = 1800
sleeptime_exception = 10 #60

#==============================================================================
# LOGGER & ERROR SETTINGS
#==============================================================================
# Path of the log file
log_file = 'C:/Users/s685317/Desktop/Logs/steamlog.log'

# Size of the logfile (in bytes)
logsize_bytes = 512000
log_backupcount = 5

# Path of the error file
errorfile = 'C:/Users/s685317/Desktop/Logs/errors.txt'

#==============================================================================
# MAIN FUNCTION SETTINGS
#==============================================================================
# How many IDs we want to scrape in this batch? 
batchsize = 30000000 # 5000000
batchinsertsize = 100
Friends_table_columns = "steamid bigint(17),steamid_friend bigint(17),friend_since int(10)"
Friends_table_primary_key="steamid, steamid_friend"
Games_table_columns = "steamid bigint(17),appid mediumint(6),playtime_2weeks mediumint(7),playtime_forever mediumint(7)"
Games_table_primary_key="steamid, appid"

firstid = 76561198044265731 #76561198014265730 #76561197984265729 #76561197979265729 #76561197974265729 # 76561197969265729 # 76561197964265729 # 76561197962265729 # 76561197961515729 # 76561197960265729   # Firstid in this batch

lastid = firstid + batchsize           # Lastid in this batch

firstid_ever_chunks = 76561197960265700
firstid_ever = 76561197960265729       # Firsteverid (lower range)
lastid_ever = 76561198280824421        # Lastever id ; think about this !!!!        
# the last id we fetched is 76561198285228226

# How many API workers we want to call in parallel (+1 logging +1/2 db_worker)
n_workers = 16

# Chunk size
chunksize = 100

#==============================================================================
# SMTP settings
#==============================================================================
server = 'smtp.gmail.com'
port = 587
username = "steamscraping@gmail.com"
password = "sababa1234" 
recipient = "steamscraping@gmail.com"