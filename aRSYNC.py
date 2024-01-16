#! /usr/bin/python

import mysql.connector, os, pathlib, sys, hashlib, shutil, time, datetime

#Global VARs
appcd = ''
dbUserName = 'admin'
dbPsw = 'examplePSW'
dbTable = 'aRSYNC'
dbHost = 'localhost'
dbPort = '40000'
speed = 0.01
deldays = 14
output = True

#My Stupid Method to Find where App is executed (to find config files and Co)
if getattr(sys, 'frozen', False):
    appcd = os.path.dirname(sys.executable)
elif __file__:
    appcd = os.path.dirname(__file__)
else:
    appcd = str(pathlib.Path(__file__).parent.resolve())

if str(os.getcwd()) != appcd:
    os.chdir(appcd)

#If Config File not exists App will exit
if os.path.isfile(appcd + "/path.conf") == False:
    exit()

#hash file in 64kb chunks to sha3
def hash3(path:str) -> str:
    BUF_SIZE = 65536  #64kb chunks

    sha3 = hashlib.sha3_256()

    with open(path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha3.update(data)
    
    return sha3.hexdigest()

def cprint(t:str) -> None:
    if output: print(t)
    try:
        with open('output.log', "a", encoding='utf8', newline='\n') as f:
            try:
                f.write(t + '\n')
                f.close()
            except (IOError, OSError) as e:
                print('ERROR while writing to Logfile')
    except (FileNotFoundError, PermissionError, OSError) as e:
        print('ERROR while opening Logfile')

#DB Insert to PrimaryIndex
def dbInsertPri(filePath, secPath, modified, size, isDir, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO primaryIndex (primaryPath, secondaryPath, modified, size, isDir, location) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (filePath, secPath, modified, size, isDir, location)
        
        cursor.execute(sql, val)
        cnx.commit()
        
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        cprint(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB Update Hash for PrimaryIndex
def dbUpdateHashPri(modified, size, filePath, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()
        
        sql = "UPDATE primaryIndex SET modified = %s, size = %s WHERE primaryPath = %s and location = %s"
        val = (modified, size, filePath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        cprint(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB CP File to Secondary Drive 
def cpSec(file:str, fileDest:str) -> tuple:
    sourceFile = pathlib.Path(file)
    destFile = pathlib.Path(fileDest)
    if not sourceFile.exists(): return ('ERROR', 'fileNotExistsInPri', 'None')
    if destFile.exists(): destFile.unlink()
    try:
        os.makedirs(os.path.dirname(destFile), exist_ok=True)
        shutil.copy2(sourceFile, destFile)
        return ('OK', 'CopyOK',)
    except IOError as e:
        return ('ERROR', 'fileCopyError', e)

#DB create Entry in DeletedIndex and delete Entry in primaryIndex
def dbMarkDeleted(primaryPath, secondaryPath, isDir, location) -> tuple:
    date = datetime.date.today()
    date = date + datetime.timedelta(days=deldays)
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO deletedIndex (deleteAt, primaryPath, secondaryPath, location, isDir, forceDelete) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (date, primaryPath, secondaryPath, location, isDir, 0)

        cursor.execute(sql, val)
        cnx.commit()
        if not cursor.rowcount == 1: return ('ERROR', 'dbCommitError')

        sql = "DELETE FROM primaryIndex WHERE primaryPath = %s and location = %s"
        val = (primaryPath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')

    except Exception as e:
        cprint(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB create Entry in DeletedIndex and delete Entry in primaryIndex
def dbInsertDel(primaryPath, secondaryPath, isDir, location) -> tuple:
    date = datetime.date.today()
    date = date + datetime.timedelta(days=deldays)
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO deletedIndex (deleteAt, primaryPath, secondaryPath, location, isDir, forceDelete) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (date, primaryPath, secondaryPath, location, isDir, 0)

        cursor.execute(sql, val)
        cnx.commit()
        if not cursor.rowcount == 1: return ('ERROR', 'dbCommitError')
        else: return ('OK', 'dbCommitOK')
    except Exception as e:
        cprint(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB delete entry in deletedIndex
def dbDelEntryDel(primaryPath, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "DELETE FROM deletedIndex WHERE primaryPath = %s and location = %s"
        val = (primaryPath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        cprint(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

def dbSelectPri() -> list:
    try:
        cnx.connect()
        cursor = cnx.cursor()
        sql = "SELECT primaryPath, secondaryPath, modified, size, isDir FROM `primaryIndex` WHERE location = %s"
        adr = (p[0],)

        cursor.execute(sql, adr)
        return cursor.fetchall()
    except Exception as e:
        cprint (e)
        exit()
    finally:
        cursor.close()
        cnx.close()

def dbSelectDel() -> list:
    try:
        cnx.connect()
        cursor = cnx.cursor()
        sql = "SELECT deleteAt, primaryPath, secondaryPath, isDir, forceDelete FROM `deletedIndex` WHERE location = %s"
        adr = (p[0],)

        cursor.execute(sql, adr)
        return cursor.fetchall()
    except Exception as e:
        cprint (e)
        exit()
    finally:
        cursor.close()
        cnx.close()

#Delete on Secondary Drive
def delSec(path, isDir) -> tuple:
    if isDir == 1:
        try:
            shutil.rmtree(path, ignore_errors=True)
            if pathlib.Path(path).exists(): return ('ERROR', 'fileStillExists')
            else: return ('OK', 'fileDeleted')
        except Exception as e:
            cprint(e)
            return ('ERROR', 'ExceptionThrown')
    else:
        try:
            pathlib.Path(path).unlink(missing_ok=True)
            if pathlib.Path(path).exists(): return ('ERROR', 'fileStillExists')
            else: return ('OK', 'fileDeleted')
        except Exception as e:
            cprint(e)
            return ('ERROR', 'ExceptionThrown')

#Check if File is started as Main
if __name__ == '__main__':
    #MySQL Connection established. CNX because in DOCS it was also named CNX and i am bad at choosing names :D
    cnx = mysql.connector.connect(
        host=dbHost,
        port=dbPort,
        user=dbUserName,
        password=dbPsw,
        database=dbTable
    )
    cursor = cnx.cursor()
    cursor.close()
    cnx.close()

    tNotindb = 0
    tIndb = 0
    tIndbchanged = 0
    tMarkedAsDeleted = 0
    tDeleted = 0
    tCleanup = 0
    tError = 0

    lines = ""
    apptime = time.time()
    with open(appcd + "/path.conf") as txt:
        lines = txt.read().splitlines()

        txt.close()

    for arg in sys.argv:
        if arg == 'silent': output = False
        if arg == 'nospeed': speed = 0

    nowdate = datetime.datetime.now()
    cprint('\n\naRSYNC Start at {0}'.format(nowdate.strftime('%H:%M:%S on %d.%m.%Y')))

    for l in lines:
        p = l.split(">")

        if os.path.exists(p[0]) and os.path.exists(p[1]):
            #DB Select primaryIndex
            rSelect = dbSelectPri()

            path = pathlib.Path(p[0])
            files = path.rglob('*')

            notindb = 0
            indb = 0
            indbchanged = 0
            markedAsDeleted = 0
            deleted = 0
            cleanup = 0
            error = 0

            startTime = time.time()

            #For Loop Compare Local Index with primaryIndex DB to find new Files / Deleted Files / Changed Files
            for file in files:
                #Only Files
                lPath = pathlib.Path(file)
                if not lPath.is_dir():
                    query = list(filter(lambda x:str(file) in x, rSelect))
                    #New File found: CP File and create new Entry in DB
                    if len(query) == 0:
                        notindb +=1
                        if not pathlib.Path(str(file).replace(p[0], p[1])).exists():
                            r = cpSec(file, str(file).replace(p[0], p[1]))
                            if r[0] == 'ERROR': 
                                cprint('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                                error +=1
                            else:
                                x = dbInsertPri(filePath=str(file), 
                                                secPath= str(file).replace(p[0], p[1]), 
                                                modified=lPath.stat().st_mtime, 
                                                size=lPath.stat().st_size, 
                                                isDir=0, 
                                                location=str(p[0]))
                                if x[0] == 'OK': cprint('OK: New File found: {0}'.format(file))
                                else: 
                                    cprint('ERROR: New File found: {0}\n{1}'.format(file, x[1]))
                                    error +=1
                        else:
                            x = dbInsertPri(filePath=str(file), 
                                            secPath= str(file).replace(p[0], p[1]), 
                                            modified=lPath.stat().st_mtime, 
                                            size=lPath.stat().st_size,
                                            isDir=0, 
                                            location=str(p[0]))
                            if x[0] == 'OK': cprint('OK: New File found: {0}'.format(file))
                            else: 
                                cprint('ERROR: New File found: {0}\n{1}'.format(file,x[1]))
                                error += 1
                    #File Hash has Changed: Update File Hash in DB and CP changed File to Secondary Drive
                    elif not lPath.stat().st_mtime == query[0][2] or not lPath.stat().st_size == query[0][3]:
                        indbchanged += 1
                        x = dbUpdateHashPri(lPath.stat().st_mtime, lPath.stat().st_size, str(file), str(p[0]))
                        if x[0] == 'ERROR':
                            cprint('ERROR: DB Hash Update failed: {0}\n{1}'.format(file, x[1]))
                            error +=1
                        r = cpSec(file, str(file).replace(p[0], p[1]))
                        if r[0] == 'ERROR': 
                            cprint('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                            error +=1
                        else: cprint('OK: Hash has changed: {0}'.format(file))
                    #File is already Present in DB and CP File if not Exists on Secondary Drive
                    else: 
                        if not pathlib.Path(str(file).replace(p[0], p[1])).exists():
                            r = cpSec(file, str(file).replace(p[0], p[1]))
                            if r[0] == 'ERROR': 
                                cprint('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                                error +=1
                        indb +=1
                #Only Folders
                else:
                    query = list(filter(lambda x:str(file) in x, rSelect))
                    #New Folder Found: create Folder on Secondary Drive if not exists and create Entry in DB
                    if len(query) == 0:
                        if not pathlib.Path(str(file).replace(p[0], p[1])).exists(): os.makedirs(str(file).replace(p[0], p[1]))
                        x = dbInsertPri(filePath=str(file), 
                                        secPath=str(file).replace(p[0], p[1]), 
                                        modified=0, 
                                        size=0, 
                                        isDir=1, 
                                        location=str(p[0]))
                        if x[0] == 'OK': cprint('OK: New Folder found: {0}'.format(file))
                        else: cprint('ERROR: New Folder found: {0}\n{1}'.format(file, x[1]))
                        notindb +=1
                    #Folder is already Present in DB: create Folder if not exists on Secondary Drive
                    else:
                        if not pathlib.Path(str(file).replace(p[0], p[1])).exists(): os.makedirs(str(file).replace(p[0], p[1]))
                        indb +=1
                time.sleep(speed)

            #DB Select for deletedIndex
            delSelect = dbSelectDel()

            #For Loop primaryIndex DB with LocalIndex to find files that Deleted on Primary Drive
            #Index DBEntry // primaryPath, secondaryPath, modified, size, isDir
            for dbEntry in rSelect:
                query = list(filter(lambda x:str(dbEntry) in x, delSelect))
                if not pathlib.Path(dbEntry[1]).exists():
                    if len(query) == 0:
                        x = dbMarkDeleted(dbEntry[0], dbEntry[1], dbEntry[2], p[0])
                        if x[0] == 'OK': cprint('OK: Deleted File/Folder found: {0}'.format(dbEntry[0]))
                        else: 
                            cprint('ERROR: Deleted File/Folder found: {0}\n{1}'.format(dbEntry[0], x[1]))
                        markedAsDeleted += 1

            #For Loop DeleteIndex DB to Check DeleteAt Date and if Files are Still Present
            date = datetime.date.today()
            for dbDelEntry in delSelect:
                if not pathlib.Path(dbDelEntry[2]).exists():
                    x = dbDelEntryDel(dbDelEntry[1], p[0])
                    if x[0] == 'OK': cprint('OK: Entry Deleted in DB: {0}'.format(dbDelEntry[1]))
                    else: cprint('ERROR: Entry Deleted in DB: {0}\n{1}'.format(dbDelEntry[1], x[1]))
                #Check if date is past or forceDelete is True
                elif date >= dbDelEntry[0] or dbDelEntry[4] == 1:
                    r = delSec(dbDelEntry[2], dbDelEntry[3])
                    if r[0] == 'ERROR': 
                        cprint('ERROR: File not on Secondary deleted: {0}\n{1}'.format(dbDelEntry[1], r[1]))
                        error +=1
                    else:
                        x = dbDelEntryDel(dbDelEntry[1], p[0])
                        if x[0] == 'ERROR': 
                            cprint('ERROR: File not deleted on DB: {0}\n{1}'.format(dbDelEntry[1], x[1]))
                            error +=1
                        else:
                            cprint('OK: File on Secondary and DB deleted: {0}'.format(dbDelEntry[1]))
                            deleted +=1
            #For Loop to Compare Secondary Drive with primaryIndex / If files are Found that not in DB these files will be deleted in delayed Days, too
            path = pathlib.Path(p[1])
            files = path.rglob('*')
            delSelect = dbSelectDel()
            rSelect = dbSelectPri()
            for file in files:
                if not pathlib.Path(file).is_dir():
                    query = list(filter(lambda x:str(file) in x, rSelect))
                    delquery = list(filter(lambda x:str(file) in x, delSelect))
                    if len(query) == 0 and len(delquery) == 0:
                        cleanup +=1
                        x = dbInsertDel(str(file).replace(p[1], p[0]), str(file), 0, p[0])
                        if x[0] == 'OK': cprint('OK: UnIndexed File Found in Secondary Drive: {0}'.format(file))
                        else: 
                            cprint('ERROR: UnIndexed File Found in Secondary Drive: {0}\n{1}'.format(file, x[1]))
                            error +=1
                else:
                    query = list(filter(lambda x:str(file) in x, rSelect))
                    delquery = list(filter(lambda x:str(file) in x, delSelect))
                    if len(query) == 0 and len(delquery) == 0:
                        cleanup +=1
                        x = dbInsertDel(str(file).replace(p[1], p[0]), str(file), 1, p[0])
                        if x[0] == 'OK': cprint('OK: UnIndexed File Found in Secondary Drive: {0}'.format(file))
                        else: 
                            cprint('ERROR: UnIndexed File Found in Secondary Drive: {0}\n{1}'.format(file, x[1]))
                            error +=1


            #Results
            runtime = ''
            if (time.time() - startTime) > 60:
                minutes = (time.time() - startTime) / 60
                seconds = (time.time() - startTime) % 60
                runtime = '{0}:{1}'.format(round(minutes),round(seconds))
            else: runtime = '0:' + str(round(time.time() - startTime))

            cprint('------------------------------')
            cprint('Results for {0}:'.format(p[0]))
            cprint('In DB found: {0}'.format(indb)) 
            cprint('In DB found but File has changed: {0}'.format(indbchanged))
            cprint('Not Found in DB: {0}'.format(notindb))
            cprint('Marked as Deleted: {0}'.format(markedAsDeleted))
            cprint('Deleted: {0}'.format(deleted))
            cprint('CleanUp: {0}'.format(cleanup))
            cprint('ERRORs: {0}'.format(error))
            cprint('Excution Time: {0} secounds'.format(runtime))
            cprint('------------------------------')

            tNotindb += notindb
            tIndb += indb
            tIndbchanged += indbchanged
            tMarkedAsDeleted += markedAsDeleted
            tDeleted += deleted
            tCleanup += cleanup
            tError += error
    
    runtime = ''
    if (time.time() - apptime) > 60:
        minutes = (time.time() - apptime) / 60
        seconds = (time.time() - apptime) % 60
        runtime = '{0}:{1}'.format(round(minutes),round(seconds))
    else: runtime = '0:' + str(round(time.time() - apptime))

    cprint('------------------------------')
    cprint('Results for {0}:'.format('Total Execution'))
    cprint('In DB found: {0}'.format(tIndb)) 
    cprint('In DB found but File has changed: {0}'.format(tIndbchanged))
    cprint('Not Found in DB: {0}'.format(tNotindb))
    cprint('Marked as Deleted: {0}'.format(tMarkedAsDeleted))
    cprint('Deleted: {0}'.format(tDeleted))
    cprint('CleanUp: {0}'.format(tCleanup))
    cprint('ERRORs: {0}'.format(tError))
    cprint('Total Excution Time: {0} Minutes'.format(runtime))
    cprint('------------------------------')
