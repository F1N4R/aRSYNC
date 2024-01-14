import mysql.connector, os, pathlib, sys, hashlib, shutil, time, datetime

#Global VARs
appcd = ''
dbUserName = 'admin'
dbPsw = 'examplePSW'
dbTable = 'aRSYNC'
dbHost = 'localhost'
dbPort = '3306'
speed = 0.05
deldays = 14

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

#DB Insert to PrimaryIndex
def dbInsertPri(fileHash, filePath, secPath, isDir, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO primaryIndex (hash, primaryPath, secondaryPath, isDir, location) VALUES (%s, %s, %s, %s, %s)"
        val = (fileHash, filePath, secPath, isDir, location)
        
        cursor.execute(sql, val)
        cnx.commit()
        
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        print(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB Update Hash for PrimaryIndex
def dbUpdateHashPri(oldFilehash, newFileHash, filePath, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()
        
        sql = "UPDATE primaryIndex SET hash = %s WHERE hash = %s and primaryPath = %s and location = %s"
        val = (newFileHash, oldFilehash, filePath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        print(e)
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
def dbMarkDeleted(hash, primaryPath, secondaryPath, isDir, location) -> tuple:
    date = datetime.date.today()
    date = date + datetime.timedelta(days=deldays)
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO deletedIndex (deleteAt, hash, primaryPath, secondaryPath, isDir, location, forceDelete) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (date, hash, primaryPath, secondaryPath, isDir, location, 0)

        cursor.execute(sql, val)
        cnx.commit()
        if not cursor.rowcount == 1: return ('ERROR', 'dbCommitError')

        sql = "DELETE FROM primaryIndex WHERE hash = %s and primaryPath = %s and location = %s"
        val = (hash, primaryPath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')

    except Exception as e:
        print(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB create Entry in DeletedIndex and delete Entry in primaryIndex
def dbInsertDel(hash, primaryPath, secondaryPath, isDir, location) -> tuple:
    date = datetime.date.today()
    date = date + datetime.timedelta(days=deldays)
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "INSERT INTO deletedIndex (deleteAt, hash, primaryPath, secondaryPath, isDir, location, forceDelete) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (date, hash, primaryPath, secondaryPath, isDir, location, 0)

        cursor.execute(sql, val)
        cnx.commit()
        if not cursor.rowcount == 1: return ('ERROR', 'dbCommitError')
        else: return ('OK', 'dbCommitOK')
    except Exception as e:
        print(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#DB delete entry in deletedIndex
def dbDelEntryDel(hash, primaryPath, location) -> tuple:
    try:
        cnx.connect()
        cursor = cnx.cursor()

        sql = "DELETE FROM deletedIndex WHERE hash = %s and primaryPath = %s and location = %s"
        val = (hash, primaryPath, location)

        cursor.execute(sql, val)
        cnx.commit()
        if cursor.rowcount == 1: return ('OK', 'dbCommitOK')
        else: return ('ERROR', 'dbCommitError')
    except Exception as e:
        print(e)
        return ('ERROR', 'ExceptionThrown')
    finally:
        cursor.close()
        cnx.close()

#Delete on Secondary Drive
def delSec(path, isDir) -> tuple:
    if isDir == 1:
        try:
            shutil.rmtree(path)
            if pathlib.Path(path).exists(): return ('ERROR', 'fileStillExists')
            else: return ('OK', 'fileDeleted')
        except Exception as e:
            print(e)
            return ('ERROR', 'ExceptionThrown')
    else:
        try:
            pathlib.Path(path).unlink()
            if pathlib.Path(path).exists(): return ('ERROR', 'fileStillExists')
            else: return ('OK', 'fileDeleted')
        except Exception as e:
            print(e)
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

    lines = ""
    with open(appcd + "/path.conf") as txt:
        lines = txt.read().splitlines()

        for l in lines:
            p = l.split(">")

            if os.path.exists(p[0]) and os.path.exists(p[1]):
                #DB Select primaryIndex
                sql = "SELECT hash, primaryPath, secondaryPath, isDir FROM `primaryIndex` WHERE location = %s"
                adr = (p[0],)

                cursor.execute(sql, adr)
                rSelect = cursor.fetchall()
        
                cursor.close()
                cnx.close()

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
                    if not pathlib.Path(file).is_dir():
                        query = list(filter(lambda x:str(file) in x, rSelect))
                        #New File found: CP File and create new Entry in DB
                        if len(query) == 0:
                            notindb +=1
                            if not pathlib.Path(str(file).replace(p[0], p[1])).exists():
                                r = cpSec(file, str(file).replace(p[0], p[1]))
                                if r[0] == 'ERROR': 
                                    print('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                                    error +=1
                                else:
                                    x = dbInsertPri(hash3(file), str(file), str(file).replace(p[0], p[1]), 0, str(p[0]))
                                    if x[0] == 'OK': print('OK: New File found: {0}'.format(file))
                                    else: 
                                        print('ERROR: New File found: {0}\n{1}'.format(file, x[1]))
                                        error +=1
                            else:
                                x = dbInsertPri(hash3(file), str(file), str(file).replace(p[0], p[1]), 0, str(p[0]))
                                if x[0] == 'OK': print('OK: New File found: {0}'.format(file))
                                else: 
                                    print('ERROR: New File found: {0}\n{1}'.format(file,x[1]))
                                    error += 1
                        #File Hash has Changed: Update File Hash in DB and CP changed File to Secondary Drive
                        elif not hash3(file) == query[0][0]:
                            indbchanged += 1
                            x = dbUpdateHashPri(query[0][0], hash3(file), str(file), str(p[0]))
                            if x[0] == 'ERROR':
                                print('ERROR: DB Hash Update failed: {0}\n{1}'.format(file, x[1]))
                                error +=1
                            r = cpSec(file, str(file).replace(p[0], p[1]))
                            if r[0] == 'ERROR': 
                                print('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                                error +=1
                            else: print('OK: Hash has changed: {0}'.format(file))
                        #File is already Present in DB and CP File if not Exists on Secondary Drive
                        else: 
                            if not pathlib.Path(str(file).replace(p[0], p[1])).exists():
                                r = cpSec(file, str(file).replace(p[0], p[1]))
                                if r[0] == 'ERROR': 
                                    print('Copy Error: {0} for File: {1}\n{2}'.format(r[1], file, r[1]))
                                    error +=1
                            indb +=1
                    #Only Folders
                    else:
                        query = list(filter(lambda x:str(file) in x, rSelect))
                        #New Folder Found: create Folder on Secondary Drive if not exists and create Entry in DB
                        if len(query) == 0:
                            if not pathlib.Path(str(file).replace(p[0], p[1])).exists(): os.makedirs(str(file).replace(p[0], p[1]))
                            x = dbInsertPri(str().zfill(64), str(file), str(file).replace(p[0], p[1]), 1, str(p[0]))
                            if x[0] == 'OK': print('OK: New Folder found: {0}'.format(file))
                            else: print('ERROR: New Folder found: {0}\n{1}'.format(file, x[1]))
                            notindb +=1
                        #Folder is already Present in DB: create Folder if not exists on Secondary Drive
                        else:
                            if not pathlib.Path(str(file).replace(p[0], p[1])).exists(): os.makedirs(str(file).replace(p[0], p[1]))
                            indb +=1
                    time.sleep(speed)

                #DB Select for deletedIndex
                cnx.connect()
                cursor = cnx.cursor()
                sql = "SELECT hash, primaryPath, secondaryPath, isDir, deleteAt, forceDelete FROM `deletedIndex` WHERE location = %s"
                adr = (p[0],)

                cursor.execute(sql, adr)
                delSelect = cursor.fetchall()

                cursor.close()
                cnx.close()

                #For Loop primaryIndex DB with LocalIndex to find files that Deleted on Primary Drive
                for dbEntry in rSelect:
                    query = list(filter(lambda x:str(dbEntry) in x, delSelect))
                    if not pathlib.Path(dbEntry[1]).exists():
                        if len(query) == 0:
                            x = dbMarkDeleted(hash=dbEntry[0], primaryPath=dbEntry[1], secondaryPath=dbEntry[2], isDir=dbEntry[3], location=p[0])
                            if x[0] == 'OK': print('OK: Deleted File/Folder found: {0}'.format(dbEntry[1]))
                            else: 
                                print('ERROR: Deleted File/Folder found: {0}\n{1}'.format(dbEntry[1], x[1]))
                            markedAsDeleted += 1

                #For Loop DeleteIndex DB to Check DeleteAt Date and if Files are Still Present
                for dbDelEntry in delSelect:
                    if not pathlib.Path(dbDelEntry[2]).exists():
                        x = dbDelEntryDel(dbDelEntry[0], dbDelEntry[1], p[0])
                        if x == 'OK': print('OK: Entry Deleted in DB: {0}'.format(dbDelEntry[1]))
                        else: print('ERROR: Entry Deleted in DB: {0}\n{1}'.format(dbDelEntry[1], x[1]))

                    date = datetime.date.today()

                    #Check if date is past or forceDelete is True
                    if date >= dbDelEntry[4] or dbDelEntry[5] == 1:
                        r = delSec(dbDelEntry[2], dbDelEntry[3])
                        print((dbDelEntry[2], dbDelEntry[3]))
                        if r[0] == 'ERROR': 
                            print('ERROR: File not on Secondary deleted: {0}\n{1}'.format(dbDelEntry[2], r[1]))
                            error +=1
                        else:
                            x = dbDelEntryDel(dbDelEntry[0], dbDelEntry[1], p[0])
                            if x[0] == 'ERROR': 
                                print('ERROR: File not deleted on DB: {0}\n{1}'.format(dbDelEntry[2], x[1]))
                                error +=1
                            else:
                                print('OK: File on Secondary and DB deleted: {0}'.format(dbDelEntry[2]))
                                deleted +=1
                #For Loop to Compare Secondary Drive with primaryIndex / If files are Found that not in DB these files will be deleted in delayed Days, too
                path = pathlib.Path(p[1])
                files = path.rglob('*')
                for file in files:
                    if not pathlib.Path(file).is_dir():
                        query = list(filter(lambda x:str(file) in x, rSelect))
                        if len(query) == 0:
                            cleanup +=1
                            x = dbInsertDel(hash3(file), str(file).replace(p[1], p[0]), str(file), 0, p[0])
                            if x[0] == 'OK': print('OK: UnIndexed File Found in Secondary Drive: {0}'.format(file))
                            else: 
                                print('ERROR: UnIndexed File Found in Secondary Drive: {0}\n{1}'.format(file, x[1]))
                                error +=1
                    else:
                        query = list(filter(lambda x:str(file) in x, rSelect))
                        if len(query) == 0:
                            cleanup +=1
                            x = dbInsertDel(str().zfill(64), str(file).replace(p[1], p[0]), str(file), 1, p[0])
                            if x[0] == 'OK': print('OK: UnIndexed File Found in Secondary Drive: {0}'.format(file))
                            else: 
                                print('ERROR: UnIndexed File Found in Secondary Drive: {0}\n{1}'.format(file, x[1]))
                                error +=1


                #Results
                print('------------------------------',
                      '\nResults for {0}'.format(p[0]),
                      '\nIn DB found: {0}'.format(indb), 
                      '\nIn DB found but Hash has changed: {0}'.format(indbchanged), 
                      '\nNot Found in DB: {0}'.format(notindb),
                      '\nMarked as Deleted: {0}'.format(markedAsDeleted),
                      '\nDeleted: {0}'.format(deleted),
                      '\nCleanUp: {0}'.format(cleanup),
                      '\nERRORs: {0}'.format(error),
                      '\nExcution Time: {0} secounds'.format(round(time.time() - startTime, 2)),
                      '\n------------------------------')
