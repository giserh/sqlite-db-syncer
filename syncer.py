import shutil, sqlite3, os, logging, ConfigParser, datetime, time

SEPERATOR = "\\"
BACKUP_SEPERATOR = "~"
TIME_FORMAT = "%Y%m%d%H%M%S"
TIMES_SECTION = "TIMES"

class Synchronizer():
    
    def __init__(self, config_file):
        self.cp = ConfigParser.ConfigParser()
        self.cp.read(config_file)

        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                            level=logging.DEBUG,
                            datefmt='%Y-%m-%d %H:%M:%S',
                            filename=self.cp.get("CONFIG", "logfile_name"))
        self.logger = logging.getLogger("Synchronizer")

        self.logger.info("======================================================")
        self.logger.info("========== STARTING NEW SYNCHRONIZATION RUN ==========")
        self.logger.info("======================================================")

        self.timestamps_file = self.cp.get("CONFIG", "timestamp_file")
        self.logger.debug("Timestamps file: %s", self.timestamps_file)

        self.historyCp = ConfigParser.ConfigParser()
        self.historyCp.read(self.timestamps_file)
        
        self.dbs_dict = {}
        for item in self.cp.items("DB_LOCATIONS"):
            self.dbs_dict[item[0].upper() + ".db"] = item[1]
        self.logger.debug("DBs to sync:\n" 
                          + "\n".join(['\t%s: %s' % (key, value) for (key, value) in self.dbs_dict.items()]))
        
        self.local_db_path = os.path.abspath(self.cp.get("CONFIG", "local_db_path"))
        self.logger.debug("Local DB path: " + self.local_db_path)
        
        self.backup_path = os.path.abspath(self.cp.get("CONFIG", "backup_folder"))
        self.logger.debug("Backup path: " + self.backup_path)
        
        self.current_time = datetime.datetime.now()
        self.logger.debug("Current time: " + str(self.current_time))
        
        self.timezone_offsets = {}
        for item in self.cp.items("TIMEZONE_OFFSETS"):
            self.timezone_offsets[item[0].upper() + ".db"] = int(item[1])
        self.logger.debug("Timezone offsets:\n" 
                          + "\n".join(['\t%s: %s' % (key, value) for (key, value) in self.timezone_offsets.items()]))
        
        self.db_sync_time = datetime.datetime.strptime(self.cp.get("CONFIG", "db_sync_time"), "%H:%M:%S")
        self.db_sync_time = datetime.datetime.now().replace(hour=self.db_sync_time.hour,
                                                            minute=self.db_sync_time.minute,
                                                            second=self.db_sync_time.second)
        self.logger.debug("DB sync time: %s", self.db_sync_time)
        
        self.time_diff = int(self.cp.get("CONFIG", "time_diff"))
        self.logger.debug("Time threshold: %s", self.time_diff)
        
        self.errors = []
        self.backup_requests = []

    def finish(self):
        status = ""
        if len(self.errors) > 0:
            self.logger.info("\n\n********** ERRORS OCCURRED *********\n" 
                             + "\n".join(self.errors) 
                             + "\n********** END OF ERRORS *********\n")
            status = "WITH ERRORS"
        else:
            self.logger.info("No errors")
            status = "SUCCESSFULLY"
            
        self._remove_temp_dbs()
        self.logger.info("========== SYNCHRONIZATION COMPLETED %s ==========", status)

    # TODO: remove
    def run_demo(self):
        self.logger.debug("Running demo...")
        for name in self.dbs_dict.keys():
            conn = sqlite3.connect(database=self.dbs_dict[name])
            cur = conn.cursor()
            demo_file = open('dbscripts/' + name + ".sql", 'r')
            sql = demo_file.read()
            cur.executescript(sql)
            conn.close()
    
    def copy_dbs_from_remote_locations(self):
        self.logger.info("Copying files from remote locations to local machine...")
        
        backup_ok = False
        create_backup = True
        
        for name in self.dbs_dict.keys():
            threshold_minutes = self._get_threshold_minutes(name)
            if threshold_minutes > self.time_diff:
                self.logger.debug("Time for %s is still %s minutes outside of threshold.", name, threshold_minutes)
                continue
            
            # check if db exists
            if not os.path.isfile(self.dbs_dict[name]):
                self.logger.error("Database " + name + " does not exist!")
                self.errors.append("Database " + name + " was not retrieved from " + self.dbs_dict[name] 
                                   + ". The file could not be found. Check path?")
                continue
            
            # check if timestamp has changed
            current_timestamp = str(os.path.getmtime(self.dbs_dict[name]))
            last_timestamp = ""
            try:
                last_timestamp = str(self.historyCp.get(TIMES_SECTION, name))
            except ConfigParser.NoSectionError:
                self.logger.debug("Section %s not found, adding it...", TIMES_SECTION)
                self.historyCp.add_section(TIMES_SECTION)
            except ConfigParser.NoOptionError:
                self.logger.debug("Option %s not found, adding it with current timestamp.", name)
                self.historyCp.set(TIMES_SECTION, name, current_timestamp)
            
            # compare with current
            if last_timestamp != current_timestamp:
                self.logger.info("%s has changed. Proceeding with synchronization...", name)
                self.historyCp.set(TIMES_SECTION, name, current_timestamp)
            else:
                self.logger.debug("No changes to %s. Ommitting synchronization.", name)
                continue
            
            with open(self.timestamps_file, "wb") as timestamp_cfg:
                self.historyCp.write(timestamp_cfg)
            
            # acquire lock for file
            conn = sqlite3.connect(database=self.dbs_dict[name], isolation_level="EXCLUSIVE")
            if not conn:
                self.logger.critical("Database connection to " + name + " could not be acquired!")
                self.errors.append("Database lock for " + name + " could not be acquired. Skipped this DB.")
                continue

            # check db consistency
            try:
                cur = conn.cursor()
                cur.execute("select * from sqlite_master")
            except sqlite3.DatabaseError, e:
                # consistency check failed, resort to backup
                create_backup = False
                self.logger.error("Error opening database: %s - %s", name, str(e))
                self.errors.append("Consistency check for database " + name + " failed: " + str(e))
                
                # get last backup and write log
                self.logger.info("Requesting last backup of %s", name)
                backup_ok = self._request_backup(name)
                if not backup_ok:
                    continue
                
            self.logger.debug("Getting " + name)
                
            # copy
            local_name = self.local_db_path + SEPERATOR + name
            shutil.copyfile(self.dbs_dict[name], local_name)
    
            # release lock for file
            conn.close()
            
            # create backup
            if create_backup:
                backup_dest = self.backup_path + SEPERATOR + name + BACKUP_SEPERATOR + datetime.datetime.now().strftime(TIME_FORMAT)
                shutil.copyfile(local_name, backup_dest)
            
            # check consistency of backup
            c_conn = sqlite3.connect(local_name)
            try:
                c_cur = c_conn.cursor()
                c_cur.execute("select * from sqlite_master")
            except sqlite3.DatabaseError, e:
                self.logger.error("Error opening backup database: %s - %s", local_name, str(e))
                self.errors.append("Consistency check for backup database " + local_name + " failed: " + str(e))
            c_conn.close()
    
    def _get_threshold_minutes(self, name):
        # get time offset for current db
        delta = 0
        if name not in self.timezone_offsets:
            self.logger.debug("No timezone offset set for %s, using default server time as measure.", name)
        else:
            delta = self.timezone_offsets[name]
        
        adapted_time = self.current_time + datetime.timedelta(hours=delta)
        
        # check if it is time to sync this db
        t1 = time.mktime(adapted_time.timetuple())
        t2 = time.mktime(self.db_sync_time.timetuple())
        
        return abs(int(t1 - t2)) / 60
    
    def _request_backup(self, prefix):
        filenames = []
        
        for currentFile in os.listdir(self.backup_path):
            if os.path.isfile(os.path.join(self.backup_path, currentFile)) and currentFile.startswith(prefix):
                filenames.append(currentFile)
    
        if len(filenames) < 1:
            self.logger.error("No backups found for %s", prefix)
            self.errors.append("No backups found for {}. Could not restore database!".format(prefix))
            return False
        
        self.logger.debug("Backup DBs:\n\t" + "\n\t".join(filenames))
        times = {}
    
        for f in filenames:
            timestring = f.split(BACKUP_SEPERATOR)[1]
            timestamp = datetime.datetime.strptime(timestring, TIME_FORMAT)
            times[timestamp] = f

        last_entry_key = sorted(times.keys())[-1]
        self.backup_requests.append(times[last_entry_key])
        return True
    
    def distribute_files(self):
        filenames = []
        
        for currentFile in os.listdir(self.local_db_path):
            if os.path.isfile(os.path.join(self.local_db_path, currentFile)):
                filenames.append(currentFile)
    
        self.logger.debug("Local DBs to distribute:\n\t" + "\n\t".join(filenames))
        
        for path in self.dbs_dict.values():
            loc = os.path.dirname(path)
            self.logger.debug("Distributing to " + loc)
            
            for f in filenames:
                # check if f in backup requests
                backup_list = filter(lambda b: b.startswith(f), self.backup_requests)
                if len(backup_list) > 0: 
                    # copy backup to location
                    self.logger.debug("Copying latest backup " + f + " to " + loc)
                    try:
                        src = self.backup_path + SEPERATOR + backup_list[0]
                        # TODO: insert log message that a backup was restored!
                        self._write_sync_log(src, "Restored latest backup", True)
                        dest = loc + SEPERATOR + f
                        shutil.copyfile(src, dest)
                    except Exception as e:
                        self.logger.error("Error occurred while copying backup of %s to other remote locations.", self.local_db_path + SEPERATOR + f)
                        self.errors.append("Error occurred while distributing backup of {}. {}".format(src, e.strerror))
                    continue
                
                if f not in path and BACKUP_SEPERATOR not in f:
                    self.logger.debug("Copying " + f + " to " + loc)
                    try:
                        src = self.local_db_path + SEPERATOR + f
                        dest = loc + SEPERATOR + f
                        shutil.copyfile(src, dest)
                    except Exception as e:
                        self.logger.error("Error occurred while copying %s to other remote locations. %s", self.local_db_path + SEPERATOR + f, e.strerror)
                        self.errors.append("Error occurred while distributing {}. {}".format(src, e.strerror))

    def _write_sync_log(self, dbfile, message, error=True):
        conn = sqlite3.connect(dbfile)
        cur = conn.cursor()
        
        # create table if not existing
        demo_file = open("dbscripts/synclog.sql", 'r')
        sql = demo_file.read()
        cur.executescript(sql)
        conn.commit()
        
        cur.execute("INSERT INTO sync_log (message, timestamp, source, error) VALUES ("
                    + "'" + message + "',"
                    + "'" + datetime.datetime.now() + "',"
                    + "'" + "Synchronizer Script" + "',"
                    + "'" + error + "'"
                    + ")")
        
        conn.close()

    def _remove_temp_dbs(self):
        self.logger.debug("Deleting local DBs...")
        for currentFile in os.listdir(self.local_db_path):
            fp = os.path.join(self.local_db_path, currentFile)
            if os.path.isfile(fp):
                try:
                    os.unlink(fp)
                except Exception, e:
                    self.errors.append("Could not delete file {}: {}".format(fp, e.strerror))
                    self.logger.error("Could not delete file %s: %s", fp, e.strerror)
        
def main():
    syncer = Synchronizer("conf.ini")
    
    # syncer.run_demo()  # TODO: remove before final!
    
    syncer.copy_dbs_from_remote_locations()
    syncer.distribute_files()
    
    syncer.finish()

if __name__ == '__main__':
    main()
