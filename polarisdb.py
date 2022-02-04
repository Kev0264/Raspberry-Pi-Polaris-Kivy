from datetime import datetime
from os import sync
import mysql.connector
from mysql.connector.cursor import MySQLCursor
from enum import IntEnum

class PolarisTagType(IntEnum):
    UNKNOWN = -1
    BOOLEAN = 0
    INTEGER = 1
    FLOAT = 2
    STRING = 3

class PolarisLogType(IntEnum):
    STATUS = 0
    ERROR = 1

class PolarisDb():

    def __init__(self):

        self.db = mysql.connector.connect(
            host="localhost",
            user="polarisuser",
            password="SBn9isCT!5WPjL4f8a6F",
            database="polaris"
        )

        
    def createTables(self):

        cur = self.db.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `tag_data` (
                `id` int NOT NULL AUTO_INCREMENT,
                `tag_id` int NOT NULL,
                `product_id` int NOT NULL,
                `downtime_reason_id` int DEFAULT NULL,
                `float_value` decimal(6,3) DEFAULT '0.000',
                `int_value` int NOT NULL DEFAULT '0',
                `string_value` varchar(45) DEFAULT NULL,
                `sync_id` char(36) DEFAULT NULL,
                `needs_resync` tinyint(1) NOT NULL DEFAULT '0',
                `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
                `updated_at` datetime DEFAULT NULL,
                `deleted_at` datetime DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `tags` (
                `id` int NOT NULL AUTO_INCREMENT,
                `name` varchar(45) NOT NULL,
                `description` varchar(255) DEFAULT NULL,
                `is_running_signal` boolean DEFAULT false,
                `type` int DEFAULT '-1',
                `sync_id` char(36) DEFAULT NULL,
                `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` datetime DEFAULT NULL,
                `deleted_at` datetime DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `users` (
                `id` int NOT NULL AUTO_INCREMENT,
                `fname` varchar(45) NOT NULL,
                `lname` varchar(45) NOT NULL,
                `is_device_admin` tinyint(1) NOT NULL DEFAULT '0',
                `is_device_operator` tinyint(1) NOT NULL DEFAULT '0',
                `sync_id` char(36) DEFAULT NULL,
                `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` varchar(45) DEFAULT NULL,
                `deleted_at` varchar(45) DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `products` (
                `id` int NOT NULL AUTO_INCREMENT,
                `name` varchar(255) NOT NULL,
                `product_code` varchar(255) DEFAULT NULL,
                `ideal_cph` decimal(6,3) DEFAULT '0.000',
                `sync_id` char(36) DEFAULT NULL,
                `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` varchar(45) DEFAULT NULL,
                `deleted_at` varchar(45) DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `downtime_reasons` (
                `id` int NOT NULL AUTO_INCREMENT,
                `name` varchar(255) NOT NULL,
                `is_secondary_for` INT DEFAULT NULL,
                `sync_id` char(36) DEFAULT NULL,
                `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` varchar(45) DEFAULT NULL,
                `deleted_at` varchar(45) DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS `log` (
                `id` int NOT NULL AUTO_INCREMENT,
                `type` int NOT NULL,
                `message` varchar(255) NOT NULL,
                `sync_id` char(36) DEFAULT NULL,
                `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` varchar(45) DEFAULT NULL,
                `deleted_at` varchar(45) DEFAULT NULL,
                PRIMARY KEY (`id`)
            )
        """)

        #self.generateTestData()

    def generateTestData(self):
        cur = self.db.cursor()

        cur.execute("SELECT COUNT(*) FROM products")
        result = cur.fetchone()[0]

        if result == 0:
            self.addProduct('Product 1', 150)
            self.addProduct('Product 2', 200)
            self.addProduct('Product 3', 300)



        cur.execute("SELECT COUNT(*) FROM tags")
        result = cur.fetchone()[0]

        if result == 0:
            self.addTag('Running Status', 'The Running Status', True, PolarisTagType.BOOLEAN)
            self.addTag('Good Count', 'The number of good parts made during a run', False, PolarisTagType.INTEGER)
            self.addTag('Reject Count', 'The number of bad parts made during a run', False, PolarisTagType.INTEGER)

            self.addTagIntData(1, 1, False)


        cur.execute("SELECT COUNT(*) FROM downtime_reasons")
        result = cur.fetchone()[0]

        if result == 0:
            id = self.addPrimaryDowntimeReason('Breakdown')
            self.addSecondaryDowntimeReason('Conveyor Failure', id)
            self.addSecondaryDowntimeReason('Power Failure', id)
            self.addSecondaryDowntimeReason('Reason 3', id)
            self.addSecondaryDowntimeReason('Reason 4', id)

            self.addPrimaryDowntimeReason('Job Change')
            
            self.addPrimaryDowntimeReason('Setup')

            id = self.addPrimaryDowntimeReason('Idle Time')
            self.addSecondaryDowntimeReason('Break', id)
            self.addSecondaryDowntimeReason('Shift Change', id)
            self.addSecondaryDowntimeReason('Planned Downtime', id)

            id = self.addPrimaryDowntimeReason('Minor Stop')
            self.addSecondaryDowntimeReason('Blocked', id)
            self.addSecondaryDowntimeReason('Starved', id)
            self.addSecondaryDowntimeReason('Unplanned Downtime', id)

            self.addPrimaryDowntimeReason('Off Shift')


        cur.execute("SELECT COUNT(*) FROM users")
        result = cur.fetchone()[0]

        if result == 0:
            self.addUser('Francis', 'Pickering', False, True)
            self.addUser('Tansy', 'Spearing', False, True)
            self.addUser('Durward', 'Fosse', False, True)
            self.addUser('Indie', 'Wallace', False, True)
            self.addUser('Belinda', 'Goodman', False, True)
            self.addUser('Tyron', 'Barnett', False, True)
            self.addUser('Luana', 'Sydney', False, True)
            self.addUser('Lottie', 'Proudfoot', False, True)
            self.addUser('Shania', 'Ripley', False, True)
            self.addUser('Lindsey', 'Sherman', False, True)
            self.addUser('Kevin', 'Finkler', False, True)



    def addTag(self, name, description, isRunningSignal, type, sync_id=None):

        cur = self.db.cursor()

        sql = "INSERT INTO tags (name, description, is_running_signal, type, sync_id) VALUES (%s,%s,%s,%s,%s)"
        val = (name, description, isRunningSignal, int(type), sync_id or 'NULL',)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def updateUserBySyncId(self, sync_id, fname, lname, is_device_admin, is_device_operator):
        cur = self.db.cursor()

        sql = "UPDATE users SET fname=%s, lname=%s, is_device_admin=%s, is_device_operator=%s, updated_at=CURRENT_TIMESTAMP WHERE sync_id=%s"
        val = (fname, lname, is_device_admin, is_device_operator, sync_id,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def updateTagById(self, id, name=None, description=None, isRunningSignal=None, type=None, sync_id=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE tags SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if description:
            updateCols.append("description=%s")
            val = val + (description,)
        if isRunningSignal:
            updateCols.append("is_running_signal=%s")
            val = val + (isRunningSignal,)
        if type:
            updateCols.append("type=%s")
            val = val + (type,)
        if sync_id:
            updateCols.append("sync_id=%s")
            val = val + (sync_id,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE id=%s"
        val = val + (id,)

        cur.execute(sql, val)
        self.db.commit()

    def updateTagBySyncId(self, sync_id, name=None, description=None, isRunningSignal=None, type=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE tags SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if description:
            updateCols.append("description=%s")
            val = val + (description,)
        if isRunningSignal:
            updateCols.append("is_running_signal=%s")
            val = val + (isRunningSignal,)
        if type:
            updateCols.append("type=%s")
            val = val + (type,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE sync_id=%s"
        val = val + (sync_id,)

        cur.execute(sql, val)
        self.db.commit()


    def getTags(self):

        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tags ORDER BY name"
        cur.execute(sql)

        results = cur.fetchall()

        return results

    def getTagsBySyncId(self, sync_id):
        cur = self.db.cursor(dictionary=True)
        sql = "SELECT * FROM tags WHERE sync_id=%s"
        val = (sync_id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getTagFromName(self, name):

        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tags WHERE name LIKE %s"
        val = ("%" + name + "%",)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getRunningTag(self):

        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tags WHERE is_running_signal=TRUE"

        cur.execute(sql)

        results = cur.fetchall()

        return results[0] if results else None

    def getTagFromId(self, id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tags WHERE id=%s"
        val = (id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getTagFromSyncId(self, sync_id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tags WHERE sync_id=%s"
        val = (sync_id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def addTagFloatData(self, tagId, productId, floatValue):
        cur = self.db.cursor()

        sql = "INSERT INTO tag_data (tag_id, product_id, float_value) VALUES (%s,%s,%s)"
        val = (tagId, productId, floatValue,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def addTagIntData(self, tagId, productId, intValue):
        cur = self.db.cursor()

        sql = "INSERT INTO tag_data (tag_id, product_id, int_value) VALUES (%s,%s,%s)"
        val = (tagId, productId, intValue,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def addTagStringData(self, tagId, productId, stringValue):
        cur = self.db.cursor()

        sql = "INSERT INTO tag_data (tag_id, product_id, string_value) VALUES (%s,%s,%s)"
        val = (tagId, productId, stringValue,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def getTagDataNotSynced(self, limit=None):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tag_data WHERE sync_id IS NULL OR needs_resync=1"

        if limit is not None:
            sql += " LIMIT %s"
            val = (limit,)
            cur.execute(sql, val)
        else:
            cur.execute(sql)

        results = cur.fetchall()

        return results

    def updateTagDataSyncId(self, id, sync_id):
        cur = self.db.cursor(dictionary=True)

        sql = "UPDATE tag_data SET sync_id=%s, needs_resync=0, updated_at=CURRENT_TIMESTAMP WHERE id=%s"
        val = (sync_id, id,)

        cur.execute(sql, val)

        self.db.commit()

    def updateTagDataNeedsResync(self, id):
        cur = self.db.cursor(dictionary=True)

        sql = "UPDATE tag_data SET needs_resync=0, updated_at=CURRENT_TIMESTAMP WHERE id=%s"
        val = (id,)

        cur.execute(sql, val)

        self.db.commit()

    def getLastTagDataForTagId(self, tagId):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM tag_data WHERE tag_id=%s ORDER BY created_at DESC LIMIT 1"
        val = (tagId,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    
    def getTagDataCountForTagId(self, tagId):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT COUNT(*) as count FROM tag_data WHERE tag_id=%s"
        val = (tagId,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getTagDataHourCountForTagId(self, tagId):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT COUNT(*) as count FROM tag_data WHERE tag_id=%s  AND created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)"
        val = (tagId,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None
    

    def addUser(self, fname, lname, is_device_admin, is_device_operator, sync_id):
        cur = self.db.cursor()

        sql = "INSERT INTO users (fname, lname, is_device_admin, is_device_operator, sync_id) VALUES (%s,%s,%s,%s,%s)"
        val = (fname, lname, is_device_admin, is_device_operator, sync_id,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def updateUserById(self, id, fname=None, lname=None, is_device_admin=None, is_device_operator=None, sync_id=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE users SET "
        val = ()

        updateCols = []
        if fname:
            updateCols.append("fname=%s")
            val = val + (fname,)
        if lname:
            updateCols.append("lname=%s")
            val = val + (lname,)
        if is_device_admin:
            updateCols.append("is_device_admin=%s")
            val = val + (is_device_admin,)
        if is_device_operator:
            updateCols.append("is_device_operator=%s")
            val = val + (is_device_operator,)
        if sync_id:
            updateCols.append("sync_id=%s")
            val = val + (sync_id,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE id=%s"
        val = val + (id,)

        cur.execute(sql, val)
        self.db.commit()

    def updateUserBySyncId(self, sync_id, fname=None, lname=None, is_device_admin=None, is_device_operator=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE users SET "
        val = ()

        updateCols = []
        if fname:
            updateCols.append("fname=%s")
            val = val + (fname,)
        if lname:
            updateCols.append("lname=%s")
            val = val + (lname,)
        if is_device_admin:
            updateCols.append("is_device_admin=%s")
            val = val + (is_device_admin,)
        if is_device_operator:
            updateCols.append("is_device_operator=%s")
            val = val + (is_device_operator,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE sync_id=%s"
        val = val + (sync_id,)

        cur.execute(sql, val)
        self.db.commit()

    def getUsers(self):

        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM users ORDER BY lname, fname"
        cur.execute(sql)

        results = cur.fetchall()

        return results

    def getUserFromId(self, id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM users WHERE id=%s"
        val = (id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getUserFromSyncId(self, sync_id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM users WHERE sync_id=%s"
        val = (sync_id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None
    

    def addProduct(self, name, productCode, idealCPH, sync_id=None):
        cur = self.db.cursor()

        sql = "INSERT INTO products (name, product_code, ideal_cph, sync_id) VALUES (%s,%s,%s,%s)"
        val = (name, productCode or 'NULL', idealCPH, sync_id or 'NULL',)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def getProducts(self):

        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM products ORDER BY name"
        cur.execute(sql)

        results = cur.fetchall()

        return results

    def getProductFromId(self, id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM products WHERE id=%s"
        val = (id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getProductFromSyncId(self, sync_id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM products WHERE sync_id=%s"
        val = (sync_id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def updateProductById(self, id, name=None, productCode=None, ideal_cph=None, sync_id=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE products SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if productCode:
            updateCols.append("product_code=%s")
            val = val + (productCode,)
        if ideal_cph:
            updateCols.append("ideal_cph=%s")
            val = val + (ideal_cph,)
        if sync_id:
            updateCols.append("sync_id=%s")
            val = val + (sync_id,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE id=%s"
        val = val + (id,)

        cur.execute(sql, val)
        self.db.commit()

    def updateProductBySyncId(self, sync_id, name=None, productCode=None, ideal_cph=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE products SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if productCode:
            updateCols.append("product_code=%s")
            val = val + (productCode,)
        if ideal_cph:
            updateCols.append("ideal_cph=%s")
            val = val + (ideal_cph,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE sync_id=%s"
        val = val + (sync_id,)

        cur.execute(sql, val)
        self.db.commit()

    def addPrimaryDowntimeReason(self, name, sync_id=None):
        cur = self.db.cursor()

        sql = "INSERT INTO downtime_reasons (name, sync_id) VALUES (%s,%s)"
        val = (name, sync_id or 'NULL',)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

    def addSecondaryDowntimeReason(self, name, primaryId, sync_id=None):
        cur = self.db.cursor()

        sql = "INSERT INTO downtime_reasons (name, is_secondary_for, sync_id) VALUES (%s,%s,%s)"
        val = (name, primaryId, sync_id or 'NULL',)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId


    def getDowntimeReasonFromId(self, id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM downtime_reasons WHERE id=%s"
        val = (id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getDowntimeReasonFromSyncId(self, sync_id):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM downtime_reasons WHERE sync_id=%s"
        val = (sync_id,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results[0] if results else None

    def getPrimaryDowntimeReasons(self):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM downtime_reasons WHERE is_secondary_for IS NULL ORDER BY name"
        cur.execute(sql)

        results = cur.fetchall()

        return results

    def getSecondaryDowntimeReasons(self, primaryId):
        cur = self.db.cursor(dictionary=True)

        sql = "SELECT * FROM downtime_reasons WHERE is_secondary_for=%s ORDER BY name"
        val = (primaryId,)
        cur.execute(sql, val)

        results = cur.fetchall()

        return results

    def addDowntimeReasonForTag(self, tagId, reasonId):
        cur = self.db.cursor()

        sql = "UPDATE tag_data SET downtime_reason_id=%s, needs_resync=1, updated_at=CURRENT_TIMESTAMP WHERE (id=%s)"
        val = (reasonId, tagId,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId


    def getDowntimeReasonsForTag(self, tagId, downtimeValue=0):
        # Note: the "downtimeValue" is probably usually 0
        cur = self.db.cursor(dictionary=True)
        sql = """
            SELECT 
                tags.name AS tag_name, 
                r2.name AS reason2, 
                r1.name AS reason1,
                CONCAT_WS(', ', r2.name, r1.name) AS downtime_reason
            FROM tag_data
            LEFT JOIN downtime_reasons AS r1 on r1.id = tag_data.downtime_reason_id
            LEFT JOIN downtime_reasons AS r2 on r2.id = r1.is_secondary_for
            LEFT JOIN tags on tags.id = tag_data.tag_id
            WHERE tag_data.tag_id=%s AND tag_data.int_value=%s
        """
        val = (tagId, downtimeValue,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results

    def updateDowntimeReasonsById(self, id, name=None, is_secondary_for=None, sync_id=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE downtime_reasons SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if is_secondary_for:
            updateCols.append("is_secondary_for=%s")
            val = val + (is_secondary_for,)
        if sync_id:
            updateCols.append("sync_id=%s")
            val = val + (sync_id,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE id=%s"
        val = val + (id,)

        cur.execute(sql, val)
        self.db.commit()

    def updateDowntimeReasonsBySyncId(self, sync_id, name=None, is_secondary_for=None, deleted_at=None):
        cur = self.db.cursor()

        sql = "UPDATE downtime_reasons SET "
        val = ()

        updateCols = []
        if name:
            updateCols.append("name=%s")
            val = val + (name,)
        if is_secondary_for:
            updateCols.append("is_secondary_for=%s")
            val = val + (is_secondary_for,)
        if deleted_at:
            updateCols.append("deleted_at=%s")
            val = val + (deleted_at,)

        updateCols.append("updated_at=CURRENT_TIMESTAMP")
        
        sql += ','.join(updateCols)

        sql += " WHERE sync_id=%s"
        val = val + (sync_id,)

        cur.execute(sql, val)
        self.db.commit()

    def getTagStatusDurations(self, tagId):
        cur = self.db.cursor(dictionary=True)
        sql = """
            SELECT 
                tags.name,
                tag_data.int_value,
                tag_data.created_at AS timestamp,
                LAG(tag_data.created_at) OVER (PARTITION BY tag_data.tag_id ORDER BY tag_data.created_at) AS previous_timestamp,
                TIMEDIFF(tag_data.created_at, LAG(tag_data.created_at) OVER (PARTITION BY tag_data.tag_id ORDER BY tag_data.created_at)) AS time_diff,
                TIMESTAMPDIFF(SECOND, LAG(tag_data.created_at) OVER (PARTITION BY tag_data.tag_id ORDER BY tag_data.created_at), tag_data.created_at) AS seconds_diff
            FROM tag_data
            INNER JOIN tags on tags.id = tag_data.tag_id
            WHERE tag_data.tag_id=%s
            ORDER BY tag_data.created_at
        """
        val = (tagId,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results

    def getRunningStatusWithoutDowntimeReason(self, tagId, downtimeValue=0):
        # Note: the "downtimeValue" is probably usually 0
        cur = self.db.cursor(dictionary=True)
        sql = """
            SELECT 
                tag_data.id as tag_id,
                tags.name AS tag_name,
                downtime_reasons.name AS downtime_reasons,
                tag_data.created_at AS timestamp
            FROM tag_data
            LEFT OUTER JOIN downtime_reasons on downtime_reasons.id = tag_data.downtime_reason_id
            LEFT OUTER JOIN tags on tags.id = tag_data.tag_id
            WHERE tag_data.tag_id=%s AND tag_data.int_value=%s AND tag_data.downtime_reason_id IS NULL
            ORDER BY tag_data.created_at DESC
        """
        val = (tagId,downtimeValue,)

        cur.execute(sql, val)

        results = cur.fetchall()

        return results

    def addLogEntry(self, type, message):
        cur = self.db.cursor()

        sql = "INSERT INTO log (type, message) VALUES (%s,%s)"
        val = (int(type), message,)

        cur.execute(sql, val)

        self.db.commit()

        insertedId = cur.lastrowid

        return insertedId

