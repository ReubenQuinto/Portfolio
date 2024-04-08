# v0.03

##### documentation ###############################################################################

# OBJECT 01: class read_table
# 	"""
# 	When a user inputs a table, this class queries the system tables and grabs:
# 		1. db name, 2. table name, 3. column names, 4. primary keys, <note: add schema name>

# 	This also gathers information from the connection string the user passed in:
# 		1. user, 2. password, 3. hostname
# 	"""

# OBJECT 02: class read_s3_table

# OBJECT 03: def generate_sql
# 	if   load_type == 'create_temporary_table'
# 	elif load_type == 'append_to_temporary_table'
# 	elif load_type == 'append'
# 	elif load_type == 'copy'
# 	elif load_type == 'merge_upsert_update'
# 	elif load_type == 'merge_upsert_insert'
# 	elif load_type == 'truncate'

# OBJECT 04: def load
# 	if   'read_s3_file in str(load_type.__class__):
# 		{ execute: create temporary table }
# 		{ execute: copy => from s3 to temporary table }
# 	if   load_type == 'merge_upsert'
# 		{ execute: merge_upsert_update }
# 		{ execute: merge_upsert_insert }
# 	elif load_type == 'merge_insert'
# 	elif load_type == 'merge_update'
# 	elif load_type == 'insert_append'
# 	elif load_type == 'insert_table'
# 	elif load_type == 'insert_partition'

# 	notes: toDo:
# 		- add delete temporary table (for cleanup)
#       - add delete partition

###################################################################################################

import pandas as pd
from sqlalchemy import create_engine
from smart_open import smart_open
import boto3

class read_table:
    def __init__(self, table_name, connection=None):
        """
        this class returns the following (7):
            from RDS system tables: 
                1) database name = ""
                2) table name = ""
                3) column names = ""
                4) primary keys = ""

            from user input:
                5) user
                6) password
                7) hostname

        requirements:
            conn = { 'user': "",
                     'password': "",
                     'hostname': "",
                     'db_name': "" }
        """

        sql_query = f"""
        WITH a AS (
        /* ===== this is for column info ========== */
        SELECT table_schema
            , table_name
            , column_name
            , ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        /* ORDER BY table_schema, table_name, ordinal_position */
        ), b AS (
        /* ===== this is for primary key info ===== */
        SELECT a.table_schema
            , a.table_name
            , b.column_name
            , a.constraint_type
            , b.ordinal_position AS pk_ordinal_position
        FROM information_schema.table_constraints AS a
        JOIN information_schema.key_column_usage AS b 
             ON ( b.constraint_name = a.constraint_name AND
                  b.constraint_schema = a.constraint_schema AND
                  b.constraint_name = a.constraint_name )
        WHERE a.constraint_type = 'PRIMARY KEY'
        /* ORDER BY b.table_schema, b.table_name, b.ordinal_position ASC */
        )
        
        SELECT a.table_schema
            , a.table_name
            , a.column_name
            , a.ordinal_position
            , b.constraint_type
            , b.pk_ordinal_position
        FROM a
        LEFT JOIN b ON ( a.table_schema = b.table_schema AND
                         a.table_name = b.table_name AND
                         a.column_name = b.column_name )
        ORDER BY a.table_schema, a.table_name, a.ordinal_position ;
        """

        engine = create_engine(f'postgresql://{connection["user"]}:{connection["password"]}@{connection["hostname"]}/{connection["db_name"]}')

        with engine.connect() as c:
            df = pd.read_sql(sql_query, c)

        self.logical_db = connection['db_name']
        self.table_name = table_name
        self.column_names = list(df['column_name'])
        self.primary_keys = [ df['column_name'][i] for i in range(len(df)) if df['constraint_type'][i] == 'PRIMARY KEY' ]

        self.user = connection['user']
        self.password = connection['password']
        self.hostname = connection['hostname']

    def get_db_name(self):
        return self.logical_db

    def get_table_name(self):
        return self.table_name
    
    def get_column_names(self):
        return self.column_names
    
    def get_primary_keys(self):
        return self.primary_keys


    def get_user(self):
        return self.user

    def get_password(self):
        return self.password

    def get_hostname(self):
        return self.hostname

class read_s3_file:
    def __init__(self, file_name, connection=None):
        """
        this class returns the following:
            from user input
                1) file name
                2) column names
                3) bucket name
                4) bucket region
                5) access key id
                6) secret access key id

        requirements:
            conn = { 'aws_access_key_id': "",    # <=== this is now optional
                     'aws_secret_access_key':"", # <=== this is now optional
                     'aws_bucket_name':"",
                     'aws_bucket_region': "" }
        """

        ############################################################
        # note: 
        #   the purpose of this section is opt for (2): 1. hard coded access keys (via smart open) or 2. role base access (boto3)
        #   the implementation is to check (if statement) if user provided those keys or not.
        ############################################################

        if 'aws_access_key_id' not in connection.keys() and 'aws_secret_access_key' not in connection.keys():
            ############################################################
            # note: 
            #   this runs "import boto3"
            #   if aws_access_key_id AND aws_secret_access_key HAVE NOT been provided
            ############################################################

            client_s3 = boto3.client('s3') 
            obj = client_s3.get_object(Bucket=connection['aws_bucket_name'], Key=file_name) 

            df = pd.read_csv(obj['Body'])
            self.access_key_id = None
            self.secret_access_key = None

        elif 'aws_access_key_id' in connection.keys() and 'aws_secret_access_key' in connection.keys():
            ############################################################
            # note: 
            #   this runs "from smart_open import smart_open"
            #   if aws_access_key_id AND aws_secret_access_key HAVE been provided
            ############################################################

            path = 's3://{0}:{1}@{2}/{3}'.format( connection['aws_access_key_id'],
                                                  connection['aws_secret_access_key'],
                                                  connection['aws_bucket_name'],
                                                  file_name )

            df = pd.read_csv(smart_open(path))
            self.access_key_id = connection['aws_access_key_id']
            self.secret_access_key = connection['aws_secret_access_key']

        else:
            print('User passed in the wrong connection arguments. Connection keywords passed in does not cover the base cases.')



        self.table_name = 'temp_{0}'.format(file_name.replace('.csv', ''))
        self.column_names = [ x.replace(' ', '_') for x in list(df.columns) ] # this is beause there are spaces in column headers
        self.primary_keys = 'None'
        
        self.file_name = file_name
        self.bucket_name = connection['aws_bucket_name']
        self.bucket_region = connection['aws_bucket_region']
    
    def get_table_name(self):
        return self.table_name

    def get_column_names(self):
        return self.column_names


    def get_bucket_name(self):
        return self.bucket_name
    
    def get_file_name(self):
        return self.file_name
    
    def get_bucket_region(self):
        return self.bucket_region

    def get_access_key_id(self):
        return self.access_key_id

    def get_secret_access_key(self):
        return self.secret_access_key

def generate_sql(load_table=None, destination_table=None, load_type=None):
    
    if load_type == 'create_temporary_table':
        """
        data model:
            CREATE TEMPORARY TABLE <destination_table>_temp ( LIKE <source_table> ) ;
        """

        ##### execute sql #################################################

        rv = f"""
                 CREATE TEMPORARY TABLE {load_table.get_table_name()} ( LIKE {destination_table.get_table_name()} ) ;
              """

        return rv

    elif load_type == 'append_to_temporary_table':
        """
        data model:
            INSERT INTO <destination_table>
            SELECT <select clause>
            FROM <source_table>_temp ;
        """

        ##### select clause ###############################################

        str_select_statement = ""

        for x in load_table.get_column_names():
            if x != load_table.get_column_names()[-1]:
                s = f'{load_table.get_table_name()}_temp.{x}, '
                str_select_statement += s
            else:
                s = f'{load_table.get_table_name()}_temp.{x}'
                str_select_statement += s

        ##### execute sql #################################################

        rv = f"""
                INSERT INTO {destination_table.get_table_name()}
                SELECT {str_select_statement}
                FROM {load_table.get_table_name()} ;
              """

        return rv

    elif load_type == 'append':
        """
        data mode:
            INSERT INTO target_table
            SELECT {cols}
            FROM load_table ;
        """

        ##### select clause ###############################################

        str_select_statement = ""

        for x in load_table.get_column_names():
            if x != load_table.get_column_names()[-1]:
                s = f'{load_table.get_table_name()}.{x}, '
                str_select_statement += s
            else:
                s = f'{load_table.get_table_name()}.{x}'
                str_select_statement += s

        ##### execute sql #################################################

        rv = f"""
                INSERT INTO {destination_table.get_table_name()}
                SELECT {str_select_statement}
                FROM {load_table.get_table_name()} ;
              """

        return rv

    elif load_type == 'copy':
        
        ##### column names ################################################

        # note: this obj isnt used because spaces in .csv headers are messing up the copy comand
        str_columns = ""

        for x in load_table.get_column_names():
            if x!= load_table.get_column_names()[-1]:
                s = f'{x}, '
                str_columns += s
            else:
                s = f'{x}'
                str_columns += s

        ##### aws_keys ####################################################
        # note: if a user provides aws access keys => this will return aws access keys OR blanks
        x = load_table.get_access_key_id()
        aws_access_key_id = lambda: "''" if x == None else f"'{x}'"

        y = load_table.get_secret_access_key()
        aws_secret_access_key = lambda: "''" if y == None else f"'{y}'"

        ##### execute sql #################################################
        
        rv = f"""
                SELECT aws_s3.table_import_from_s3(
                    '{load_table.get_table_name()}'
                    , '{str_columns}'
                    , '(FORMAT csv, HEADER true, DELIMITER ",")'
                    , '{load_table.get_bucket_name()}'
                    , '{load_table.get_file_name()}'
                    , '{load_table.get_bucket_region()}'
                    , {aws_access_key_id()}
                    , {aws_secret_access_key()}
                ) ;
            """

        return rv

    elif load_type == 'merge_upsert_update':
        """
        data model:
            UPDATE { destination_table }
                SET actual_sale = b.actual_sale
            FROM { load_table } AS b
            WHERE target_table.date = b.date
                AND target_table.product = b.product
                AND target_table.state = b.state
                AND target_table.addr_id = b.addr_id
                AND target_table.actual_sale != b.actual_sale /* <---- SUPER IMPORTANT */ ;
        """

        ##### set clause ##################################################
        # notes: ls_set is a list of column names that is not a primary key

        str_set_statement = ""
        
        ls_set = [ x for x in destination_table.get_column_names() if x not in destination_table.get_primary_keys() ]

        for x in ls_set:
            if x != ls_set[-1]:
                s = f'{x} = {load_table.get_table_name()}.{x}, '
                str_set_statement += s
            else:
                s = f'{x} = {load_table.get_table_name()}.{x}'
                str_set_statement += s

        ##### where clause ################################################

        str_where_statement = ""

        for x in destination_table.get_primary_keys():
            if x != destination_table.get_primary_keys()[-1]:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x} AND '
                str_where_statement += s
            else:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x}'
                str_where_statement += s

        ##### execute sql #################################################
        
        rv = f"""
                  UPDATE {destination_table.get_table_name()}
                      SET {str_set_statement}
                  FROM {load_table.get_table_name()}
                  WHERE {str_where_statement} ;                  
              """

        return rv

    elif load_type == 'merge_upsert_insert':
        """
        data model:
            INSERT INTO testtable
            SELECT newvals.id, newvals.somedata
            FROM newvals
            LEFT OUTER JOIN testtable ON ( testtable.id = newvals.id )
            WHERE testtable.id IS NULL ;
        """

        ##### select clause ###############################################

        str_select_statement = ""
        
        for x in load_table.get_column_names():
            if x != load_table.get_column_names()[-1]:
                s = f'{load_table.get_table_name()}.{x}, '
                str_select_statement += s
            else:
                s = f'{load_table.get_table_name()}.{x}'
                str_select_statement += s

        ##### on clause ###################################################

        str_on_statement = ""
        
        for x in destination_table.get_primary_keys():
            if x != destination_table.get_primary_keys()[-1]:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x} AND '
                str_on_statement += s
            else:
                s = f'{destination_table.get_table_name()}.{x} = {load_table.get_table_name()}.{x}'
                str_on_statement += s

        ##### where clause ################################################

        str_where_statement = f'{destination_table.get_table_name()}.{destination_table.get_primary_keys()[0]}'

        ##### execute sql #################################################

        rv = f"""
                  INSERT INTO {destination_table.get_table_name()}
                  SELECT {str_select_statement}
                  FROM {load_table.get_table_name()}
                  LEFT OUTER JOIN {destination_table.get_table_name()} ON ( {str_on_statement} )
                  WHERE {str_where_statement} IS NULL ;
              """

        return rv

    elif load_type == 'truncate':
        """
        data model:
            TRUCATE TABLE table_name;
        """

        ##### execute sql #################################################
        
        rv = f"""
                  TRUNCATE TABLE {destination_table.get_table_name()} ;
              """
        
        return rv

def load(load_table=None, destination_table=None, load_type=None):
    database = destination_table.get_db_name()
    user = destination_table.get_user()
    password = destination_table.get_password()
    host = destination_table.get_hostname()    

    engine = create_engine(f'postgresql://{user}:{password}@{host}/{database}')

    with engine.connect() as connection:
        cursor = connection.begin()

        #####################################################################################################################
        # note: if the load_table is a csv file, then load csv to a staging/temp table
        try:
            if 'read_s3_file' in str(load_table.__class__):
                # create temporary table like destination table
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='create_temporary_table')
                print('executing: create_temporary_table', rv)
                connection.execute( rv )

                # copy into temporary table
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='copy')
                print('executing: copy', rv)
                connection.execute( rv )

        except Exception as e:
            print('Load Failed. Rolling back. Error msg: ', e)
            cursor.rollback()

        #####################################################################################################################
        # note: this is the main load_type section
        try:
            if load_type=="merge_upsert":
                # update statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='merge_upsert_update')
                print('executing: merge_upsert_update', rv)
                connection.execute( rv )

                # insert statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='merge_upsert_insert')
                print('executing: merge_upsert_insert', rv)
                connection.execute( rv )

                cursor.commit()

            elif load_type=="merge_insert":
                # insert statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='merge_upsert_insert')
                print('executing: merge_upsert_insert')
                connection.execute( rv )

                cursor.commit()

            elif load_type=="merge_update":
                # update statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='merge_upsert_update')
                print('executing: merge_upsert_update')
                connection.execute( rv )

                cursor.commit()

            elif load_type=="insert_append":
                # append statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='append')
                print('executing: append', rv)
                connection.execute( rv )

                cursor.commit()

            elif load_type=="insert_table":
                # truncate statement
                rv = generate_sql(load_table=None, destination_table=destination_table, load_type='truncate')
                print('executing: truncate')
                connection.execute( rv )

                # append statement
                rv = generate_sql(load_table=load_table, destination_table=destination_table, load_type='append')
                print('executing: append')
                connection.execute( rv )

                cursor.commit()

            elif load_type=="insert_partition":
                pass

            else:
                print('Error: Must choose an existing load type')

            print('LOAD FINISHED.')

        except Exception as e:
            print('Load Failed. Rolling back. Error msg: ', e)
            cursor.rollback()