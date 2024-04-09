Objective: 
	- The goal of this python library is to create a sql wrapper to automate loading data into a database.
	- The hope is to reduce the turnaround time to deploy Load scripts.

The solution: 
	- I created a python library that auto-generates SQL commands and passes those commands to the database.
	- Its able to auto-generate these commands by reading the tables DDL (column names & primary keys).
	- You can either load FROM a file in S3 OR load file from another table.

Key Takeaways: 
	- If you've done enough data engineering you notice roughly 6 patterns in LOAD scripts:
		1. Upsert
			® Will process every record in dataset
			® Will either: update or insert the record into the target table
		2. Insert Only
			® Will insert only: those records in your dataset that do not match a record already in your target table
		3. Update Only
			® Will update only: those records that already exist in your table
		4. Insert None
			® Will insert: continually to the end of your table
		5. Insert Table
			® Will truncate: the entire target table prior to loading (use only if you plan to rebuild every time)
		6. Insert Partition
			® Will truncate: target tables designated partitions based on load settings
	- This library will auto generate the appropriate sql procedures & sql commands to load your dataset into your database.
	- Overall, this library seeks to reduce turnaround time to load data & improve the quality of your database through automation.

Python Example:
        ##### import libraries #################################################

        import datetime
        from module_load_types import load_types

        ##### defining objects #################################################

        ##### target_file name

        # s3 target file name
        target_file = { 'dryrun' : True,
                        'file_name' : '2023_05_01.csv' }
            
        yesterday_utc = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        yyyy = yesterday_utc.strftime('%Y')
        mm = yesterday_utc.strftime('%m')
        dd = yesterday_utc.strftime('%d')
        file_name = f'{yyyy}_{mm}_{dd}.csv'

        ##### credentials

        # aws s3 credentials
        connection_iam_user = {
            # 'aws_access_key_id' : '<...>',
            # 'aws_secret_access_key' : '<...>',
            'aws_bucket_name' : '<bucket>',
            'aws_bucket_region' : '<region>'
        }

        # rds credentails
        connection_rds_user = {
            'user' : '<user>',
            'password' : '<pw>',
            'hostname' : '<endpoint>',
            'db_name' : '<db_name>'
        }

        ##### execute code #####################################################

        # load table
        load_table = load_types.read_s3_file(file_name, connection=connection_iam_user )

        # destination table
        destination_table = load_types.read_table('stonks', connection=connection_rds_user )

        load_types.load(load_table=load_table, destination_table=destination_table, load_type='merge_upsert')