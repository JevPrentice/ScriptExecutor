# ScriptExecutor author jevprentice

#This program iterates over a given directory executing all '.SQL' files on a specific schema to a connected database using jdbc
#Only postgres has been tested.

#How to use this software:
#Setup the config.properties
#Forward the remote servers postgres to your port 10000 (or anything that matches the database_url config)
#Make sure all scripts in the sql_dir contain only working reusable SQL, ordered alphabetically for execution (files execute before folders)
#Make sure all scripts in the sql_dir will execute correctly when running from context 'set search_path to schema_name;'
# *When the ScriptExecutor reads the sql scripts into memory it replaces the string pattern 'schema_name' in the sql files with the schema_name config value
# *references to public schema must be explicit
#Customize PROPERTIES_DIR or pass your directory into main
#Run it.