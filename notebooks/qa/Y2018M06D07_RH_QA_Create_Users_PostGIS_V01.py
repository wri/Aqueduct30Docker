
# coding: utf-8

# In[ ]:

""" Create users and manage permissions in database.
-------------------------------------------------------------------------------

Run the query in PGAdmin or using python. 

passwords sent using email to tianyi and sam.


Author: Rutger Hofste
Date: 20180607
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


# In[ ]:




# In[ ]:

CREATE USER tianyi WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOREPLICATION
    CONNECTION LIMIT -1
    PASSWORD 'xxxxxx';
    
GRANT USAGE ON SCHEMA test TO tianyi;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test TO tianyi;

ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT ALL ON TABLES to tianyi;


