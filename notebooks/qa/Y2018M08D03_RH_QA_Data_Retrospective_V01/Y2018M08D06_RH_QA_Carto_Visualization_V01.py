
# coding: utf-8

# In[1]:

""" Create the SQL and CartoCss required for the visual in carto builder.
-------------------------------------------------------------------------------





"""

SCRIPT_NAME = 'Y2018M08D06_RH_QA_Carto_Visualization_V01'
OUTPUT_VERSION = 1

CARTO_HYBAS_INPUT_TABLE_NAME = "y2018m07d18_rh_upload_hydrobasin_carto_v01_simplified_v03"
CARTO_INPUT_TABLE_NAME = "y2018m08d06_rh_qa_delta_ids_v01_v01"
CARTO_OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)


print("\nCARTO_OUTPUT_TABLE_NAME: :", CARTO_OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import cartoframes


# In[4]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()
creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = cartoframes.CartoContext(creds=creds)


# In[5]:

base_sql = """
SELECT 
  l.cartodb_id,
  l.pfaf_id,
  l.the_geom_webmercator,
  r.delta_id,
  r.waterstress_label_dimensionless_coalesced,
  r.waterstress_category_dimensionless_coalesced,
  r.waterstress_score_dimensionless_coalesced,
  r.waterstress_raw_dimensionless_coalesced,
  r.waterstress_label_dimensionless_delta,
  r.waterstress_category_dimensionless_delta,
  r.waterstress_score_dimensionless_delta,
  r.waterstress_raw_dimensionless_delta,
  r.waterstress_label_dimensionless_30spfaf06,
  r.waterstress_category_dimensionless_30spfaf06,
  r.waterstress_score_dimensionless_30spfaf06,
  r.waterstress_raw_dimensionless_30spfaf06
FROM
  {} l 
LEFT JOIN {} r
  ON l.pfaf_id = r.pfafid_30spfaf06      
""".format(CARTO_HYBAS_INPUT_TABLE_NAME,CARTO_INPUT_TABLE_NAME)


# In[6]:

def create_sql(base_sql,temporal_resolution,year,month):
    sql = base_sql
    sql += " WHERE temporal_resolution = '{}'".format(temporal_resolution)
    sql += " AND month = {}".format(month)
    sql += " AND year = {}".format(year)
    return sql
    


# In[7]:

temporal_resolutions = ["month","year"]

process_dict = {}

for temporal_resolution in temporal_resolutions:
    if temporal_resolution == "year":
        month = 12
        output_file_name = "data_retrospective_20180811_{}_Y2014M{:02.0f}".format(temporal_resolution,month)
        output_file_name = output_file_name.lower()
        query =  create_sql(base_sql,"year",2014,month)
        process_dict[output_file_name] = query
        
    
    elif temporal_resolution == "month":
        for month in range(1,13):
            output_file_name = "data_retrospective_20180811_{}_Y2014M{:02.0f}".format(temporal_resolution,month)
            output_file_name = output_file_name.lower()
            query =  create_sql(base_sql,temporal_resolution,2014,month)
            process_dict[output_file_name] = query

    


# In[8]:

for output_file_name, query in process_dict.items():
    print("output_file_name: ",output_file_name)
    print("query: ",query)
    cc.query(query=query,
             table_name= output_file_name)


# In[9]:

# carto css


# In[ ]:

#layer {
  polygon-fill: ramp([waterstress_raw_dimensionless_30spfaf06], (#4E4E4E,#ffff99, #ffe600, #ff9900, #ff1900, #990000,#808080), quantiles);
}
#layer::outline {
  line-width: 1;
  line-color: #FFFFFF;
  line-opacity: 0.5;
}


# In[ ]:




# In[ ]:




# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
