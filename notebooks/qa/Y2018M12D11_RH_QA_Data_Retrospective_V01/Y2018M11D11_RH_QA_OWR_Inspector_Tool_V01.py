
# coding: utf-8

# In[1]:

"""
Inspect Overall Water Risk per sector.



"""


# In[ ]:

sql = """"""
SELECT
  aq30_id,
  string_id,
  pfaf_id,
  gid_1,
  aqid,
  area_km2,
  name_1,
  gid_0,
  name_0,
  delta_id,
  bws_score,
  bwd_score,
  iav_score,
  sev_score,
  rfr_score,
  cfr_score,
  drr_score,
  gtd_score,
  ucw_score,
  cep_score,
  udw_score,
  usa_score,
  rri_score,
  bws_def_weight,
  bwd_def_weight,
  iav_def_weight,
  sev_def_weight,
  rfr_def_weight,
  cfr_def_weight,
  drr_def_weight,
  gtd_def_weight,
  ucw_def_weight,
  cep_def_weight,
  udw_def_weight,
  usa_def_weight,
  rri_def_weight,
  bws_def_weightedscore,
  bwd_def_weightedscore,
  iav_def_weightedscore,
  sev_def_weightedscore,
  rfr_def_weightedscore,
  cfr_def_weightedscore,
  drr_def_weightedscore,
  gtd_def_weightedscore,
  ucw_def_weightedscore,
  cep_def_weightedscore,
  udw_def_weightedscore,
  usa_def_weightedscore,
  rri_def_weightedscore,
  def_weight_sum,
  def_weightedscore_sum,
  owr_def_score
FROM
  `aqueduct30.aqueduct30v01.y2018m12d11_rh_master_weights_gpd_v01_v01`
WHERE string_id = '561400-AUS.11_1-2814'

