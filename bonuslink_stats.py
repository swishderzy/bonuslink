import pandas as pd
import numpy as np
import os, sys
path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)
import get_data
from datetime import datetime
import datetime as dt
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json



get_data.data_extractor.SQL_Query("""
    select distinct mob.date_created, mob.id, mob.fullname, mob.phone_number, mob.email_address, 
	case when pp.id is null then "TT" else "LL" end as user_type, 
    case when agr.id is null then "False" else "True" end as has_dd, 
    case when agr.allianz_insurance is null then "no_deal" else agr.allianz_insurance end as insuarnce
	from  mobile_user mob left join property pp on mob.id=pp.mobile_user_id
    left join speedmanage.users on users.phone = mob.phone_number collate utf8_general_ci
    left join speedmanage.agreement agr on agr.tenant_user_id = users.id
    where mob.install_source rlike "bonuslink"
    and (agr.status like "Complete" or agr.status is null)
    and (agr.date_created >= "2021-01-01" or agr.date_created is null)
    and (agr.tenant_user_id in (select distinct user_id from speedmanage.invoices where status like "paid") or agr.tenant_user_id is null)
    
    """)