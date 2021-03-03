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
from trello import TrelloClient


# get name, phone, blcard, ttorll, dd, ins:
df = get_data.data_extractor.SQL_Query("""
    select distinct  mob.date_created, users.bonus_link_card_number, mob.fullname, users.id, users.phone, mob.email_address, 
	case when pp.id is null then "TT" else "LL" end as user_type, 
    case when agr.status like "Complete" then "True" else "False" end as has_dd, 
    agr.date_created as deal_date, agr.price,
    case when agr.allianz_insurance is null then "no_insurance" else agr.allianz_insurance end as insuarnce, ccs.trello_card_id
	from speedmanage.users left join speedmanage.agreement agr on (agr.tenant_user_id = users.id or agr.landlord_user_id=users.id)
    left join speedrent.mobile_user mob on users.phone = mob.phone_number collate utf8_general_ci
    left join property pp on mob.id=pp.mobile_user_id
    left join speedrent.callcenter_sales ccs on ccs.tenant_id = mob.id
    where users.bonus_link_card_number is not null
    and agr.status like "Complete"
    and agr.date_created >= "2021-01-25"
    and agr.tenant_user_id in (select distinct user_id from speedmanage.invoices where status like "paid" and speedmanage.invoices.date_created >= "2021-01-25")
    """)


df = pd.DataFrame(df, columns = ['date_created', 'bonus_link_card_number',	'fullname','user_id', 'phone',	'email_address','user_type','has_dd', 'deal_date', 'price', 'insurance', 'trello_id'])
df




#get trello card lane:
client = TrelloClient(
    api_key='f374d0ae3f7452f55020f32ba2b5fc40',
    api_secret='002c000d57b399220a61d9dabf727c6dbe97d6d3d89b6935488fe6d5e12c0a25',
    token='86dc1336b9bdeedf9ba22e7715e2e3412154fea42f191838310b04a294799534',
    token_secret='002c000d57b399220a61d9dabf727c6dbe97d6d3d89b6935488fe6d5e12c0a25'
)


all_boards = client.list_boards()

board_id = [b.id for b in all_boards if "Speedrent Sales" in str(b)][0]


board = client.get_board(board_id)

current_lanes = pd.DataFrame(columns = ['card_id', 'lane'])
for _, card_id in enumerate(df.dropna(subset=['trello_id']).reset_index(drop=True)['trello_id']):
    if not _%5:
        print("fetching trello card current lane ===> ", _, "/", len(df.dropna(subset=['trello_id']).reset_index(drop=True)))
    try:
        card = client.get_card(card_id)
        lane = card.get_list().name
        current_lanes = current_lanes.append(pd.Series([card_id, lane], index = ['card_id', 'lane']), ignore_index=True)
    except:
        pass



# merge with main df:
df = pd.merge(df, current_lanes, how='left', left_on = "trello_id", right_on='card_id').drop(['trello_id', 'card_id'], 1)

df.fillna('none', inplace=True)


# update in sheet:

def update_sheet(data, spreadsheet_name = "BonusLink tracking", sheet_name = "BonusLink users (auto)"):    
    """
    insert values into a sheet, \n first create the sheet manually, \n this function will only update the values, \n the columns must stay the same
    """


    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(path + '/dealtrello-33db507b4f94.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open(spreadsheet_name).worksheet(sheet_name)
    last_cell = gspread.utils.rowcol_to_a1(len(data) + 1, len(data.columns))
    cell_list = sheet.range(f"A1:{last_cell}")
    values = data.columns.tolist() + sum(data.values.tolist(), [])


    for cell, value in zip(cell_list, values):
        if type(value) in [int, float, str]:
            cell.value = value
        else:
            cell.value = str(value)


    sheet.clear()

    sheet.update_cells(cell_list)


    print("Done updating:", spreadsheet_name, "==>", sheet_name)

update_sheet(df.sort_values('deal_date', ascending = False))