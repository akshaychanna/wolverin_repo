from bs4 import BeautifulSoup
from lxml import html
from datetime import datetime
import logging
from utilities import DatalakeConnect
from typing import Dict


def to_rows_updated(html_text, index):
    def cell_text(td): return '\n'.join(
        t.text.replace('\xa0', ' ').strip() for t in td).strip()

    def parse_row(tr): return [cell_text(td)
                               for td in tr.find_all(['td', 'th'])]
    soup = BeautifulSoup(html_text, 'lxml')
    tables = soup.find_all("table")
    if tables:
        table_rows = tables[index].find_all('tr')
        if not table_rows:
            return None
        data = [parse_row(tr) for tr in table_rows]
        return data
    else:
        return None


def insert_data(value=Dict()):
    datalake_connection = DatalakeConnect()
    data_collection = datalake_connection.connect_to_collection(
        database="Land_Records", collection="tn_data_bulk"
    )
    data_collection.insert_one(value)

def tn_parser(response, records):
    geography_id = 23
    site_ref_id = 23001
    response_content = response.read() 
    file_name = f"{records['district_name']}_{records['taluka_name']}_{records['village_name']}_{records['survey_number']}_{records['subdivcode']}"
    file_name.replace('/', '-')
    resp = html.fromstring(response_content)
    tn_details = {}
    owner_list = []
    ref_xpath = '//img[@alt="barCode"]/@src'
    owner_data = to_rows_updated(response_content, 1)
    if owner_data and len(owner_data) > 1:
        for owner_item in owner_data[1:]:
            if len(owner_item) == 5:
                owner = {
                    'name': owner_item[3],
                    'relation': owner_item[2],
                    'relative': owner_item[1]}
                owner_list.append(owner)

    else:
        logger.info('Failed to parse owner data!!')
    # Survey No parse
    inner_info = to_rows_updated(response_content, 2)
    survey_details = []
    total_values = {}
    if inner_info and len(inner_info) > 8:
        for row in inner_info[5:-3]:
            if len(row) == 9:
                details = {
                    "survey_no": row[0],
                    "sub_division": row[1],
                    "barren_land": {
                        "area": row[2],
                        "tax": row[3]
                    },
                    "cultivable_land": {
                        "area": row[4],
                        "tax": row[5]
                    },
                    "others": {
                        "area": row[6],
                        "tax": row[7]
                    },
                    "remarks": row[8]
                }
                survey_details.append(details)

            else:
                logger.info(
                    "length of columns is less than or greator than 9")

        total_keys = [
            'total_barren_area',
            'total_barren_tax',
            'total_cultivable_area',
            'total_cultivable_tax',
            'total_other_area',
            'total_other_tax']
        total_values = dict(zip(total_keys, inner_info[-2][2:-1]))

    else:
        logger.info('Failed to parse survey details!!')

    # Extract Reference no

    ref_no = resp.xpath(ref_xpath)
    refno = ref_no[0].split('=')[-1] if ref_no else None

    def _check_data(tag, path):
        span_val = resp.xpath(f'//{tag}[contains(.,"{path}")]/text()')
        if span_val:
            splited_span = span_val[0].split(':')
            return splited_span[-1].strip() if splited_span else None
        else:
            return None

    tn_details['district'] = _check_data('td', 'மாவட்டம் : ')
    tn_details['taluka'] = _check_data('span', 'வட்டம் : ')
    tn_details['revenue_village'] = _check_data(
        'td', 'வருவாய் கிராமம் : ')
    tn_details['khata_no'] = _check_data('span', 'பட்டா எண் : ')
    tn_details['owner_details'] = owner_list
    tn_details['survey_details'] = survey_details
    tn_details['total'] = total_values
    tn_details['referance_no'] = refno


    inner_status = tn_details.pop('inner', True)
    order_result = {
        # 'job_id': job_id,
        'geography_id': geography_id,
        'site_ref_id': site_ref_id,
        'district_code': records['district_code'],
        'district_name': records['district_name'],
        'taluka_code': records['taluka_code'],
        'taluka_name': records['taluka_name'],
        'village_code': records['village_code'],
        'village_name': records['village_name'],
        'survey_no': records['survey_number'],
        'subdivcode': records['subdivcode'],
        'scrape_type': "satbara",
        'inner_data_available': inner_status,
        'land_details': tn_details,
        'scrape_time': datetime.now()}

    insert_data(order_result)


if __name__ == '__main__':
    sample_dict = {
        'file_name':'/home/abhijeet/tn_lr_files/survey_doc_1_1B2.html',
        'meta_data': {
            'district_code': '123',
            'district_name': 'abc',
            'taluka_code': '456',
            'taluka_name': 'pqr',
            'village_code': '789',
            'village_name': 'xyz',
            'survey_number': '1',
            'subdivcode': '1abc'
        }
    }
    file_name = sample_dict.get('file_name')
    meta_data = sample_dict.get('meta_data')
    file_content = open(file_name, 'r')
    tn_parser(file_content, meta_data)