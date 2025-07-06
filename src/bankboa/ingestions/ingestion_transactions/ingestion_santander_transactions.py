"""
Ingestion module for the MOVEMENTS files from Santander bank

Written by Daniel Brosnan Blázquez

module bankboa
"""
# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import pandas as pd
from dateutil.relativedelta import relativedelta

# Import xml parser
from lxml import etree

# Import EBOA ingestion functions helpers
import eboa.ingestion.functions as eboa_ingestion_functions
import eboa.ingestion.xpath_functions as xpath_functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import query
from eboa.engine.query import Query

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log

# Import query
from eboa.engine.query import Query

logging_module = Log(name = __name__)
logger = logging_module.logger

version = "1.0"

@debug
def _generate_movements_events(parsed_xls, source, engine, query):
    """
    Method to generate the events of the movements files

    :param parsed_xls: source of information already parsed
    :type parsed_xls: pandas object
    :param source: information of the source
    :type source: dict
    :param engine: Engine instance
    :type engine: Engine
    :param query: Query instance
    :type query: Query
    """

    # Default alert notification time
    notification_time = (parser.parse(source["reported_validity_start"]) - datetime.timedelta(days=1)).isoformat()
    if notification_time > datetime.datetime.now().isoformat():
        notification_time = datetime.datetime.now().isoformat()
    # end if

    # Get XML configurations
    groups_xml = etree.parse(get_resources_path() + "/groups.xml")
    groups_xpath = etree.XPathEvaluator(groups_xml)
    entities_xml = etree.parse(get_resources_path() + "/entities.xml")
    entities_xpath = etree.XPathEvaluator(entities_xml)
    
    events = {}
    events["movements"] = []
    
    # Iterate through rows
    for index, row in parsed_xls.iterrows():

        operation_date = parser.parse(row["FECHA OPERACIÓN"], dayfirst=True).isoformat()
        value_date = parser.parse(row["FECHA VALOR"], dayfirst=True).isoformat()
        start = value_date
        stop = (parser.parse(row["FECHA VALOR"], dayfirst=True) + datetime.timedelta(days=1)).isoformat()
        concept = row["CONCEPTO"]
        amount = float(row["IMPORTE EUR"])
        balance = float(row["SALDO"])

        groups = groups_xpath("/groups/group[boolean(matching_rules/rule[contains($concept, match)][$amount > 0 and amount = '>0' or $amount < 0 and amount = '<0'])]", concept = concept, amount = amount)

        entities = entities_xpath("/entities/entity[boolean(matching_strings/string[contains($concept, text())])]", concept = concept)

        values = [
            {"name": "bank",
             "type": "text",
             "value": "BANCO SANTANDER"},
            {"name": "concept",
             "type": "text",
             "value": concept},
            {"name": "amount",
             "type": "double",
             "value": amount},
            {"name": "balance",
             "type": "double",
             "value": balance},
            {"name": "value_date",
             "type": "timestamp",
             "value": value_date},
            {"name": "operation_date",
             "type": "timestamp",
             "value": operation_date}
        ]

        # Insert groups
        i = 0
        for group in groups:
            values.append(
                {"name": f"group{i}",
                 "type": "text",
                 "value": group.xpath("name")[0].text},
            )
            i += 1
        # end for
        if i == 0:
            values.append(
                {"name": "number_of_groups",
                 "type": "double",
                 "value": 1}
            )
            if amount < 0:
                values.append(
                    {"name": "group0",
                     "type": "text",
                     "value": "Spending no group"}
                )
            else:
                values.append(
                    {"name": "group0",
                     "type": "text",
                     "value": "Income no group"}
                )
            # end if
        else:
            values.append(
                {"name": "number_of_groups",
                 "type": "double",
                 "value": i}
            )
        # end if
                    
            
        # Insert entities
        i = 0
        for entity in entities:
            values.append(
                {"name": f"entity{i}",
                 "type": "text",
                 "value": entity.xpath("name")[0].text},
            )
            i += 1
        # end for
        values.append(
            {"name": "number_of_entities",
             "type": "double",
             "value": i}
        )
            
        events["movements"].append({
            "gauge": {
                "insertion_type": "INSERT_and_ERASE",
                "name": "MOVEMENT",
                "system": "BANCO SANTANDER"
            },
            "start": start,
            "stop": stop,
            "values": values,
        })

    # end for

    return events

def process_file(file_path, engine, query, reception_time):
    """Function to process the file and insert its relevant information
    into the DDBB of the eboa

    :param file_path: path to the file to be processed
    :type file_path: str
    :param engine: Engine instance
    :type engine: Engine
    :param query: Query instance
    :type query: Query
    :param reception_time: time of the reception of the file by the triggering
    :type reception_time: str

    :return: data with the structure to be inserted into the DDBB
    :rtype: dict
    """
    global ingestion_completeness
    global ingestion_completeness_message

    # Ingestion completeness
    ingestion_completeness = "true"
    ingestion_completeness_message = ""

    file_name = os.path.basename(file_path)

    # Get the general source entry (processor = None, version = None, DIM signature = PENDING_SOURCES)
    # This is for registrering the ingestion progress
    query_general_source = Query()
    session_progress = query_general_source.session
    general_source_progress = query_general_source.get_sources(names = {"filter": file_name, "op": "=="},
                                                               dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="},
                                                               processors = {"filter": "", "op": "=="},
                                                               processor_version_filters = [{"filter": "", "op": "=="}])

    if len(general_source_progress) > 0:
        general_source_progress = general_source_progress[0]
    # end if

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 10)
    
    # Parse file
    parsed_xls = pd.read_excel(file_path, header = 7)

    # Set metadata of source
    generation_time = file_name[19:34]
    reported_validity_start = file_name[35:50]
    reported_validity_stop = file_name[51:66]

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 20)

    source = {
        "name": file_name,
        "reception_time": reception_time,
        "generation_time": generation_time,
        "reported_validity_start": reported_validity_start,
        "reported_validity_stop": reported_validity_stop,
    }
    
    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 40)

    # Default alert notification time
    notification_time = (parser.parse(source["reported_validity_start"]) - datetime.timedelta(days=1)).isoformat()
    if notification_time > datetime.datetime.now().isoformat():
        notification_time = datetime.datetime.now().isoformat()
    # end if

    # Generate movements events
    events = _generate_movements_events(parsed_xls, source, engine, query)

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 90)
    
    # Build the xml
    operations = []
    data = {"operations": operations}

    # Register the ingestion completeness
    source["ingestion_completeness"] = {
        "check": ingestion_completeness,
        "message": ingestion_completeness_message
    } 

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 94)
    
    # Insert movements events
    if len(events["movements"]):
        event_starts = [event["start"] for event in events["movements"]]
        event_starts.sort()
        event_stops = [event["stop"] for event in events["movements"]]
        event_stops.sort()

        source = {
            "name": file_name,
            "reception_time": reception_time,
            "generation_time": generation_time,
            "reported_validity_start": reported_validity_start,
            "reported_validity_stop": reported_validity_stop,
            "validity_start": event_starts[0],
            "validity_stop": event_stops[-1]
        }

        operations.append({
            "mode": "insert_and_erase",
            "dim_signature": {
                "name": "MOVEMENTS_SANTANDER",
                "exec": os.path.basename(__file__),
                "version": version
            },
            "source": source,
            "events": events["movements"]
        })

        eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 96)

        events["update_months"] = []
        month_start = parser.parse(event_starts[0][0:7], default=datetime.datetime(2015, 1, 1))
        first_month_start = month_start
        month_stop = month_start + relativedelta(months=1)
        while month_start.isoformat() < event_stops[-1]:
            last_month_stop = month_stop
            events["update_months"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "UPDATE_MONTH",
                    "system": "BANCO SANTANDER"
                },
                "start": month_start.isoformat(),
                "stop": month_stop.isoformat(),
                "values": [
                    {"name": "status",
                     "type": "text",
                     "value": "UPDATE"}
                ]
            })
            month_start = month_stop
            month_stop = month_stop + relativedelta(months=1)
        # end while

        source = {
            "name": file_name,
            "reception_time": reception_time,
            "generation_time": generation_time,
            "reported_validity_start": reported_validity_start,
            "reported_validity_stop": reported_validity_stop,
            "validity_start": first_month_start.isoformat(),
            "validity_stop": last_month_stop.isoformat()
        }

        operations.append({
            "mode": "insert_and_erase",
            "dim_signature": {
                "name": "UPDATE_MONTHS_SANTANDER",
                "exec": os.path.basename(__file__),
                "version": version
            },
            "source": source,
            "events": events["update_months"]
        })

        eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 98)

        events["update_years"] = []
        year_start = parser.parse(event_starts[0][0:4], default=datetime.datetime(2015, 1, 1))
        first_year_start = year_start
        year_stop = year_start + relativedelta(years=1)
        while year_start.isoformat() < event_stops[-1]:
            last_year_stop = year_stop
            events["update_years"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "UPDATE_YEAR",
                    "system": "BANCO SANTANDER"
                },
                "start": year_start.isoformat(),
                "stop": year_stop.isoformat(),
                "values": [
                    {"name": "status",
                     "type": "text",
                     "value": "UPDATE"}
                ]
            })
            year_start = year_stop
            year_stop = year_stop + relativedelta(years=1)
        # end while

        source = {
            "name": file_name,
            "reception_time": reception_time,
            "generation_time": generation_time,
            "reported_validity_start": reported_validity_start,
            "reported_validity_stop": reported_validity_stop,
            "validity_start": first_year_start.isoformat(),
            "validity_stop": last_year_stop.isoformat()
        }

        operations.append({
            "mode": "insert_and_erase",
            "dim_signature": {
                "name": "UPDATE_YEARS_SANTANDER",
                "exec": os.path.basename(__file__),
                "version": version
            },
            "source": source,
            "events": events["update_years"]
        })
    # end if

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 100)

    query.close_session()

    return data
