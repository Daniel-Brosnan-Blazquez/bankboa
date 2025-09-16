"""
Ingestion module for grouping transactions

Written by Daniel Brosnan Blázquez

module bankboa
"""
# Import python utilities
import os
import argparse
from dateutil import parser
import datetime
import json
import openpyxl
import pandas as pd
from dateutil.relativedelta import relativedelta
import pdb

# Import xml parser
from lxml import etree

# Import EBOA ingestion functions helpers
import eboa.ingestion.functions as eboa_ingestion_functions
import eboa.ingestion.xpath_functions as xpath_functions
from eboa.engine.functions import get_resources_path, get_schemas_path

# Import query
from eboa.engine.query import Query

# Import datamodel
from eboa.datamodel.events import Event, EventText

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
def _generate_aggregated_movements_events(parsed_xls, source, engine, query):
    """
    Method to generate the aggregated events of the transactions

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
    movement_groups = [node.text for node in groups_xpath("/groups/group/name")]
    entities_xml = etree.parse(get_resources_path() + "/entities.xml")
    entities_xpath = etree.XPathEvaluator(entities_xml)
    movement_entities = [node.text for node in entities_xpath("/entities/entity/name")]

    events = {}
    events["aggregated_movements_month"] = []

    update_month_events = query.get_events(
        gauge_names = {"filter": "UPDATE_MONTH", "op": "=="},
        value_filters = [{"name": {"filter": "status", "op": "=="}, "type": "text", "value": {"op": "==", "filter": "UPDATE"}}],
        order_by = {"field": "start", "descending": False})
    
    # Iterate through events
    for event in update_month_events:

        # Iterate through groups
        for group in movement_groups + ["Spending no group", "Income no group"]:
            
            movement_events = query.get_events(
                gauge_names = {"filter": "MOVEMENT", "op": "=="},
                start_filters = [{"date": event.stop.isoformat(), "op": "<"}, {"date": event.start.isoformat(), "op": ">="}],
                value_filters = [{"name": {"filter": "group%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": group}}])

            if len(movement_events) == 0:
                continue

            movement_events_uuids = [event.event_uuid for event in movement_events]
            
            amounts = [amount.value for event in movement_events for amount in event.eventDoubles if amount.name == "amount"]
            
            amount = sum(amounts)

            values = [
                {"name": "bank",
                 "type": "text",
                 "value": "BANCO SANTANDER"},
                {"name": "group",
                 "type": "text",
                 "value": group},
                {"name": "amount",
                 "type": "double",
                 "value": amount}
            ]

            links = []
            for event_uuid in movement_events_uuids:
                links.append({
                    "link": str(event_uuid),
                    "link_mode": "by_uuid",
                    "name": "AGGREGATED_MOVEMENTS_GROUP_MONTH",
                    "back_ref": "MOVEMENT"
                })
            # end for

            events["aggregated_movements_month"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "AGGREGATED_MOVEMENTS_GROUP_MONTH",
                    "system": "BANCO SANTANDER"
                },
                "start": event.start.isoformat(),
                "stop": event.stop.isoformat(),
                "values": values,
                "links": links
            })

        # end for

        # Iterate through entities
        for entity in movement_entities + ["No entity"]:
            
            movement_events = query.get_events(
                gauge_names = {"filter": "MOVEMENT", "op": "=="},
                start_filters = [{"date": event.stop.isoformat(), "op": "<"}, {"date": event.start.isoformat(), "op": ">="}],
                value_filters = [{"name": {"filter": "entity%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": entity}}])

            if len(movement_events) == 0:
                continue

            movement_events_uuids = [event.event_uuid for event in movement_events]
            
            amounts = [amount.value for event in movement_events for amount in event.eventDoubles if amount.name == "amount"]
            
            amount = sum(amounts)

            values = [
                {"name": "bank",
                 "type": "text",
                 "value": "BANCO SANTANDER"},
                {"name": "entity",
                 "type": "text",
                 "value": entity},
                {"name": "amount",
                 "type": "double",
                 "value": amount}
            ]

            links = []
            for event_uuid in movement_events_uuids:
                links.append({
                    "link": str(event_uuid),
                    "link_mode": "by_uuid",
                    "name": "AGGREGATED_MOVEMENTS_ENTITY_MONTH",
                    "back_ref": "MOVEMENT"
                })
            # end for

            events["aggregated_movements_month"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "AGGREGATED_MOVEMENTS_ENTITY_MONTH",
                    "system": "BANCO SANTANDER"
                },
                "start": event.start.isoformat(),
                "stop": event.stop.isoformat(),
                "values": values,
                "links": links
            })

        # end for

    # end for

    events["aggregated_movements_year"] = []

    update_year_events = query.get_events(
        gauge_names = {"filter": "UPDATE_YEAR", "op": "=="},
        value_filters = [{"name": {"filter": "status", "op": "=="}, "type": "text", "value": {"op": "==", "filter": "UPDATE"}}],
        order_by = {"field": "start", "descending": False})
    
    # Iterate through events
    for event in update_year_events:

        # Iterate through groups
        for group in movement_groups + ["Spending no group", "Income no group"]:
            
            movement_events = query.get_events(
                gauge_names = {"filter": "MOVEMENT", "op": "=="},
                start_filters = [{"date": event.stop.isoformat(), "op": "<"}, {"date": event.start.isoformat(), "op": ">="}],
                value_filters = [{"name": {"filter": "group%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": group}}])
            
            if len(movement_events) == 0:
                continue

            movement_events_uuids = [event.event_uuid for event in movement_events]
            
            amounts = [amount.value for event in movement_events for amount in event.eventDoubles if amount.name == "amount"]
            
            amount = sum(amounts)

            values = [
                {"name": "bank",
                 "type": "text",
                 "value": "BANCO SANTANDER"},
                {"name": "group",
                 "type": "text",
                 "value": group},
                {"name": "amount",
                 "type": "double",
                 "value": amount}
            ]

            links = []
            for event_uuid in movement_events_uuids:
                links.append({
                    "link": str(event_uuid),
                    "link_mode": "by_uuid",
                    "name": "AGGREGATED_MOVEMENTS_GROUP_YEAR",
                    "back_ref": "MOVEMENT"
                })
            # end for

            events["aggregated_movements_year"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "AGGREGATED_MOVEMENTS_GROUP_YEAR",
                    "system": "BANCO SANTANDER"
                },
                "start": event.start.isoformat(),
                "stop": event.stop.isoformat(),
                "values": values,
                "links": links
            })

        # end for

        # Iterate through entities
        for entity in movement_entities + ["No entity"]:
            
            movement_events = query.get_events(
                gauge_names = {"filter": "MOVEMENT", "op": "=="},
                start_filters = [{"date": event.stop.isoformat(), "op": "<"}, {"date": event.start.isoformat(), "op": ">="}],
                value_filters = [{"name": {"filter": "entity%", "op": "like"}, "type": "text", "value": {"op": "==", "filter": entity}}])

            if len(movement_events) == 0:
                continue

            movement_events_uuids = [event.event_uuid for event in movement_events]
            
            amounts = [amount.value for event in movement_events for amount in event.eventDoubles if amount.name == "amount"]
            
            amount = sum(amounts)

            values = [
                {"name": "bank",
                 "type": "text",
                 "value": "BANCO SANTANDER"},
                {"name": "entity",
                 "type": "text",
                 "value": entity},
                {"name": "amount",
                 "type": "double",
                 "value": amount}
            ]

            links = []
            for event_uuid in movement_events_uuids:
                links.append({
                    "link": str(event_uuid),
                    "link_mode": "by_uuid",
                    "name": "AGGREGATED_MOVEMENTS_ENTITY_YEAR",
                    "back_ref": "MOVEMENT"
                })
            # end for

            events["aggregated_movements_year"].append({
                "gauge": {
                    "insertion_type": "INSERT_and_ERASE",
                    "name": "AGGREGATED_MOVEMENTS_ENTITY_YEAR",
                    "system": "BANCO SANTANDER"
                },
                "start": event.start.isoformat(),
                "stop": event.stop.isoformat(),
                "values": values,
                "links": links
            })

        # end for

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
    generation_time = (parser.parse(file_name[19:34]) + datetime.timedelta(microseconds=1)).isoformat()
    reported_validity_start = file_name[35:50]
    reported_validity_stop = file_name[51:66]

    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 20)

    source = {
        "name": file_name,
        "reception_time": reception_time,
        "generation_time": generation_time,
        "reported_validity_start": reported_validity_start,
        "reported_validity_stop": reported_validity_stop
    }
    
    eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 40)

    # Default alert notification time
    notification_time = (parser.parse(source["reported_validity_start"]) - datetime.timedelta(days=1)).isoformat()
    if notification_time > datetime.datetime.now().isoformat():
        notification_time = datetime.datetime.now().isoformat()
    # end if

    # Generate aggregated events
    events = _generate_aggregated_movements_events(parsed_xls, source, engine, query)

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
    
    # Insert aggregated movements events
    if len(events["aggregated_movements_month"]):
        event_starts = [event["start"] for event in events["aggregated_movements_month"]]
        event_starts.sort()
        event_stops = [event["stop"] for event in events["aggregated_movements_month"]]
        event_stops.sort()
    
        source["validity_start"] = event_starts[0]
        source["validity_stop"] = event_stops[-1]
            
        operations.append({
            "mode": "insert_and_erase",
            "dim_signature": {
                "name": "AGGREGATED_MOVEMENTS_MONTH_SANTANDER",
                "exec": os.path.basename(__file__),
                "version": version
            },
            "source": source,
            "events": events["aggregated_movements_month"]
        })

        eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 95)

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
                     "value": "UPDATED"}
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

        eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 96)

        event_starts = [event["start"] for event in events["aggregated_movements_year"]]
        event_starts.sort()
        event_stops = [event["stop"] for event in events["aggregated_movements_year"]]
        event_stops.sort()
    
        source["validity_start"] = event_starts[0]
        source["validity_stop"] = event_stops[-1]
            
        operations.append({
            "mode": "insert_and_erase",
            "dim_signature": {
                "name": "AGGREGATED_MOVEMENTS_YEAR_SANTANDER",
                "exec": os.path.basename(__file__),
                "version": version
            },
            "source": source,
            "events": events["aggregated_movements_year"]
        })

        eboa_ingestion_functions.insert_ingestion_progress(session_progress, general_source_progress, 97)

        events["update_years"] = []
        year_start = parser.parse(event_starts[0][0:7], default=datetime.datetime(2015, 1, 1))
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
                     "value": "UPDATED"}
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

    query.close_session()

    return data
