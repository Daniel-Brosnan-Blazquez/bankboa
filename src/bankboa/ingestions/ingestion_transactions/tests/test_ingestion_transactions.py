"""
Automated tests for the ingestion of the MOVEMENTS files from Santander bank

Written by Daniel Brosnan Blázquez

module bankboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import re
import json

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

# Import ingestion
import eboa.ingestion.eboa_ingestion as ingestion

class TestIngestionTransactions(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query_eboa.close_session()

    # def test_insert_empty_movements(self):
        
    #     filename = "BANKSAN_MOVEMENTS__20230602T000000_20230602T000000_0001.xls"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

    #     exit_status = ingestion.command_process_file("bankboa.ingestions.ingestion_transactions.ingestion_santander_transactions", file_path, "2018-01-01T00:00:00")

    #     assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

    #     # Check number of sources inserted
    #     sources = self.query_eboa.get_sources()

    #     assert len(sources) == 2

    def test_insert_movements(self):
        
        filename = "BANKSAN_MOVEMENTS__20250703T120000_20230602T000000_20250703T000000_0001.xls"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        exit_status = ingestion.command_process_file("bankboa.ingestions.ingestion_transactions.ingestion_santander_transactions", file_path, "2018-01-01T00:00:00")

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        # Check number of sources inserted
        sources = self.query_eboa.get_sources()

        assert len(sources) == 1
