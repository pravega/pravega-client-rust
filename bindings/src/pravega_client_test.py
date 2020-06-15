#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#

import unittest
import pravega_client;

class PravegaTest(unittest.TestCase):
    def test_writeEvent(self):
        print("Creating a Stream Manager, ensure pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope("testScope")
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream("testScope", "testStream", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a writer for Stream")
        w1=stream_manager.create_writer("testScope","testStream")

        print("Write events")
        w1.write_event("test event1")
        w1.write_event("test event2")

    def test_writeTxn(self):
        print("Creating a Stream Manager, ensure pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope("testScope1")
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream("testScope1", "testTxn", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a txn writer for Stream")
        w1=stream_manager.create_transaction_writer("testScope1","testTxn", 1)
        txn1 = w1.begin_txn()
        print("Write events")
        txn1.write_event("test event1")
        txn1.write_event("test event2")
        self.assertTrue(txn1.is_open(), "Transaction is open")
        print("commit transaction")
        txn1.commit()
        self.assertEqual(False, txn1.is_open(), "Transaction is closed")

        txn2 = w1.begin_txn()
        print("Write events")
        txn2.write_event("test event1")
        txn2.write_event("test event2")
        self.assertTrue(txn2.is_open(), "Transaction is open")
        print("commit transaction")
        txn2.abort()
        self.assertEqual(False, txn2.is_open(), "Transaction is closed")


if __name__ == '__main__':
    unittest.main()
