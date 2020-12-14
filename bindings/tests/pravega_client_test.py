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
import secrets
import string
import pravega_client
from pravega_client import TxnFailedException

class PravegaTest(unittest.TestCase):

    def test_writeEvent(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                      for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope(scope)
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream(scope, "testStream", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a writer for Stream")
        w1=stream_manager.create_writer(scope,"testStream")

        print("Write events")
        w1.write_event("test event1")
        w1.write_event("test event2")

    def test_writeTxn(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                        for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope(scope)
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream(scope, "testTxn", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a txn writer for Stream")
        w1=stream_manager.create_transaction_writer(scope,"testTxn", 1)
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

    def test_TxnError(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                        for i in range(10))

        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope(scope)
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream(scope, "testTxn", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a txn writer for Stream")
        w1=stream_manager.create_transaction_writer(scope,"testTxn", 1)
        txn1 = w1.begin_txn()
        print("Write events")
        txn1.write_event("test event1")
        txn1.write_event("test event2")
        self.assertTrue(txn1.is_open(), "Transaction is open")
        print("commit transaction")
        txn1.commit()
        self.assertEqual(False, txn1.is_open(), "Transaction is closed")

        #Attempt writing to an already commited transaction.
        try:
            txn1.write_event("Error")
            self.fail("Write on an already closed transaction should throw a TxnFailedException")
        except TxnFailedException as e:
            print("Exception ", e)

        #Attempt committing an closed transaction.
        try:
            txn1.commit()
            self.fail("Commit of an already closed transaction should throw a TxnFailedException")
        except TxnFailedException as e:
            print("Exception ", e)

        #Attempt aborting an closed transaction.
        try:
            txn1.abort()
            self.fail("Abort of an already closed transaction should throw a TxnFailedException")
        except TxnFailedException as e:
            print("Exception ", e)

if __name__ == '__main__':
    unittest.main()
