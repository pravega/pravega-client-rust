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
from pravega_client import StreamScalingPolicy
from pravega_client import StreamRetentionPolicy

class PravegaTest(unittest.TestCase):

    def test_tags(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                        for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("tcp://127.0.0.1:9090", False, False)

        print("Creating a scope")
        scope_result=stream_manager.create_scope(scope)
        self.assertEqual(True, scope_result, "Scope creation status")
        print("Creating a stream")
        # stream is created using a fixed scaling policy.
        stream_result=stream_manager.create_stream(scope, "testStream", 1)
        self.assertTrue(stream_result, "Stream creation status")
        tags = stream_manager.get_stream_tags(scope, "testStream")
        # verify empty tags.
        self.assertTrue(len(tags)==0)
        stream_update=stream_manager.update_stream_with_policy(scope_name=scope, stream_name="testStream", tags=["t1"])
        self.assertTrue(stream_update, "Stream update status")
        tags = stream_manager.get_stream_tags(scope, "testStream")
        self.assertEqual(["t1"], tags)

        # create a stream with stream scaling is enabled with data rate as 10kbps, scaling factor as 2 and initial segments as 1
        policy = StreamScalingPolicy.auto_scaling_policy_by_data_rate(10, 2, 1)
        stream_result=stream_manager.create_stream_with_policy(scope_name=scope, stream_name="testStream1", scaling_policy=policy)
        self.assertTrue(stream_result, "Stream creation status")
        # add tags
        stream_update=stream_manager.update_stream_with_policy(scope_name=scope, stream_name="testStream1", scaling_policy=policy, tags=['t1', 't2'])
        self.assertTrue(stream_update, "Stream update status")
        tags = stream_manager.get_stream_tags(scope, "testStream1")
        self.assertEqual(['t1', 't2'], tags)

        # update retention policy
        # retention policy of 10GB
        retention = StreamRetentionPolicy.by_size(10*1024*1024 * 1024)
        stream_update=stream_manager.update_stream_with_policy(scope, "testStream1", policy, retention, tags=["t4", "t5"])
        self.assertTrue(stream_update, "Stream update status")
        tags = stream_manager.get_stream_tags(scope, "testStream1")
        self.assertEqual(["t4", "t5"], tags)

    def test_writeEvent(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                      for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("tcp://127.0.0.1:9090", False, False)

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

    def test_byteStream(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                        for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("tcp://127.0.0.1:9090", False, False)

        print("Creating a scope")
        scope_result=stream_manager.create_scope(scope)
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream(scope, "testStream", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        # write and read data.
        print("Creating a writer for Stream")
        bs=stream_manager.create_byte_stream(scope,"testStream")
        self.assertEqual(5, bs.write(b"bytes"))
        bs.flush()
        self.assertEqual(5, bs.current_tail_offset())
        buf=bytearray(5)
        self.assertEqual(5, bs.readinto(buf))

        # fetch the current read offset.
        current_offset=bs.tell()
        self.assertEqual(5, current_offset)

        # seek to a given offset and read
        bs.seek(3, 0)
        buf=bytearray(2)
        self.assertEqual(2, bs.readinto(buf))

        bs.truncate(2)
        self.assertEqual(2, bs.current_head_offset())

    def test_writeTxn(self):
        scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                        for i in range(10))
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")

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
        stream_manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")

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
