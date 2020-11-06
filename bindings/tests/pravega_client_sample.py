#!/usr/bin/env python3
import asyncio
import logging
logging.getLogger().setLevel(logging.DEBUG) # Set debugging level
FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(filename="pravega_debug.log", level=logging.DEBUG, filemode="w+", format=FORMAT) # Write logs to a file called "log.txt"
logging.info("") # Needed to initialize logging
import pravega_client

# FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
# logging.basicConfig(format=FORMAT)
# logging.getLogger().setLevel(logging.INFO)
logging.info("Logging from Python")
stream_manager=pravega_client.StreamManager("127.0.0.1:9090")
stream_manager.create_scope("sc1")
