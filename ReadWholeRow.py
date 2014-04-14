#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from accumulo import AccumuloProxy
from accumulo.ttypes import *
import StringIO, struct

def main():
    transport = TSocket.TSocket('localhost', 42424)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = AccumuloProxy.Client(protocol)
    transport.open()

    login = client.login('root', {'password':'secret'})

    testtable = "records"
    if not client.tableExists(login, testtable):
        return

    options = ScanOptions("", Range(Key("1"), True, Key("2\x00"), False), [], [IteratorSetting(50, 'wholerow',
        'org.apache.accumulo.core.iterators.WholeRowIterator', {})], None)
    cookie = client.createScanner(login, testtable, options)
    for entry in client.nextK(cookie, 10).results:
        #print entry
        decode_row(entry)

def decode_row(cell):
    value = StringIO.StringIO(cell.value)
    numCells = struct.unpack('!i',value.read(4))[0]
    columns = []
    for i in range(numCells):
        if value.pos == value.len:
            raise Exception(
                    'Reached the end of the parsable string without'
                    ' having finished unpacking. Likely an error'
                    ' of passing a cell that is not from a'
                    ' WholeRowIterator.'
                    )
        cf = value.read(struct.unpack('!i',value.read(4))[0])
        cq = value.read(struct.unpack('!i',value.read(4))[0])
        cv = value.read(struct.unpack('!i',value.read(4))[0])
        ts = struct.unpack('!q',value.read(8))[0]/1000.
        val = value.read(struct.unpack('!i',value.read(4))[0])
        columns.append("%s:%s [%s] %d => %s" % (cf, cq, cv, ts, val))
    print "%s => %s" % (cell.key.row, columns)


if  __name__ == '__main__':
    main()
