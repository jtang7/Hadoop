#!/usr/bin/env python
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from hadoop.io.SequenceFile import CompressionType
from hadoop.io import BytesWritable,Text
from hadoop.io import SequenceFile

def writeData(writer, filename, data):
    key = Text()
    value = BytesWritable()

    key.set(filename)
    value.set(data)
    writer.append(key, value)

def mergeFiles(seq_file_name, directory, suffix):
    writer = SequenceFile.createWriter(seq_file_name, Text, BytesWritable)
    for filename in os.listdir(directory):
        if filename.endswith(suffix):
            f = open(os.path.join(directory, filename), 'rb')
            data = f.read()
            writeData(writer, filename, data)
    writer.close()

if __name__ == '__main__':
    mergeFiles('zipfiles.seq', '/Users/jietang/Downloads/D33', '.zip')
