## Introduction of KETI-OpenCSD KETI-DB-Connector-Instance
-------------

KETI-DB-Connector-Instance analyzes queries and generates snippets to pushdown queries to CSD.

<img src=https://github.com/opencsd/KETI-DB-Connector-Instance/assets/100827738/395c5822-4b8a-4f79-b461-71eea4ff2658 width="50%" height="50%">


## Contents
-------------
[1. Requirement](#1.-Requirement)

[2. How To Install](#2.-How-To-Build)

[3. Modules](#modules)

[4. Governance](#governance)

## 1. Requirement
-------------
>   gcc-11

>   g++-11

>   gRPC

>   cpprestSDK

>   RapidJSON

>   ODBC


## 2. How To Build
1. Install gcc-11 & g++-11
```bash
add-apt-repository ppa:ubuntu-toolchain-r/test
apt-get update
apt-get install gcc-11 g++-11
ln /usr/bin/gcc-11 /usr/bin/gcc
ln /usr/bin/g++-11 /usr/bin/g++
```

2. Install gRPC
```bash
apt install -y cmake
apt install -y build-essential autoconf libtool pkg-config
git clone --recurse-submodules -b v1.46.3 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make –j
make install
cd ../..
```

3. Install cpprestSDK
```bash
apt-get install libcpprest-dev
```

4. Install RapidJSON
```bash
apt-get install -y rapidjson-dev
```

5. Install ODBC
```bash
wget http://www.unixodbc.org/unixODBC-2.3.7.tar.gz
tar xvzf unixODBC-2.3.7.tar.gz
cd unixODBC-2.3.7/
./configure --prefix=/
make -j
make install
```

6. Clone KETI-DB-Connector-Instance
```bash
git clone https://github.com/opencsd/KETI-DB-Connector-Instance.git
cd KETI-DB-Connector-Instance/cmake/build/
```

7. Build
```bash
cmake ../..
make -j
```

8. Run DB-Connector-Instance
```bash
./db_connector_instance
```

## gRPC Protobuf
-------------
### SnippetRequest
-------------
```protobuf
message SnippetRequest {
  int32 type = 1;
  Snippet snippet = 2;
}
```

### Snippet
-------------
```protobuf
message Snippet {
  int32 query_ID = 1;
  int32 work_ID = 2;
  repeated string table_name = 3;
  repeated string table_col = 4;
  repeated Filter table_filter = 5;
  Dependency dependency = 6;
  repeated int32 table_offset = 7;
  repeated int32 table_offlen = 8;
  repeated int32 table_datatype = 9;
  string table_alias = 10;
  repeated string column_alias = 11;
  repeated Projection column_projection = 12;
  repeated string column_filtering = 13;
  repeated string group_by = 14;
  Order order_by = 15;
  int32 limit = 16;
}
```

## REST Server(DB_Connector_Instance.h DB_Connector_Instance.cpp)
-------------

## gRPC Client(Storage_Engine_Interface.h)
-------------
### OpenStream
-------------
* The start point of gRPC
* Open Stream for repeated data transmission

### SendSnippet
-------------
* Snippet sending method
* Called repeatedly to perform a query

### CloseStream
-------------
* The end point of gRPC
* Close Stream

## gRPC Server(storage_engine_instance.cc)
-------------
### SetSnippet
-------------
* Receive Snippet and convert to JSON

## Governance
-------------
This work was supported by Institute of Information & communications Technology Planning & Evaluation (IITP) grant funded by the Korea government(MSIT) (No.2021-0-00862, Development of DBMS storage engine technology to minimize massive data movement)
