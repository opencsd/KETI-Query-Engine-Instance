#pragma once
#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"

#include "keti_log.h"
#include "meta_data_manager.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReaderWriter;
using StorageEngineInstance::StorageEngineInterface;
using StorageEngineInstance::Snippet;
using StorageEngineInstance::SnippetRequest;
using StorageEngineInstance::QueryStringResult;
using StorageEngineInstance::DBInfo;
using StorageEngineInstance::DBInfo_DB;
using StorageEngineInstance::DBInfo_DB_Table;
using StorageEngineInstance::Request;
using StorageEngineInstance::Response;
using google::protobuf::Empty;

class StorageEngineConnector {
	public:
		StorageEngineConnector(){}
		StorageEngineConnector(std::shared_ptr<Channel> channel) : stub_(StorageEngineInterface::NewStub(channel)) {}
		
		QueryStringResult OffloadingQuery(std::list<SnippetRequest> &snippet_list, int query_id){
			QueryStringResult result;

			streamcontext.reset(new ClientContext());
			stream = stub_->OffloadingQueryInterface(streamcontext.get(), &result);

			for(list<SnippetRequest>::iterator iter = snippet_list.begin(); iter != snippet_list.end(); iter++){
				iter->mutable_snippet()->set_query_id(query_id);//set query id
				stream->Write(*iter);
			}
			
			stream->WritesDone();
			Status status = stream->Finish();
			
			if (!status.ok()) {
				KETILOG::FATALLOG(LOGTAG,status.error_code() + ": " + status.error_message());
				KETILOG::FATALLOG(LOGTAG,"RPC failed");
			}

			return result;
		}

		void SyncMetaDataManager() {
			while(1){
				DBInfo dbInfo;
				Response response;
				ClientContext context;

				unordered_map<string, MetaDataManager::DB> metaData = MetaDataManager::GetMetaData();

				for(const auto db_ : metaData){
					DBInfo_DB db;
					for(const auto table_ : db_.second.table_list){
						DBInfo_DB_Table table;
						table.set_table_index_number(table_.second.table_index_number);
						for(const auto sst_ : table_.second.sst_list){
							table.add_sst_list(sst_.second.sst_name);
						}
						db.mutable_table_list()->insert({table_.first, table});
					}
					dbInfo.mutable_db_list()->insert({db_.first, db});
				}	
				
				Status status = stub_->SyncMetaDataManager(&context, dbInfo, &response);

				if (status.ok()) {
					KETILOG::INFOLOG(LOGTAG,"metadata sync success");
					break;
				}else{
					KETILOG::TRACELOG(LOGTAG,"there is no storage engine");
					sleep(2);
				}
			}
			
			return;
		}

	private:
		std::unique_ptr<StorageEngineInterface::Stub> stub_;
		std::unique_ptr<grpc::ClientWriter<SnippetRequest>> stream;
		std::unique_ptr<ClientContext> streamcontext;
		const std::string LOGTAG = "Query Engine";
};

