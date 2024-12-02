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
using StorageEngineInstance::SnippetRequest;
using StorageEngineInstance::QueryStringResult;
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
				iter->set_query_id(query_id);//set query id
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

	private:
		std::unique_ptr<StorageEngineInterface::Stub> stub_;
		std::unique_ptr<grpc::ClientWriter<SnippetRequest>> stream;
		std::unique_ptr<ClientContext> streamcontext;
		const std::string LOGTAG = "Query Engine";
};

