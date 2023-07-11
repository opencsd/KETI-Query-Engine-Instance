#pragma once
#include <grpcpp/grpcpp.h>
#include <google/protobuf/empty.pb.h>
#include "snippet_sample.grpc.pb.h"
#include "keti_log.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using StorageEngineInstance::InterfaceContainer;
using StorageEngineInstance::Snippet;
using StorageEngineInstance::SnippetRequest;
using StorageEngineInstance::Result;
using StorageEngineInstance::Request;
using google::protobuf::Empty;

class Storage_Engine_Interface {
	public:
		Storage_Engine_Interface(std::shared_ptr<Channel> channel) : stub_(InterfaceContainer::NewStub(channel)) {}

		void OpenStream(){
			streamcontext.reset(new ClientContext());
			stream = stub_->SetSnippet(streamcontext.get());
		}
		void SendSnippet(const SnippetRequest &snippet) {
      		stream->Write(snippet);
			KETILOG::DEBUGLOG("Storage Engine Interface","Send Snippet (WorkID : " + std::to_string(snippet.snippet().work_id()) + ") to Storage Engine Instance");
		}
		void GetReturn(){		
			Result result;		
			stream->Read(&result);
		}
		void CloseStream(){
			stream->WritesDone();
			Status status = stream->Finish();
			
			if (!status.ok()) {
				KETILOG::FATALLOG(LOGTAG,status.error_code() + ": " + status.error_message());
				KETILOG::FATALLOG(LOGTAG,"RPC failed");
			}
		}

		std::string Run(int queryid) {
			Request request;
			request.set_query_id(queryid);
    		ClientContext context;
			Result result;
			
			Status status = stub_->Run(&context, request, &result);

			KETILOG::DEBUGLOG("Storage Engine Interface","Query Result : \n" + result.value());
			
	  		if (!status.ok()) {
				KETILOG::FATALLOG(LOGTAG,status.error_code() + ": " + status.error_message());
				KETILOG::FATALLOG(LOGTAG,"RPC failed");
			}
			return result.value();
		}

	private:
		std::unique_ptr<InterfaceContainer::Stub> stub_;
		std::unique_ptr<grpc::ClientReaderWriter<StorageEngineInstance::SnippetRequest, StorageEngineInstance::Result>> stream;
		std::unique_ptr<ClientContext> streamcontext;
		inline const static std::string LOGTAG = "DB Connector Instance::Storage Engine Interface";
};
