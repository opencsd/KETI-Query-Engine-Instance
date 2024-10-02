#ifndef LOG_MSG_H_INCLUDED
#define LOG_MSG_H_INCLUDED

#pragma once

#include <iostream>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <sys/msg.h>
#include <stdlib.h>
#include <string.h>

#define MSQID 12345
#define MSGMAAX 4096 //test

struct log_data {
    char msg[MSGMAAX];
};

struct message {
    long msg_type;
    log_data data;
};

typedef enum DEBUGG_LEVEL {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    WARN = 3,
    ERROR = 4,
    FATAL = 5
}KETI_DEBUGG_LEVEL;

class KETILOG {
    public:
        static void SetDefaultLogLevel(){
            GetInstance().LOG_LEVEL = INFO;
        }
        
        static void SetLogLevel(int level){
            GetInstance().LOG_LEVEL = level;
        }

        static void TRACELOG(std::string id, std::string msg){
        //     key_t key = MSQID;
        //     int msqid;
        //     std::string log_str;
        // retry:
        //     message mess;    
        //     mess.msg_type=1;
        //     log_str = "[" + id + "] " + msg;
        //     memcpy(mess.data.msg,log_str.c_str(),MSGMAAX);

        //     if((msqid=msgget(key,IPC_CREAT|0666))==-1){
        //         printf("msgget failed %s\n",strerror(errno));
        //         goto retry;
        //     }
            
        //     if(msgsnd(msqid,&mess,sizeof(log_data),0)==-1){
        //         printf("msgsnd failed %s\n",strerror(errno));
        //         goto retry;
        //     }
            if(GetInstance().LOG_LEVEL <= TRACE){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static void DEBUGLOG(std::string id, std::string msg){
            if(GetInstance().LOG_LEVEL <= DEBUG){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static void INFOLOG(std::string id, std::string msg){
            if(GetInstance().LOG_LEVEL <= INFO){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static void WARNLOG(std::string id, std::string msg){
            if(GetInstance().LOG_LEVEL <= WARN){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static void ERRORLOG(std::string id, std::string msg){
            if(GetInstance().LOG_LEVEL <= ERROR){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static void FATALLOG(std::string id, std::string msg){
            if(GetInstance().LOG_LEVEL <= FATAL){
                std::cout << "[" << id << "] " << msg << std::endl;
            }
        }

        static bool IsLogLevelUnder(int level){
            bool flag = (GetInstance().LOG_LEVEL <= level);
            return flag;
        }

    private:
        KETILOG(){};
        KETILOG(const KETILOG&);
        KETILOG& operator=(const KETILOG&){
            return *this;
        }

        static KETILOG& GetInstance(){
            static KETILOG _instance;
            return _instance;
        }

        int LOG_LEVEL;
};

#endif // LOG_MSG_H_INCLUDED