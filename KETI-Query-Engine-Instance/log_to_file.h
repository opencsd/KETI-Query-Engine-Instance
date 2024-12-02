#pragma once

#include <iostream>
#include <fstream>
#include <string>

void logToFile(const std::string& message){
    std::string logFilePath = "/root/KTL-Script/Script/log.txt";

    std::ofstream logFile(logFilePath, std::ios::app);

    if(logFile.is_open()){
        logFile << message << std::endl;
        logFile.close();
    }else{
        std::cerr << "Unable to open log file." << std::endl;
    }
}