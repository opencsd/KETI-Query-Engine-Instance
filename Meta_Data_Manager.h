#pragma once

class Meta_Data_Manager {
public:
    Meta_Data_Manager(std::string sync_address){
        sync_address_ = sync_address;
    }
    
    bool Sync();

private:
    std::string sync_address_;
};