#pragma once

#include <thread>
#include <mutex>
#include <iostream>
#include <ctime>
#include <unistd.h>

#include "influxdb.hpp"
#include "keti_log.h"

using namespace std;

class DB_Monitoring_Manager {
public:
    const std::string LOGTAG = "Query Engine Instance::DB Monitoring Manager";

    static void UpdateSelectCount(){
        GetInstance().updateSelectCount();
    }

    static void UpdateInsertCount(){
        GetInstance().updateInsertCount();
    }

    static void UpdateDeleteCount(){
        GetInstance().updateDeleteCount();
    }

    static void UpdateUpdateCount(){
        GetInstance().updateUpdateCount();
    }

    static void UpdateDDLCount(){
        GetInstance().updateDDLCount();
    }

    static void UpdateDMLCount(){
        GetInstance().updateDMLCount();
    }

    static void UpdateOffloadingCount(){
        GetInstance().updateOffloadingCount();
    }

    static void UpdateClientCount(){
        GetInstance().updateClientCount();
    }

    static void UpdateDiskReadRate(int rate){
        GetInstance().updateDiskReadRate(rate);
    }

    static void UpdateDiskWriteRate(int rate){
        GetInstance().updateDiskWriteRate(rate);
    }

    static void UpdateCacheHitRate(int rate){
        GetInstance().updateCacheHitRate(rate);
    }

    static void UpdateCacheUsageRate(int rate){
        GetInstance().updateCacheUsageRate(rate);
    }

    static void UpdateScanRowCount(int row){
        GetInstance().updateScanRowCount(row);
    }

    static void UpdateFilterRowCount(int row){
        GetInstance().updateFilterRowCount(row);
    }

    static DB_Monitoring_Manager& GetInstance() {
        static DB_Monitoring_Manager _instance;
        return _instance;
    }
    
private:
    DB_Monitoring_Manager() {
        std::thread thd(&DB_Monitoring_Manager::updateInstanceMonitoringDBCycle,this);
        thd.detach();
    };
    DB_Monitoring_Manager(const DB_Monitoring_Manager&);
    DB_Monitoring_Manager& operator=(const DB_Monitoring_Manager&){
        return *this;
    };

    void updateInstanceMonitoringDB(){
        unique_lock<mutex> lock(safe_mutex);

        try {
            influxdb_cpp::server_info server_info("10.0.4.87", 30701, "keti_opencsd", "root", "ketilinux");
            string resp;

            int ret = influxdb_cpp::builder()
                .meas("database_status")
                .field("select_count", select_count_)
                .field("insert_count", insert_count_)
                .field("delete_count", delete_count_)
                .field("update_count", update_count_)
                .field("ddl_count", ddl_count_)
                .field("dml_count", dml_count_)
                .field("dcl_count", dcl_count_)
                .field("offloading_count", offloading_count_)
                .field("client_count", client_count_)
                .field("disk_read_rate", disk_read_rate_)
                .field("disk_write_rate", disk_write_rate_)
                .field("cache_hit_rate", cache_hit_rate_)
                .field("cache_usage_rate", cache_usage_rate_)
                .field("csd_scan_row_count", csd_scan_row_count_)
                .field("csd_filter_row_count", csd_filter_row_count_)
                .post_http(server_info, &resp);

        } catch (std::exception const& e) {
            KETILOG::ERRORLOG("DB Connector Instance::DB Monitoring Manager::SQL Error", e.what());
        }

        // cout << "-------------------------" << endl;
        // cout << "Insert DB Monitorint Info" << endl;
        // cout << select_count_<<" / "<<insert_count_<<" / "<<delete_count_<<" / "<<update_count_ << endl;
        // cout << ddl_count_<<" / "<<dml_count_<<" / "<<dcl_count_<<" / "<<offloading_count_<<" / "<< client_count_ << endl;
        // cout << disk_read_rate_<<" / "<<disk_write_rate_<<" / "<<cache_hit_rate_<<" / "<<cache_usage_rate_ << endl;
        // cout << csd_scan_row_count_<<" / "<<csd_filter_row_count_<< endl;
        // cout << "-------------------------" << endl;
    
        resetMonitoringData();
    }

    void updateInstanceMonitoringDBCycle(){
        while (true) {
            updateInstanceMonitoringDB();
            sleep(30);
        }
    }

    void updateSelectCount(){
        unique_lock<mutex> lock(safe_mutex);
        select_count_++;
        ddl_count_++;
    }

    void updateInsertCount(){
        unique_lock<mutex> lock(safe_mutex);
        insert_count_++;
        ddl_count_++;
    }

    void updateDeleteCount(){
        unique_lock<mutex> lock(safe_mutex);
        delete_count_++;
        ddl_count_++;
    }

    void updateUpdateCount(){
        unique_lock<mutex> lock(safe_mutex);
        update_count_++;
        ddl_count_++;
    }

    void updateDDLCount(){
        unique_lock<mutex> lock(safe_mutex);
        ddl_count_++;
    }

    void updateDMLCount(){
        unique_lock<mutex> lock(safe_mutex);
        dml_count_++;
    }

    void updateOffloadingCount(){
        unique_lock<mutex> lock(safe_mutex);
        offloading_count_++;
    }

    void updateClientCount(){
        unique_lock<mutex> lock(safe_mutex);
        client_count_++;
    }

    void updateDiskReadRate(int rate){
        unique_lock<mutex> lock(safe_mutex);
        disk_read_rate_ = rate;
    }

    void updateDiskWriteRate(int rate){
        unique_lock<mutex> lock(safe_mutex);
        disk_write_rate_ = rate;
    }

    void updateCacheHitRate(int rate){
        unique_lock<mutex> lock(safe_mutex);
        cache_hit_rate_ = rate;
    }

    void updateCacheUsageRate(int rate){
        unique_lock<mutex> lock(safe_mutex);
        cache_usage_rate_ = rate;
    }

    void updateScanRowCount(int row){
        unique_lock<mutex> lock(safe_mutex);
        csd_scan_row_count_ += row;
    }

    void updateFilterRowCount(int row){
        unique_lock<mutex> lock(safe_mutex);
        csd_filter_row_count_ += row;
    }

    void resetMonitoringData(){
        select_count_ = 0;
        insert_count_ = 0;
        delete_count_ = 0;
        update_count_ = 0;
        ddl_count_ = 0;
        dml_count_ = 0;
        dcl_count_ = 0;
        offloading_count_ = 0;
        client_count_ = 0;
        disk_read_rate_ = 0;
        disk_write_rate_ = 0;
        cache_hit_rate_ = 0;
        cache_usage_rate_ = 0;
        csd_scan_row_count_ = 0;
        csd_filter_row_count_ = 0;
    }

    mutex safe_mutex;

    int select_count_;
    int insert_count_;
    int delete_count_;
    int update_count_;
    int ddl_count_;
    int dml_count_;
    int dcl_count_;
    int offloading_count_;
    int client_count_;
    int disk_read_rate_;
    int disk_write_rate_;
    int cache_hit_rate_;
    int cache_usage_rate_;
    int csd_scan_row_count_;
    int csd_filter_row_count_;
};