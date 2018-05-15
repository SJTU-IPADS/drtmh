// This module will initiliaze the views of the test environment,
// by parsing the config file

/*
 *   Format of the configuration file:
 *   N ( how many machine in the cluster)
 *   ip0
 *   ...
 *   ipN
 *   P ( how many partitions for the test)
 *   0 1 ( 0 partition  is on machine 0 with backup @ machine 1)
 */


#ifndef NOCC_FRAMEWORK_VIEW_MANAGER_H_
#define NOCC_FRAMEWORK_VIEW_MANAGER_H_

#include <string>
#include <vector>
#include <deque>

#include <set>

#define MAX_PRIMARY_NUM 1
#define MAX_BACKUP_NUM 2

#define MAX_REPO_NUM 3

namespace nocc {
  namespace oltp {

    enum {
      MAC_PAR,
      MAC_BACK,
      MAC_NONE
    };

    enum {
      VIEW_RUN,   // normal case
      VIEW_HALT,  // when backup receive new-config change
      VIEW_REC,   // when primary and backup start to recovery,after this view
      VIEW_ERR,   // err state during re-configuration?
    };

    struct PartitionInfo {
      int primary;
      int primary_mac_index;
      int backups[MAX_BACKUP_NUM];
      int num_backups;

      int& operator[](std::size_t index) {
        return backups[index];
      }
    };

    struct MacInfo {
      int id;
      int capcity;
      bool alive;
      int num_primaries;
      int num_backups;
      int partitions[MAX_REPO_NUM];

      MacInfo(int id, int capcity){
        this->id = id;
        this->capcity = capcity;
        this->num_primaries = 0;
        this->num_backups = 0;
      }
      int& operator[](std::size_t index) {
        return partitions[index];
      }
    };


    class View {
    public:
      View(std::string config,std::vector<std::string> &network);
      View(View *view); // make a copy of the view

      int num_partitions_, rep_factor_;  
      std::vector<PartitionInfo> partitions_;
      std::vector<MacInfo> macs_;

      std::vector<std::string> net_def_;
      
    private:
      void arrange_primary(std::deque<int>& machines);
      void arrange_backup(std::deque<int>& machines);

    public:

      /*
      * Print current view in a user visable form
      */
      void print_view();

      // query the backup shards i am responsible for,return the backup number
      int is_backup(int mac_id,int *backups = NULL);

      // query the primaries shars i am responsible for,return the backup number
      int is_primary(int mac_id,int *primaries = NULL);

      // query whether a machine is responsible for some job in at a partition
      bool response(int mac_id,int p_id) {
        
        if(mac_id == (partitions_[p_id].primary))
          return true;
        for(uint b_i = 0; b_i < partitions_[p_id].num_backups; b_i++) {
          if(mac_id == (partitions_[p_id][b_i]))
            return true;
        }
        return false;
      }

      // add the corresponding backups for a partition to a set
      inline void add_backup(int p_id,std::set<int> &backs) {
        for(uint b_i = 0; b_i < partitions_[p_id].num_backups; b_i++) {
          backs.insert(partitions_[p_id][b_i]);
        }    
      }

      // return the primary mac id for a partition
      inline int partition_to_mac(int p_id) {
        return partitions_[p_id].primary;
      }
    };
  } // oltp
} // nocc

#endif
