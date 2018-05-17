#include "view_manager.h"

#include <stdio.h>
#include <assert.h>

#include <sstream>
#include <fstream>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/algorithm/string.hpp>

#include "util/util.h"
#include "util/printer.h"

using namespace nocc::util;

namespace nocc {
  
  namespace oltp {

    std::deque<int> free_machines;
    int rep_factor;

    View::View(std::string config,std::vector<std::string> &network)
      :net_def_(network)
    {

      using boost::property_tree::ptree;
      using namespace boost;
      using namespace property_tree;

      Debugger::debug_fprintf(stdout,"[View] using config file %s\n",config.c_str());
      ptree pt;
      read_xml(config, pt);

      // assume that the number of backups is the same as the network
      num_partitions_ = net_def_.size();
      try{
        rep_factor_ = pt.get<int>("bench.rep-factor");
      } catch (const ptree_error &e) {
        rep_factor_ = 2;
      }
      assert(rep_factor_ <= MAX_BACKUP_NUM);
      rep_factor = rep_factor_; // update global rep factor

      if((num_partitions_ * rep_factor_ ) > MAX_REPO_NUM * net_def_.size()) {
        fprintf(stderr,"[View] there are not enough machine to handle %d shards with %d backups\n",
          num_partitions_,rep_factor_);
        exit(-1);
      }

      // view initalization
      for(uint i = 0;i < num_partitions_;++i) {
        partitions_.push_back(PartitionInfo());
        partitions_[i].primary = -1;
        partitions_[i].num_backups = 0;
      }
      // node initalization
      for (uint i = 0;i < net_def_.size();++i) {
        macs_.push_back(MacInfo(i,MAX_REPO_NUM));
        macs_[i].alive = true;
        free_machines.push_back(i);
      }

      // start allocating the primary/backup jobs to machines
      // first allocate primaries, which ensures primaries are at different machines
      arrange_primary(free_machines);
      arrange_backup(free_machines);

      // end valid open configure file
    }

    void View::arrange_primary(std::deque<int>& machines){

      for(uint p_id = 0; p_id < num_partitions_; ++p_id) {
        int mac_id = machines.front();      
        machines.pop_front();

        int primaries[MAX_PRIMARY_NUM],num_p;
        
        if((num_p = is_primary(mac_id,primaries)) == MAX_PRIMARY_NUM) {
          // ignore this machine,find another one
          machines.push_back(mac_id);
        } else {
          PartitionInfo& partition_info = partitions_[p_id];
          partition_info.primary = mac_id;
          
          MacInfo& mac_info = macs_[mac_id];
          mac_info.capcity -= 1;
          mac_info[mac_info.num_primaries++] = p_id;

          if(macs_[mac_id].capcity > 0)
            machines.push_back(mac_id);

          partition_info.primary_mac_index = num_p == 0 ? 0 : 1;
        }
      }
    }

    void View::arrange_backup(std::deque<int>& machines){

      for(uint rep_num = 1;rep_num <= rep_factor_;rep_num++) {
        for(uint p_id = 0; p_id < num_partitions_; p_id++) {

          PartitionInfo& partition_info = partitions_[p_id];
          int mac_id = (partition_info.primary + rep_num) % net_def_.size();
          if(response(mac_id,p_id))continue;
          
          // uint num_machines = machines.size(),counter(0); 
          // int mac_id = machines.front();

          // // find the first free machine which is not the primary
          // for(;counter < num_machines; counter++) {
          //   machines.pop_front();
            
          //   if(response(mac_id,p_id)) { 
          //     // ignore the one with the same id as primiary
          //     machines.push_back(mac_id);      
          //   } else {
          //     break;
          //   }
          //   mac_id = machines.front();
          // }
          // // not found
          // if(counter == num_machines)
          //   continue;

          partition_info[partition_info.num_backups++] = mac_id;

          MacInfo& mac_info = macs_[mac_id];
          mac_info.capcity -= 1;
          int num_partitions = mac_info.num_primaries + mac_info.num_backups++;
          mac_info[num_partitions] = p_id;

          // if(macs_[mac_id].capcity > 0)
            // free_machines.push_back(mac_id);  
        }
      }
    }

    View::View(View *v) {

      // Here we assume that net_def_ is not changed,
      // So there is no machine addition?
      // ma~
      this->num_partitions_ = v->num_partitions_;
      for(uint i = 0;i < v->net_def_.size();++i) {
        this->net_def_.push_back(v->net_def_[i]);
      }
      
      this->partitions_ = std::vector<PartitionInfo>(v->partitions_);
      this->macs_ = std::vector<MacInfo>(v->macs_);
    }


    void View::print_view() {

      fprintf(stdout,"\n**** view ****\n\n");
      
      if (net_def_.size() == 0) {
        fprintf(stderr,"The view has not been initilized.\n");
        return;
      }

      fprintf(stdout,"All life machine in the cluster: \n");
      for(uint mac_id = 0; mac_id < net_def_.size(); mac_id++) {
        if(macs_[mac_id].alive){
          MacInfo& mac_info = macs_[mac_id];
          fprintf(stdout,"%s ",net_def_[mac_id].c_str());

          fprintf(stdout,"capcity: %d, num_primaries: %d, num_backups: %d ",mac_info.capcity,
            mac_info.num_primaries, mac_info.num_backups);

          int num_partitions = mac_info.num_primaries + mac_info.num_backups;
          fprintf(stdout, "partitions: ");
          for(uint i = 0; i < num_partitions; i++){
            fprintf(stdout, "%d ",mac_info[i]);
          }
          fprintf(stdout, "\n");
        }
      }
      fprintf(stdout,"\n");

      fprintf(stdout,"There are %d partitions: \n", num_partitions_);
      for(uint p_id = 0; p_id < num_partitions_; p_id++) {
        if(partitions_[p_id].num_backups == 0) {
          fprintf(stdout,"part %d at %d,no backup.\n",p_id,partitions_[p_id].primary);
          continue;
        }
        fprintf(stdout,"part %d at %d,index %d: backed by ",p_id,partitions_[p_id].primary, 
          partitions_[p_id].primary_mac_index);
        assert(partitions_[p_id].num_backups <= MAX_BACKUP_NUM);
        for(uint j = 0;j < partitions_[p_id].num_backups;++j) {
          fprintf(stdout,"%d ",partitions_[p_id][j]);
        }
        fprintf(stdout,"\n");
      }
      
      fprintf(stdout,"\n**** view ****\n\n");
    }

    int View::is_backup(int mac_id,int *backups) {
      // if(mac_id == 0){
      //   if(backups)backups[0]==0;
      //   return 1;
      // }
      int res = 0;

      MacInfo& mac_info = macs_[mac_id];
      int num_partitions = mac_info.num_primaries + mac_info.num_backups;
      for(uint i = 0; i < num_partitions; i++){
        uint p_id = mac_info[i];
        if(partitions_[p_id].primary != mac_id){
          if(backups)
            backups[res++] = p_id;
          else
            res++;
        }
      }

      return res;
    }

    int View::is_primary(int mac_id,int *primaries) {

      int res = 0;

      MacInfo& mac_info = macs_[mac_id];
      int num_partitions = mac_info.num_primaries + mac_info.num_backups;
      for(uint i = 0; i < num_partitions; i++){
        uint p_id = mac_info[i];
        if(partitions_[p_id].primary == mac_id){
          if(primaries)
            primaries[res++] = p_id;
          else
            res++;
        }
      }

      return res;
    }



  }
}
