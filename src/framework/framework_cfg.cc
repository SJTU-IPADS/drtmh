// This file contains the parsing of the framework


#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <boost/algorithm/string.hpp>

#include <map>
#include <vector>
#include <string>

namespace nocc {

  namespace oltp {

    // return a network with @num@ servers given a host file
    std::vector<std::string> parse_network(int num, std::string &hosts) {

      std::vector<std::string> network;
      std::map<std::string, bool> black_list;
      network.clear();
      black_list.clear();

      using boost::property_tree::ptree;
      using namespace boost;
      using namespace property_tree;
      ptree pt;

      try {
        read_xml(hosts, pt);
      } catch (const ptree_error &e) {
        fprintf(stderr,"Failed to parse host file!");
        assert(false);
      }

      try {
        // parse the black list of hosts
        BOOST_FOREACH(ptree::value_type &v,
                      pt.get_child("hosts.black"))
          {
            std::string  s = v.second.data();
            boost::algorithm::trim_right(s);
            boost::algorithm::trim_left(s);

            if(black_list.find(s) == black_list.end())
              black_list.insert(std::make_pair(s,true));
          }
      }
      catch(const ptree_error &e) {
        // pass, no black list provided
      }
      // parse the hosts file
      try {

        // parse hosts
        BOOST_FOREACH(ptree::value_type &v,
                      pt.get_child("hosts.macs"))
          {
            std::string  s = v.second.data();
            boost::algorithm::trim_right(s);
            boost::algorithm::trim_left(s);
            if(black_list.find(s) != black_list.end()) {

            } else {
              if(network.size() + 1 > num)
                break;
              network.push_back(s);
            }
          } // end iterating hosts

      } catch (const ptree_error &e) {
        assert(false);
      }

      assert(network.size() == num);
      return network;
    }

  }; // namespace oltp

}; // namespace nocc
