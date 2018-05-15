## usage: ./sync.sh  config_file_name destination node

scp ./$1.cfg $2:~/
scp ./$1 $2:~/

