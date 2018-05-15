# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}

export HRD_REGISTRY_IP="192.168.15.100"
export MLX5_SINGLE_THREADED=1

if [ "$#" -ne 2 ]; then
    blue "Illegal number of parameters"
	blue "Usage: ./run-machine.sh <machine_number>"
	exit
fi

blue "Removing hugepages"
#shm-rm.sh 1>/dev/null 2>/dev/null

num_threads=1			# Threads per client machine
: ${HRD_REGISTRY_IP:?"Need to set HRD_REGISTRY_IP non-empty"}

blue "Running $num_threads client threads"

LD_LIBRARY_PATH=/usr/local/lib/:/home/dongzy/libs/boost:/home/dongzy/libs/zeromq/lib  \
	numactl --cpunodebind=0 --membind=0 ./testframework \
	--num-threads $2 \
	--dual-port 0 \
	--use-uc 0 \
	--is-client 1 \
	--machine-id $1 \
