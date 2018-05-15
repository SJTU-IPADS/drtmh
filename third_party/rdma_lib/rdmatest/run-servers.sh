numactl --cpunodebind=0 --membind=0 ./testframework \
	-n $1 \
	-t $2 
