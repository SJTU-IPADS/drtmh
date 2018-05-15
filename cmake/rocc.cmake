function(load_global_config) 
  if(ROCC_RBUF_SIZE)
  else()
  set(ROCC_RBUF_SIZE 512)  ## default value
  endif()

  if(USE_RDMA)
  else()
  set(USE_RDMA 1)
  endif()

  if(RDMA_STORE_SIZE)
  else()
    set(RDMA_STORE_SIZE 8)
  endif()

  configure_file("src/rocc_config.h.in" "src/rocc_config.h")
endfunction()

function(load_tx_config)

  if(TX_LOG_STYLE)
  else()
    set(TX_LOG_STYLE 1)  ## default uses RPC as logging
  endif()

  if(TX_BACKUP_STORE)
  else()
  set(TX_BACKUP_STORE 1)
  endif()
  

  if(ONE_SIDED_READ)
  else()
  set(ONE_SIDED_READ 0)
  endif()

  if(RDMA_CACHE)
  else()
  set(RDMA_CACHE 0)
  endif()

  if(RDMA_STORE_SIZE)
  else()
    if(ONE_SIDED_READ)
    set(RDMA_STORE_SIZE 0)
    else()
    set(RDMA_STORE_SIZE 8)
    endif()
  endif()            
  

  configure_file("src/tx_config.h.in" "src/tx_config.h")
endfunction()