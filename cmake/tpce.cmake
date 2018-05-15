## TPC-E related configurations
## current it's not well since not all file in *egen* is necessary
set(EGEN_PREFIX src/app/tpce/egen/)
file(GLOB TPCE_SOURCES
          "${EGEN_PREFIX}DateTime.cpp" "${EGEN_PREFIX}error.cpp"
          "${EGEN_PREFIX}Random.cpp" "${EGEN_PREFIX}Money.cpp"
          "${EGEN_PREFIX}EGenVersion.cpp" "${EGEN_PREFIX}locking.cpp"
          "${EGEN_PREFIX}threading.cpp" "${EGEN_PREFIX}BaseLogger.cpp" "${EGEN_PREFIX}EGenLogFormatterTab.cpp"
          "${EGEN_PREFIX}MEEPriceBoard.cpp" "${EGEN_PREFIX}MEESecurity.cpp" "${EGEN_PREFIX}MEETickerTape.cpp"
          "${EGEN_PREFIX}MEETradingFloor.cpp" "${EGEN_PREFIX}WheelTime.cpp" "${EGEN_PREFIX}AddressTable.cpp"
          "${EGEN_PREFIX}CustomerSelection.cpp" "${EGEN_PREFIX}CustomerTable.cpp" "${EGEN_PREFIX}InputFlatFilesStructure.cpp"
          "${EGEN_PREFIX}Person.cpp" "${EGEN_PREFIX}ReadRowFunctions.cpp"
          "${EGEN_PREFIX}TradeGen.cpp" "${EGEN_PREFIX}FlatFileLoader.cpp" "${EGEN_PREFIX}CE.cpp" "${EGEN_PREFIX}CETxnInputGenerator.cpp"
          "${EGEN_PREFIX}CETxnMixGenerator.cpp" "${EGEN_PREFIX}DM.cpp" "${EGEN_PREFIX}EGenGenerateAndLoad.cpp" "${EGEN_PREFIX}strutil.cpp"
          "${EGEN_PREFIX}progressmeter.cpp" "${EGEN_PREFIX}progressmeterinterface.cpp" "${EGEN_PREFIX}bucketsimulator.cpp")
