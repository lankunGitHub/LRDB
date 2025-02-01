# Source files for LRDB library
set(LRDB_SOURCES
    # Core components
    core/coding.cc
    
    # Utility components
    util/bloom_filter.cc
    util/comparator.cc
    util/env.cc
    util/error_handler.cc
    util/hash.cc
    util/logging.cc
    
    # Storage engine
    storage/lsm_tree.cc
    storage/memtable.cc
    storage/sstable.cc
    
    # Database interface
    db/column_family.cc
    db/db_impl.cc
    db/db_iterator.cc
    db/manifest.cc
    
    # Concurrency control
    concurrency/lock_manager.cc
    concurrency/tx_manager.cc
    concurrency/transaction.cc
    concurrency/version_chain_manager.cc
    
    # WAL system
    wal/wal.cc
    
    # Background management
    background/background_manager.cc
)

# Create static library
add_library(lrdb_static STATIC ${LRDB_SOURCES})
target_include_directories(lrdb_static PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_compile_features(lrdb_static PUBLIC cxx_std_17)

# Set library output name
set_target_properties(lrdb_static PROPERTIES OUTPUT_NAME lrdb)

# Install library
install(TARGETS lrdb_static
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

# Install headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/lrdb
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)
