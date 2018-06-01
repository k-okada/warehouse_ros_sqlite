cmake_minimum_required(VERSION 2.8.3)
project(warehouse_ros_sqlite)

add_compile_options(-std=c++11)

find_package(catkin REQUIRED
  roscpp
  rostest
  rostime
  std_msgs
  warehouse_ros
)

find_package(PkgConfig)
pkg_check_modules(SQLite REQUIRED sqlite3)

catkin_package(
  INCLUDE_DIRS include
  LIBRARIES warehouse_ros
  CATKIN_DEPENDS roscpp rostime std_msgs
)

include_directories(include 3rdparty/bson-cpp/test 3rdparty/bson-cpp/inc 3rdparty/bson-cpp ${SQLite_INCLUDE_DIR} ${catkin_INCLUDE_DIRS})
link_directories(${catkin_LIBRARY_DIRS})

## build BSON-C++ library https://github.com/chrismanning/bson-cpp
set(BSON_SOURCE
	3rdparty/bson-cpp/src/bsonobj.cpp
        3rdparty/bson-cpp/src/oid.cpp
        3rdparty/bson-cpp/lib/base64.cpp
        3rdparty/bson-cpp/lib/md5.c
        3rdparty/bson-cpp/lib/nonce.cpp
        3rdparty/bson-cpp/test/json.cpp
)
#add_library(bsoncpp-static STATIC ${BSON_SOURCE})
#set_property(TARGET bsoncpp-static PROPERTY POSITION_INDEPENDENT_CODE ON)
##

set(warehouse_srcs
  src/query_results.cpp
  src/message_collection.cpp
  src/database_connection.cpp
)

add_library(warehouse_ros_sqlite SHARED ${warehouse_srcs} ${BSON_SOURCE})
#target_link_libraries(warehouse_ros_sqlite bsoncpp-static ${catkin_LIBRARIES})
target_link_libraries(warehouse_ros_sqlite ${SQLite_LIBRARIES} ${catkin_LIBRARIES})

if(CATKIN_ENABLE_TESTING)
  find_package(class_loader)
  catkin_add_gtest(test_warehouse_ros_sqlite_cpp test/test_warehouse_ros_sqlite.cpp)
  target_link_libraries(test_warehouse_ros_sqlite_cpp warehouse_ros_sqlite ${class_loader_LIBRARIES})
endif()

install(TARGETS warehouse_ros_sqlite LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION})

install(FILES sqlite_database_connection_plugin_description.xml
        DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION})
