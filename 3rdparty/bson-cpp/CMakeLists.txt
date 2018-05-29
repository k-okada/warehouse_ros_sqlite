cmake_minimum_required(VERSION 2.8)
project(bson-cpp)
if (APPLE)
	set(CMAKE_XCODE_ATTRIBUTE_GCC_VERSION "com.apple.compilers.llvm.clang.1_0")
	set(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LANGUAGE_STANDARD "c++11")
	set(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LIBRARY "libc++")
	
	#these have absolutely no effect :(
	if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUCC)
		message(FATAL_ERROR "You need to use clang on Apple!
		use CC=clang CXX=clang++ cmake path/to/project")
	endif()
	#SET(CMAKE_C_COMPILER "clang")
	#SET(CMAKE_CXX_COMPILER "clang++")
	
	SET(CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/inc)

if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUCC)
  set ( CMAKE_CXX_FLAGS "-std=c++0x")
  #set ( CMAKE_CXX_FLAGS_DEBUG "")
  #set ( CMAKE_CXX_FLAGS_RELEASE  "" )
endif ()
set(BSON_HEADERS
	inc/bson-inl.h
	inc/bson.h
	inc/bsonassert.h
	inc/bsonelement.h
	inc/bsonmisc.h
	inc/bsonobj.h
	inc/bsonobjbuilder.h
	inc/bsonobjiterator.h
	inc/bsontypes.h
	inc/inline_decls.h
	inc/oid.h
	inc/ordering.h
	inc/stringdata.h
	inc/builder.h
	inc/embedded_builder.h
	inc/hex.h
	inc/misc.h
	inc/noncopyable.h
	inc/optime.h
)
set(BSON_SOURCE
	src/bsonobj.cpp
        src/oid.cpp
        lib/base64.cpp
        lib/md5.c
        lib/nonce.cpp
)

add_library(bsoncpp-static STATIC ${BSON_SOURCE} ${BSON_HEADERS})
SET_TARGET_PROPERTIES(bsoncpp-static PROPERTIES OUTPUT_NAME bsoncpp)
add_library(bsoncpp SHARED ${BSON_SOURCE} ${BSON_HEADERS})


install(TARGETS bsoncpp bsoncpp-static
	LIBRARY DESTINATION lib
	ARCHIVE DESTINATION lib)
install(DIRECTORY inc/ DESTINATION include/bson
	FILES_MATCHING PATTERN "*.h"
)
