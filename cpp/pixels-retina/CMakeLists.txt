cmake_minimum_required(VERSION 3.10)
project(pixels-retina)

cmake_policy(SET CMP0177 NEW)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find Java and JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# Include directories
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SOURCES
        lib/visibility/visibility_JNI.cpp
        lib/visibility/visibility.cpp)

# Create shared library
add_library(pixels-retina SHARED ${SOURCES})

# Set the target properties
set_target_properties(pixels-retina PROPERTIES
    OUTPUT_NAME "pixels-retina"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# Ensure the library is placed in the PIXELS_HOME directory
install(TARGETS pixels-retina
        LIBRARY DESTINATION $ENV{PIXELS_HOME}/lib)

# Add the test executable
add_executable(VisibilityTest test/visibilityTest.cpp)

# Link the test executable with the library
target_link_libraries(VisibilityTest pixels-retina)

# Add a test to CTest
add_test(NAME VisibilityTest COMMAND VisibilityTest)
