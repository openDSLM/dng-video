cmake_minimum_required(VERSION 3.16)
project(dng_recorder VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)

add_executable(dng-recorder
    src/main.cpp
    src/raw_recorder.cpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBCAMERA REQUIRED libcamera)
target_include_directories(dng-recorder PRIVATE ${LIBCAMERA_INCLUDE_DIRS})
target_link_libraries(dng-recorder PRIVATE ${LIBCAMERA_LIBRARIES})

target_compile_definitions(dng-recorder PRIVATE
    DNG_RECORDER_VERSION="${PROJECT_VERSION}"
)

find_package(TIFF REQUIRED)
target_link_libraries(dng-recorder PRIVATE TIFF::TIFF pthread)

