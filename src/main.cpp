#include "raw_recorder.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace {

void print_help(const char *prog) {
    std::cout << "Usage: " << prog << " [options] [output_directory]\n\n"
              << "Options:\n"
              << "  --fps <fps>               target frames per second\n"
              << "  --shutter <microsec>      exposure time in microseconds\n"
              << "  --gain <gain>             analogue gain\n"
              << "  --ae <0|1>                disable auto-exposure if set to 1\n"
              << "  --awb <0|1>               disable auto white balance if set to 1\n"
              << "  --preview                 enable hardware preview stream\n"
              << "  --preview-size WxH        preview dimensions\n"
              << "  --preview-sink <sink>     preview sink (gl or kmssink)\n"
              << "  --preview-every N         push every Nth preview frame\n"
              << "  --size WxH                RAW stream size\n"
              << "  --help, -h                display this help and exit\n"
              << "  --version, -v             output version information and exit\n";
}

} // namespace

int main(int argc, char **argv) {
    std::string outDir = "./frames";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_help(argv[0]);
            return 0;
        } else if (arg == "--version" || arg == "-v") {
            std::cout << "dng-recorder version " << DNG_RECORDER_VERSION << std::endl;
            return 0;
        } else if (arg == "--fps" && i + 1 < argc) {
            setenv("FPS", argv[++i], 1);
        } else if (arg == "--shutter" && i + 1 < argc) {
            setenv("EXP_US", argv[++i], 1);
        } else if (arg == "--gain" && i + 1 < argc) {
            setenv("AGAIN", argv[++i], 1);
        } else if (arg == "--ae" && i + 1 < argc) {
            setenv("AE", argv[++i], 1);
        } else if (arg == "--awb" && i + 1 < argc) {
            setenv("AWB", argv[++i], 1);
        } else if (arg == "--preview") {
            setenv("PREVIEW", "1", 1);
        } else if (arg == "--preview-size" && i + 1 < argc) {
            setenv("PREVIEW_SIZE", argv[++i], 1);
        } else if (arg == "--preview-sink" && i + 1 < argc) {
            setenv("PREVIEW_SINK", argv[++i], 1);
        } else if (arg == "--preview-every" && i + 1 < argc) {
            setenv("PREVIEW_EVERY", argv[++i], 1);
        } else if (arg == "--size" && i + 1 < argc) {
            setenv("SIZE", argv[++i], 1);
        } else if (arg.rfind("--", 0) == 0) {
            std::cerr << "Unknown option: " << arg << std::endl;
            return 1;
        } else {
            outDir = arg;
        }
    }

    std::vector<char *> args;
    args.push_back(argv[0]);
    args.push_back(const_cast<char *>(outDir.c_str()));
    return run_raw_recorder(static_cast<int>(args.size()), args.data());
}

