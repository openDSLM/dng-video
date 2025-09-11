#include "raw_recorder.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    std::string outDir = "./frames";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--fps" && i + 1 < argc) {
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

