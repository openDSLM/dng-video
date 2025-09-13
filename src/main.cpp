#include "raw_recorder.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    std::string outDir = "./frames";
    std::string width, height;

    auto next_arg = [&](int &i) -> const char * {
        if (i + 1 < argc)
            return argv[++i];
        return nullptr;
    };

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--fps" || arg == "--framerate") && i + 1 < argc) {
            setenv("FPS", next_arg(i), 1);
        } else if (arg == "--shutter" && i + 1 < argc) {
            setenv("EXP_US", next_arg(i), 1);
        } else if ((arg == "--gain" || arg == "--analoggain") && i + 1 < argc) {
            setenv("AGAIN", next_arg(i), 1);
        } else if (arg == "--ae" && i + 1 < argc) {
            setenv("AE", next_arg(i), 1);
        } else if (arg == "--awb" && i + 1 < argc) {
            setenv("AWB", next_arg(i), 1);
        } else if (arg == "--preview") {
            setenv("PREVIEW", "1", 1);
        } else if (arg == "--preview-size" && i + 1 < argc) {
            setenv("PREVIEW_SIZE", next_arg(i), 1);
        } else if (arg == "--preview-sink" && i + 1 < argc) {
            setenv("PREVIEW_SINK", next_arg(i), 1);
        } else if (arg == "--preview-every" && i + 1 < argc) {
            setenv("PREVIEW_EVERY", next_arg(i), 1);
        } else if (arg == "--size" && i + 1 < argc) {
            setenv("SIZE", next_arg(i), 1);
        } else if (arg == "--width" && i + 1 < argc) {
            width = next_arg(i);
        } else if (arg == "--height" && i + 1 < argc) {
            height = next_arg(i);
        } else if ((arg == "-o" || arg == "--output") && i + 1 < argc) {
            outDir = next_arg(i);
        } else if ((arg == "-t" || arg == "--timeout") && i + 1 < argc) {
            setenv("TIMEOUT", next_arg(i), 1); // currently unused
        } else if (arg == "-n" || arg == "--nopreview") {
            setenv("PREVIEW", "0", 1);
        } else if (arg == "--denoise" && i + 1 < argc) {
            setenv("DENOISE", next_arg(i), 1);
        } else if (arg == "--save-pts" && i + 1 < argc) {
            setenv("SAVE_PTS", next_arg(i), 1);
        } else if (arg == "--codec" && i + 1 < argc) {
            setenv("CODEC", next_arg(i), 1);
        } else if (arg == "--segment" && i + 1 < argc) {
            setenv("SEGMENT", next_arg(i), 1);
        } else if (arg == "--level" && i + 1 < argc) {
            setenv("LEVEL", next_arg(i), 1);
        } else if (arg == "--buffer-count" && i + 1 < argc) {
            setenv("BUFCOUNT", next_arg(i), 1);
        } else if (arg == "--raw-format" && i + 1 < argc) {
            setenv("RAW_FMT", next_arg(i), 1);
        } else if (arg == "--camera" && i + 1 < argc) {
            setenv("CAMERA_INDEX", next_arg(i), 1);
        } else if (arg == "--list-cameras") {
            setenv("LIST_CAMERAS", "1", 1);
        } else if (!arg.empty() && arg[0] == '-') {
            // Ignore unknown option, consume its argument if present
            if (i + 1 < argc && argv[i + 1][0] != '-')
                ++i;
        } else {
            outDir = arg;
        }
    }

    if (!width.empty() || !height.empty()) {
        if (width.empty() || height.empty()) {
            std::cerr << "Both --width and --height must be specified" << std::endl;
            return 1;
        }
        std::string size = width + "x" + height;
        setenv("SIZE", size.c_str(), 1);
    }

    std::vector<char *> args;
    args.push_back(argv[0]);
    args.push_back(const_cast<char *>(outDir.c_str()));
    return run_raw_recorder(static_cast<int>(args.size()), args.data());
}

