# dng-video

Prototype raw video recorder using libcamera. The recorder now exposes a
command line interface compatible with common `rpicam-vid` parameters.

## Building

```sh
cmake -S . -B build
cmake --build build
```

## Usage

```sh
./build/dng-recorder [options] [output_directory]
```

Options:

- `-o, --output <dir>`        directory for captured DNG frames
- `-t, --timeout <ms>`        capture duration in milliseconds (currently ignored)
- `--fps, --framerate <fps>`  target frames per second
- `--shutter <microsec>`      exposure time in microseconds
- `--gain <gain>`             analogue gain (alias: `--analoggain`)
- `--ae <0|1>`                disable auto-exposure if set to 1
- `--awb <0|1>`               disable auto white balance if set to 1
- `--preview`                 enable hardware preview stream
- `-n, --nopreview`           disable preview
- `--preview-size WxH`        preview dimensions
- `--preview-sink <sink>`     preview sink (`gl` or `kmssink`)
- `--preview-every N`         push every Nth preview frame
- `--size WxH` or `--width` & `--height` specify RAW stream size
- `--buffer-count N`          minimum buffers for capture
- `--raw-format <fmt>`        force RAW pixel format (e.g. `SRGGB12`)
- `--camera <index>`          select camera by index
- `--list-cameras`            list available cameras and exit

The legacy environment variable overrides from the original proof of
concept remain available.

