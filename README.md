# dng-video

Prototype cinema dng video recorder using libcamera for raspi

Only tested on
- Raspi 5 with IMX585 

## Building

```sh
cmake -S . -B build
cmake --build build

```

## Usage

```sh
./build/dng-recorder [options] [output_directory]
```

Run `./build/dng-recorder --help` to see a full list of options or `--version`
to print the application's version.

Options:

- `--fps <fps>`               target frames per second
- `--shutter <microsec>`      exposure time in microseconds
- `--gain <gain>`             analogue gain
- `--ae <0|1>`                disable auto-exposure if set to 1
- `--awb <0|1>`               disable auto white balance if set to 1
- `--preview`                 enable hardware preview stream
- `--preview-size WxH`        preview dimensions
- `--preview-sink <sink>`     preview sink (`gl` or `kmssink`)
- `--preview-every N`         push every Nth preview frame
- `--size WxH`                RAW stream size
- `--help, -h`                display help and exit
- `--version, -v`             show program version and exit

The legacy environment variable overrides from the original proof of
concept remain available.

