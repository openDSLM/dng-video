#include "raw_recorder.h"

#include <libcamera/libcamera.h>
#include <tiffio.h>

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace libcamera;
namespace {
using SteadyClock = std::chrono::steady_clock;

static void unpack_raw12_to_u16(const uint8_t *in, uint16_t *out,
                                unsigned width, unsigned height, unsigned strideBytes)
{
    for (unsigned y = 0; y < height; ++y) {
        const uint8_t *row = in + size_t(y) * strideBytes;
        unsigned i = 0, j = 0;
        while (i + 3 < width && j + 5 < strideBytes) {
            uint8_t b0 = row[j + 0], b1 = row[j + 1], b2 = row[j + 2];
            uint8_t b3 = row[j + 3], b4 = row[j + 4], b5 = row[j + 5];
            uint16_t p0 = uint16_t(b0 | ((b1 & 0x0F) << 8));
            uint16_t p1 = uint16_t((b1 >> 4) | (b2 << 4));
            uint16_t p2 = uint16_t(b3 | ((b4 & 0x0F) << 8));
            uint16_t p3 = uint16_t((b4 >> 4) | (b5 << 4));
            const size_t base = size_t(y) * width + i;
            out[base + 0] = p0;
            out[base + 1] = p1;
            out[base + 2] = p2;
            out[base + 3] = p3;
            i += 4;
            j += 6;
        }
        while (i < width) {
            // Handle any trailing pixels conservatively.
            const size_t base = size_t(y) * width + i;
            out[base] = row[j] << 4;
            ++i;
            ++j;
        }
    }
}

static bool write_dng16(const std::filesystem::path &path,
                        const uint16_t *data,
                        uint32_t width,
                        uint32_t height,
                        const std::string &cfa,
                        uint16_t blackLevel,
                        uint32_t whiteLevel)
{
    TIFF *tif = TIFFOpen(path.string().c_str(), "w");
    if (!tif)
        return false;

    TIFFSetField(tif, TIFFTAG_IMAGEWIDTH, width);
    TIFFSetField(tif, TIFFTAG_IMAGELENGTH, height);
    TIFFSetField(tif, TIFFTAG_SAMPLESPERPIXEL, 1);
    TIFFSetField(tif, TIFFTAG_BITSPERSAMPLE, 16);
    TIFFSetField(tif, TIFFTAG_COMPRESSION, COMPRESSION_NONE);
    TIFFSetField(tif, TIFFTAG_PHOTOMETRIC, PHOTOMETRIC_CFA);
    TIFFSetField(tif, TIFFTAG_PLANARCONFIG, PLANARCONFIG_CONTIG);
    TIFFSetField(tif, TIFFTAG_ORIENTATION, ORIENTATION_TOPLEFT);

    uint8_t patt[4] = {0, 1, 1, 2};
    std::string f = cfa;
    std::transform(f.begin(), f.end(), f.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    if (f == "BGGR") {
        const uint8_t p[4] = {2, 1, 1, 0};
        std::memcpy(patt, p, 4);
    } else if (f == "GRBG") {
        const uint8_t p[4] = {1, 0, 2, 1};
        std::memcpy(patt, p, 4);
    } else if (f == "GBRG") {
        const uint8_t p[4] = {1, 2, 0, 1};
        std::memcpy(patt, p, 4);
    }

    uint16_t dim[2] = {2, 2};
    TIFFSetField(tif, TIFFTAG_CFAREPEATPATTERNDIM, dim);
    TIFFSetField(tif, TIFFTAG_CFAPATTERN, 4, patt);

    uint8_t cfaColors[3] = {0, 1, 2};
    TIFFSetField(tif, TIFFTAG_CFAPLANECOLOR, 3, cfaColors);
    TIFFSetField(tif, TIFFTAG_CFALAYOUT, 1);

    uint16_t blDim[2] = {1, 1};
    TIFFSetField(tif, TIFFTAG_BLACKLEVELREPEATDIM, blDim);
    double bl = static_cast<double>(blackLevel);
    TIFFSetField(tif, TIFFTAG_BLACKLEVEL, 1, &bl);
    if (whiteLevel) {
        double wl = static_cast<double>(whiteLevel);
        TIFFSetField(tif, TIFFTAG_WHITELEVEL, 1, &wl);
    }

    uint8_t dngVersion[4] = {1, 4, 0, 0};
    TIFFSetField(tif, TIFFTAG_DNGVERSION, dngVersion);

    TIFFSetField(tif, TIFFTAG_ROWSPERSTRIP, height);
    const tsize_t bytes = tsize_t(width) * tsize_t(height) * 2;
    bool ok = TIFFWriteEncodedStrip(tif, 0, (void *)data, bytes) >= 0;
    TIFFClose(tif);
    return ok;
}

static std::string cfa_from_fourcc(const PixelFormat &pf)
{
    const std::string s = pf.toString();
    if (s.find("RGGB") != std::string::npos || s.find("rggb") != std::string::npos)
        return "RGGB";
    if (s.find("BGGR") != std::string::npos)
        return "BGGR";
    if (s.find("GRBG") != std::string::npos)
        return "GRBG";
    if (s.find("GBRG") != std::string::npos)
        return "GBRG";
    return "RGGB";
}

struct MappedPlane {
    void *base{};
    size_t map_len{};
    off_t aligned_off{};
    int fd{-1};
    uint8_t *data{};
    size_t data_len{};
};

struct MappedBuffer {
    std::vector<MappedPlane> planes;
    ~MappedBuffer() {
        for (auto &p : planes) {
            if (p.base && p.map_len)
                ::munmap(p.base, p.map_len);
        }
    }
};

static bool mmapFrameBuffer(const FrameBuffer *fb, MappedBuffer &map)
{
    map.planes.resize(fb->planes().size());
    const long page = sysconf(_SC_PAGE_SIZE);
    for (unsigned i = 0; i < fb->planes().size(); ++i) {
        const FrameBuffer::Plane &pl = fb->planes()[i];
        int fd = pl.fd.get();
        off_t off = static_cast<off_t>(pl.offset);
        off_t aligned = off & ~static_cast<off_t>(page - 1);
        size_t delta = static_cast<size_t>(off - aligned);
        size_t map_len = static_cast<size_t>(pl.length) + delta;

        void *base = ::mmap(nullptr, map_len, PROT_READ, MAP_SHARED, fd, aligned);
        if (base == MAP_FAILED)
            return false;

        map.planes[i].base = base;
        map.planes[i].map_len = map_len;
        map.planes[i].aligned_off = aligned;
        map.planes[i].fd = fd;
        map.planes[i].data = static_cast<uint8_t *>(base) + delta;
        map.planes[i].data_len = static_cast<size_t>(pl.length);
    }
    return true;
}

struct CaptureSettings {
    double fps{0.0};
    int64_t exposureUs{-1};
    float gain{0.0f};
    float iso{0.0f};
    bool autoExposure{true};
    bool autoWhiteBalance{true};
};

class CameraBackend {
  public:
    struct Config {
        std::filesystem::path outputDir{"./frames"};
        bool daemonMode{false};
        unsigned previewEvery{1};
        unsigned previewDownscale{2};
        bool previewEnabled{true};
        int shotLimit{-1};
        bool continuous{false};
        CaptureSettings initialSettings{};
        std::optional<std::pair<unsigned, unsigned>> sizeOverride;
    };

    explicit CameraBackend(const Config &cfg)
        : cfg_(cfg),
          outputDir_(cfg.outputDir),
          shotLimit_(cfg.shotLimit),
          continuousCapture_(cfg.continuous)
    {
        settings_ = cfg.initialSettings;
        std::error_code ec;
        std::filesystem::create_directories(outputDir_, ec);
    }

    ~CameraBackend() { stop(); }

    bool init();
    bool start();
    void stop();

    void requestSingleCapture();
    void setContinuous(bool enable, int limit);

    CaptureSettings currentSettings();
    bool updateSettings(const std::map<std::string, std::string> &params, std::string &message);
    std::string statusJson();

    bool waitForPreview(std::vector<uint8_t> &out, uint64_t &lastFrame, std::chrono::milliseconds timeout);

    void signalStop();
    bool isRunning() const { return running_.load(); }

    void runCliLoop(std::atomic<bool> &keepRunning);

  private:
    struct SaveJob {
        std::vector<uint16_t> data;
        uint64_t index{0};
    };

    ControlList buildControlList(const CaptureSettings &set);
    void handleRequestComplete(Request *req);
    bool copyFrameToScratch(const FrameBuffer *fb);
    void enqueueSave(const std::vector<uint16_t> &img, uint64_t frameIndex);
    void writerLoop();
    void updatePreview(uint64_t frameIndex);
    std::vector<uint8_t> makePreviewImage();

    Config cfg_;
    std::filesystem::path outputDir_;

    CameraManager cm_;
    std::shared_ptr<Camera> camera_;
    std::unique_ptr<FrameBufferAllocator> allocator_;
    const Stream *streamRaw_{nullptr};
    Size sizeRaw_{};
    PixelFormat pixRaw_{};
    std::string cfa_{};
    bool rawIsPacked12_{false};
    bool rawIs16_{false};
    uint16_t blackLevel_{0};
    uint32_t whiteLevel_{0};

    std::vector<std::unique_ptr<Request>> requests_;
    std::map<const FrameBuffer *, std::unique_ptr<MappedBuffer>> mappings_;

    std::vector<uint16_t> scratch_;
    std::mutex scratchMutex_;

    std::mutex previewMutex_;
    std::condition_variable previewCv_;
    std::vector<uint8_t> previewFrame_;
    uint64_t previewFrameId_{0};

    std::mutex saveMutex_;
    std::condition_variable saveCv_;
    std::deque<SaveJob> saveQueue_;
    std::thread saveThread_;
    bool saveThreadRunning_{false};

    std::atomic<uint64_t> frameCounter_{0};
    std::atomic<uint64_t> savedCounter_{0};
    std::atomic<uint64_t> captureRequests_{0};
    std::atomic<int> shotLimit_{-1};
    bool continuousCapture_{false};
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};

    CaptureSettings settings_{};
    std::mutex settingsMutex_;

    ControlInfoMap controlInfo_;
    bool haveAeEnable_{false};
    bool haveAwbEnable_{false};
    bool haveFrameDurationLimits_{false};
    bool haveExposure_{false};
    bool haveGain_{false};

    uint64_t nextCaptureIndex_{0};
    unsigned previewEveryCount_{0};
};

bool CameraBackend::init()
{
    if (cm_.start()) {
        std::fprintf(stderr, "CameraManager start failed\n");
        return false;
    }
    if (cm_.cameras().empty()) {
        std::fprintf(stderr, "No cameras detected\n");
        return false;
    }

    camera_ = cm_.cameras()[0];
    if (!camera_) {
        std::fprintf(stderr, "Failed to get camera instance\n");
        return false;
    }
    if (camera_->acquire()) {
        std::fprintf(stderr, "Failed to acquire camera\n");
        return false;
    }

    StreamRole role = StreamRole::VideoRecording;
    std::unique_ptr<CameraConfiguration> config = camera_->generateConfiguration({role});
    if (!config || config->size() != 1) {
        std::fprintf(stderr, "Failed to generate configuration\n");
        return false;
    }

    StreamConfiguration &cfg = config->at(0);
    const StreamFormats fmtsRaw = cfg.formats();
    const std::vector<PixelFormat> available = fmtsRaw.pixelformats();
    auto hasFmt = [&](const PixelFormat &pf) {
        return std::find(available.begin(), available.end(), pf) != available.end();
    };

    if (hasFmt(formats::SRGGB12_CSI2P))
        cfg.pixelFormat = formats::SRGGB12_CSI2P;
    else if (hasFmt(formats::SRGGB12))
        cfg.pixelFormat = formats::SRGGB12;
    else if (hasFmt(formats::SRGGB16))
        cfg.pixelFormat = formats::SRGGB16;

    if (cfg_.sizeOverride) {
        cfg.size.width = cfg_.sizeOverride->first;
        cfg.size.height = cfg_.sizeOverride->second;
    }

    if (config->validate() == CameraConfiguration::Invalid) {
        std::fprintf(stderr, "Configuration invalid\n");
        return false;
    }

    if (camera_->configure(config.get())) {
        std::fprintf(stderr, "Camera configure failed\n");
        return false;
    }

    streamRaw_ = cfg.stream();
    sizeRaw_ = cfg.size;
    pixRaw_ = cfg.pixelFormat;
    cfa_ = cfa_from_fourcc(pixRaw_);
    rawIsPacked12_ = (pixRaw_ == formats::SRGGB12_CSI2P || pixRaw_ == formats::SRGGB12);
    rawIs16_ = (pixRaw_ == formats::SRGGB16);
    blackLevel_ = 0;
    whiteLevel_ = rawIs16_ ? 65535u : 4095u;

    allocator_ = std::make_unique<FrameBufferAllocator>(camera_);
    if (allocator_->allocate(streamRaw_) < 0) {
        std::fprintf(stderr, "Failed to allocate frame buffers\n");
        return false;
    }

    auto &buffers = allocator_->buffers(streamRaw_);
    if (buffers.empty()) {
        std::fprintf(stderr, "No frame buffers allocated\n");
        return false;
    }

    for (auto &buf : buffers) {
        auto req = camera_->createRequest();
        if (!req) {
            std::fprintf(stderr, "Failed to create request\n");
            return false;
        }
        if (req->addBuffer(streamRaw_, buf.get()) < 0) {
            std::fprintf(stderr, "Failed to add buffer to request\n");
            return false;
        }
        requests_.emplace_back(std::move(req));

        auto map = std::make_unique<MappedBuffer>();
        if (!mmapFrameBuffer(buf.get(), *map)) {
            std::fprintf(stderr, "Failed to mmap buffer\n");
            return false;
        }
        mappings_.emplace(buf.get(), std::move(map));
    }

    scratch_.assign(size_t(sizeRaw_.width) * size_t(sizeRaw_.height), 0);

    controlInfo_ = camera_->controls();
    haveAeEnable_ = (controlInfo_.find(&controls::AeEnable) != controlInfo_.end());
    haveAwbEnable_ = (controlInfo_.find(&controls::AwbEnable) != controlInfo_.end());
    haveFrameDurationLimits_ = (controlInfo_.find(&controls::FrameDurationLimits) != controlInfo_.end());
    haveExposure_ = (controlInfo_.find(&controls::ExposureTime) != controlInfo_.end());
    haveGain_ = (controlInfo_.find(&controls::AnalogueGain) != controlInfo_.end());

    std::printf("Configured RAW stream: %s %ux%u CFA=%s\n",
                pixRaw_.toString().c_str(), sizeRaw_.width, sizeRaw_.height, cfa_.c_str());

    return true;
}

ControlList CameraBackend::buildControlList(const CaptureSettings &set)
{
    ControlList ctrls(controlInfo_);
    if (haveAeEnable_)
        ctrls.set(controls::AeEnable, set.autoExposure);
    if (haveAwbEnable_)
        ctrls.set(controls::AwbEnable, set.autoWhiteBalance);

    if (haveFrameDurationLimits_ && set.fps > 0.0) {
        int64_t frameDurNs = static_cast<int64_t>(1e9 / set.fps);
        int64_t frameDurUs = frameDurNs / 1000;
        if (frameDurUs < 1)
            frameDurUs = 1;
        int64_t lim[2] = {frameDurUs, frameDurUs};
        ctrls.set(controls::FrameDurationLimits, Span<const int64_t, 2>(lim));
    }

    if (!set.autoExposure && haveExposure_ && set.exposureUs > 0)
        ctrls.set(controls::ExposureTime, set.exposureUs);

    float gain = set.gain;
    if (gain <= 0.0f && set.iso > 0.0f)
        gain = set.iso / 100.0f;

    if (haveGain_ && gain > 0.0f)
        ctrls.set(controls::AnalogueGain, gain);

    return ctrls;
}

bool CameraBackend::start()
{
    if (!camera_)
        return false;

    saveThreadRunning_ = true;
    saveThread_ = std::thread(&CameraBackend::writerLoop, this);

    camera_->requestCompleted.connect(this, &CameraBackend::handleRequestComplete);

    ControlList initCtrls = buildControlList(settings_);
    if (camera_->start(&initCtrls)) {
        std::fprintf(stderr, "Failed to start camera\n");
        saveThreadRunning_ = false;
        saveCv_.notify_all();
        if (saveThread_.joinable())
            saveThread_.join();
        return false;
    }

    for (auto &req : requests_) {
        if (camera_->queueRequest(req.get()) < 0)
            std::fprintf(stderr, "Failed to queue request\n");
    }

    running_ = true;
    stopping_ = false;
    return true;
}

void CameraBackend::stop()
{
    if (!camera_)
        return;

    if (running_.exchange(false)) {
        stopping_ = true;
        camera_->stop();
    }

    if (camera_) {
        camera_->requestCompleted.disconnect(this, &CameraBackend::handleRequestComplete);
        camera_->release();
        camera_.reset();
    }

    requests_.clear();
    mappings_.clear();
    allocator_.reset();

    cm_.stop();

    {
        std::lock_guard<std::mutex> lk(saveMutex_);
        saveThreadRunning_ = false;
    }
    saveCv_.notify_all();
    if (saveThread_.joinable())
        saveThread_.join();
}

void CameraBackend::requestSingleCapture()
{
    captureRequests_.fetch_add(1);
}

void CameraBackend::setContinuous(bool enable, int limit)
{
    continuousCapture_ = enable;
    shotLimit_.store(limit);
}

CaptureSettings CameraBackend::currentSettings()
{
    std::lock_guard<std::mutex> lk(settingsMutex_);
    return settings_;
}

bool CameraBackend::updateSettings(const std::map<std::string, std::string> &params, std::string &message)
{
    CaptureSettings updated;
    {
        std::lock_guard<std::mutex> lk(settingsMutex_);
        updated = settings_;
    }

    auto parseBool = [](const std::string &v) {
        return !(v == "0" || v == "false" || v == "False" || v == "FALSE");
    };

    for (const auto &[k, v] : params) {
        try {
            if (k == "fps") {
                updated.fps = std::stod(v);
            } else if (k == "exposure" || k == "exposure_us") {
                updated.exposureUs = std::stoll(v);
            } else if (k == "gain") {
                updated.gain = std::stof(v);
            } else if (k == "iso") {
                updated.iso = std::stof(v);
            } else if (k == "auto_exposure") {
                updated.autoExposure = parseBool(v);
            } else if (k == "auto_white_balance" || k == "awb") {
                updated.autoWhiteBalance = parseBool(v);
            }
        } catch (const std::exception &) {
            message = std::string("Invalid value for ") + k;
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lk(settingsMutex_);
        settings_ = updated;
    }

    if (running_ && camera_)
        camera_->setControls(buildControlList(updated));

    message = "{\"status\":\"ok\"}";
    return true;
}

std::string CameraBackend::statusJson()
{
    CaptureSettings s = currentSettings();
    std::ostringstream oss;
    oss << "{"
        << "\"frames\":" << frameCounter_.load() << ","
        << "\"saved\":" << savedCounter_.load() << ","
        << "\"pending\":" << captureRequests_.load() << ","
        << "\"continuous\":" << (continuousCapture_ ? "true" : "false") << ","
        << "\"settings\":{"
        << "\"fps\":" << s.fps << ","
        << "\"exposure_us\":" << s.exposureUs << ","
        << "\"gain\":" << s.gain << ","
        << "\"iso\":" << s.iso << ","
        << "\"auto_exposure\":" << (s.autoExposure ? "true" : "false") << ","
        << "\"auto_white_balance\":" << (s.autoWhiteBalance ? "true" : "false")
        << "}"
        << "}";
    return oss.str();
}

bool CameraBackend::waitForPreview(std::vector<uint8_t> &out, uint64_t &lastFrame,
                                   std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lk(previewMutex_);
    if (previewFrameId_ <= lastFrame) {
        if (!previewCv_.wait_for(lk, timeout, [&] {
                return previewFrameId_ > lastFrame || stopping_.load();
            }))
            return false;
    }
    if (previewFrame_.empty())
        return false;
    out = previewFrame_;
    lastFrame = previewFrameId_;
    return true;
}

void CameraBackend::signalStop()
{
    stopping_ = true;
}

void CameraBackend::runCliLoop(std::atomic<bool> &keepRunning)
{
    auto last = SteadyClock::now();
    uint64_t lastFrame = 0;
    uint64_t lastSaved = 0;
    while (keepRunning.load() && !stopping_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto now = SteadyClock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count();
        if (ms >= 1000) {
            uint64_t frames = frameCounter_.load();
            uint64_t saved = savedCounter_.load();
            double inFps = (frames - lastFrame) * 1000.0 / ms;
            double saveFps = (saved - lastSaved) * 1000.0 / ms;
            std::printf("[1s] in=%.2f fps saved=%.2f fps (total frames=%llu saved=%llu)\n",
                        inFps, saveFps,
                        static_cast<unsigned long long>(frames),
                        static_cast<unsigned long long>(saved));
            last = now;
            lastFrame = frames;
            lastSaved = saved;
        }
    }
}

void CameraBackend::handleRequestComplete(Request *req)
{
    if (req->status() == Request::RequestCancelled)
        return;

    const uint64_t frameIdx = frameCounter_.fetch_add(1) + 1;

    auto it = req->buffers().find(streamRaw_);
    if (it == req->buffers().end()) {
        req->reuse(Request::ReuseBuffers);
        camera_->queueRequest(req);
        return;
    }

    const FrameBuffer *fb = it->second;
    auto mapIt = mappings_.find(fb);
    if (mapIt == mappings_.end()) {
        req->reuse(Request::ReuseBuffers);
        camera_->queueRequest(req);
        return;
    }

    if (!copyFrameToScratch(fb)) {
        req->reuse(Request::ReuseBuffers);
        camera_->queueRequest(req);
        return;
    }

    if (cfg_.previewEnabled) {
        if (cfg_.previewEvery <= 1 || ++previewEveryCount_ % cfg_.previewEvery == 0)
            updatePreview(frameIdx);
    }

    bool shouldSave = false;
    if (continuousCapture_) {
        int limit = shotLimit_.load();
        if (limit != 0) {
            shouldSave = true;
            if (limit > 0) {
                if (shotLimit_.fetch_sub(1) == 1)
                    stopping_ = true;
            }
        }
    } else {
        uint64_t expected = captureRequests_.load();
        while (expected > 0) {
            if (captureRequests_.compare_exchange_weak(expected, expected - 1)) {
                shouldSave = true;
                break;
            }
        }
    }

    if (shouldSave) {
        std::vector<uint16_t> copy;
        {
            std::lock_guard<std::mutex> lk(scratchMutex_);
            copy = scratch_;
        }
        enqueueSave(copy, frameIdx);
    }

    req->reuse(Request::ReuseBuffers);
    camera_->queueRequest(req);
}

bool CameraBackend::copyFrameToScratch(const FrameBuffer *fb)
{
    auto mapIt = mappings_.find(fb);
    if (mapIt == mappings_.end())
        return false;
    const MappedBuffer &map = *mapIt->second;
    if (map.planes.empty() || !map.planes[0].data)
        return false;

    const FrameBuffer::Plane &pl = fb->planes()[0];
    const uint8_t *src = map.planes[0].data;
    unsigned strideBytes = static_cast<unsigned>(pl.length / sizeRaw_.height);

    std::lock_guard<std::mutex> lk(scratchMutex_);
    if (rawIsPacked12_)
        unpack_raw12_to_u16(src, scratch_.data(), sizeRaw_.width, sizeRaw_.height, strideBytes);
    else if (rawIs16_) {
        const uint16_t *src16 = reinterpret_cast<const uint16_t *>(src);
        unsigned stridePixels = strideBytes / 2;
        for (unsigned y = 0; y < sizeRaw_.height; ++y) {
            std::memcpy(&scratch_[size_t(y) * sizeRaw_.width],
                        src16 + size_t(y) * stridePixels,
                        sizeRaw_.width * sizeof(uint16_t));
        }
    } else {
        for (unsigned y = 0; y < sizeRaw_.height; ++y) {
            for (unsigned x = 0; x < sizeRaw_.width; ++x) {
                scratch_[size_t(y) * sizeRaw_.width + x] = src[size_t(y) * strideBytes + x] << 4;
            }
        }
    }
    return true;
}

void CameraBackend::enqueueSave(const std::vector<uint16_t> &img, uint64_t frameIndex)
{
    SaveJob job;
    job.data = img;
    job.index = frameIndex;
    {
        std::lock_guard<std::mutex> lk(saveMutex_);
        saveQueue_.push_back(std::move(job));
    }
    saveCv_.notify_one();
}

void CameraBackend::writerLoop()
{
    while (true) {
        SaveJob job;
        {
            std::unique_lock<std::mutex> lk(saveMutex_);
            saveCv_.wait(lk, [&] {
                return !saveQueue_.empty() || !saveThreadRunning_;
            });
            if (!saveThreadRunning_ && saveQueue_.empty())
                break;
            job = std::move(saveQueue_.front());
            saveQueue_.pop_front();
        }
        char name[256];
        std::snprintf(name, sizeof(name), "frame_%06llu.dng",
                      static_cast<unsigned long long>(job.index));
        std::filesystem::path path = outputDir_ / name;
        if (write_dng16(path, job.data.data(), sizeRaw_.width, sizeRaw_.height,
                        cfa_, blackLevel_, whiteLevel_)) {
            savedCounter_.fetch_add(1);
        }
    }
}

void CameraBackend::updatePreview(uint64_t frameIndex)
{
    std::vector<uint8_t> ppm = makePreviewImage();
    {
        std::lock_guard<std::mutex> lk(previewMutex_);
        previewFrame_ = std::move(ppm);
        previewFrameId_ = frameIndex;
    }
    previewCv_.notify_all();
}

std::vector<uint8_t> CameraBackend::makePreviewImage()
{
    std::vector<uint16_t> copy;
    {
        std::lock_guard<std::mutex> lk(scratchMutex_);
        copy = scratch_;
    }

    unsigned step = std::max(2u, cfg_.previewDownscale);
    if (step % 2 != 0)
        ++step;
    if (step > sizeRaw_.width || step > sizeRaw_.height)
        step = 2;

    unsigned outW = sizeRaw_.width / step;
    unsigned outH = sizeRaw_.height / step;
    if (outW == 0 || outH == 0) {
        step = 2;
        outW = std::max(1u, sizeRaw_.width / 2);
        outH = std::max(1u, sizeRaw_.height / 2);
    }

    std::vector<uint8_t> rgb(outW * outH * 3);
    for (unsigned y = 0, oy = 0; y + 1 < sizeRaw_.height && oy < outH; y += step, ++oy) {
        for (unsigned x = 0, ox = 0; x + 1 < sizeRaw_.width && ox < outW; x += step, ++ox) {
            size_t base = size_t(y) * sizeRaw_.width + x;
            uint16_t tl = copy[base];
            uint16_t tr = copy[base + 1];
            uint16_t bl = copy[base + sizeRaw_.width];
            uint16_t br = copy[base + sizeRaw_.width + 1];

            uint16_t r{}, g{}, b{};
            if (cfa_ == "BGGR") {
                r = br;
                b = tl;
                g = static_cast<uint16_t>((tr + bl) / 2);
            } else if (cfa_ == "GRBG") {
                r = tr;
                b = bl;
                g = static_cast<uint16_t>((tl + br) / 2);
            } else if (cfa_ == "GBRG") {
                r = bl;
                b = tr;
                g = static_cast<uint16_t>((tl + br) / 2);
            } else { // RGGB
                r = tl;
                b = br;
                g = static_cast<uint16_t>((tr + bl) / 2);
            }

            uint16_t maxVal = whiteLevel_ ? whiteLevel_ : 4095u;
            auto to8 = [&](uint16_t v) {
                double norm = std::max(0.0, std::min(1.0, (v - blackLevel_) / double(maxVal)));
                return static_cast<uint8_t>(norm * 255.0 + 0.5);
            };
            size_t dst = (size_t(oy) * outW + ox) * 3;
            rgb[dst + 0] = to8(r);
            rgb[dst + 1] = to8(g);
            rgb[dst + 2] = to8(b);
        }
    }

    std::ostringstream header;
    header << "P6\n" << outW << " " << outH << "\n255\n";
    std::string hdr = header.str();
    std::vector<uint8_t> ppm(hdr.begin(), hdr.end());
    ppm.insert(ppm.end(), rgb.begin(), rgb.end());
    return ppm;
}

void CameraBackend::signalStop()
{
    stopping_ = true;
}

class HttpServer {
  public:
    HttpServer(CameraBackend &backend, std::string listen)
        : backend_(backend), listenAddress_(std::move(listen)) {}

    bool start();
    void stop();

  private:
    void serverLoop();
    void handleClient(int fd);
    void handlePreview(int fd, const std::string &path);
    void handleStatus(int fd);
    void handleCapture(int fd);
    void handleSettings(int fd, const std::map<std::string, std::string> &params);
    void sendResponse(int fd, const std::string &status,
                      const std::string &contentType, const std::string &body);
    void sendNotFound(int fd);
    static bool parseListen(const std::string &listen, std::string &host, uint16_t &port);
    static std::map<std::string, std::string> parseQuery(const std::string &path, std::string &resource);

    CameraBackend &backend_;
    std::string listenAddress_;
    int listenFd_{-1};
    std::thread serverThread_;
    std::atomic<bool> running_{false};
};

bool HttpServer::parseListen(const std::string &listen, std::string &host, uint16_t &port)
{
    auto pos = listen.rfind(':');
    if (pos == std::string::npos)
        return false;
    host = listen.substr(0, pos);
    std::string portStr = listen.substr(pos + 1);
    port = static_cast<uint16_t>(std::stoi(portStr));
    if (host.empty())
        host = "0.0.0.0";
    return true;
}

std::map<std::string, std::string> HttpServer::parseQuery(const std::string &path, std::string &resource)
{
    auto pos = path.find('?');
    if (pos == std::string::npos) {
        resource = path;
        return {};
    }
    resource = path.substr(0, pos);
    std::map<std::string, std::string> params;
    std::string query = path.substr(pos + 1);
    std::istringstream iss(query);
    std::string kv;
    while (std::getline(iss, kv, '&')) {
        auto eq = kv.find('=');
        if (eq == std::string::npos)
            params[kv] = "";
        else
            params[kv.substr(0, eq)] = kv.substr(eq + 1);
    }
    return params;
}

bool HttpServer::start()
{
    std::string host;
    uint16_t port = 0;
    if (!parseListen(listenAddress_, host, port)) {
        std::fprintf(stderr, "Invalid listen address: %s\n", listenAddress_.c_str());
        return false;
    }

    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        std::perror("socket");
        return false;
    }

    int opt = 1;
    ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host == "0.0.0.0")
        addr.sin_addr.s_addr = INADDR_ANY;
    else
        addr.sin_addr.s_addr = inet_addr(host.c_str());

    if (::bind(listenFd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }

    if (::listen(listenFd_, 8) < 0) {
        std::perror("listen");
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }

    running_ = true;
    serverThread_ = std::thread(&HttpServer::serverLoop, this);
    std::printf("HTTP backend listening on %s\n", listenAddress_.c_str());
    return true;
}

void HttpServer::stop()
{
    running_ = false;
    if (listenFd_ >= 0) {
        ::shutdown(listenFd_, SHUT_RDWR);
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (serverThread_.joinable())
        serverThread_.join();
}

void HttpServer::serverLoop()
{
    while (running_) {
        sockaddr_in cli{};
        socklen_t len = sizeof(cli);
        int fd = ::accept(listenFd_, reinterpret_cast<sockaddr *>(&cli), &len);
        if (fd < 0) {
            if (running_)
                std::perror("accept");
            continue;
        }
        std::thread(&HttpServer::handleClient, this, fd).detach();
    }
}

void HttpServer::handleClient(int fd)
{
    char buf[4096];
    std::string req;
    while (req.find("\r\n\r\n") == std::string::npos) {
        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
            ::close(fd);
            return;
        }
        req.append(buf, buf + r);
        if (req.size() > 8192)
            break;
    }

    std::istringstream iss(req);
    std::string method, path, version;
    iss >> method >> path >> version;
    if (method.empty()) {
        ::close(fd);
        return;
    }

    std::string resource;
    auto params = parseQuery(path, resource);

    if (resource == "/preview" || resource == "/preview/live") {
        handlePreview(fd, resource);
    } else if (resource == "/api/v1/status") {
        handleStatus(fd);
    } else if (resource == "/api/v1/capture") {
        handleCapture(fd);
    } else if (resource == "/api/v1/settings") {
        handleSettings(fd, params);
    } else {
        sendNotFound(fd);
    }
    ::close(fd);
}

void HttpServer::handlePreview(int fd, const std::string &path)
{
    uint64_t last = 0;
    if (path == "/preview") {
        std::vector<uint8_t> frame;
        if (backend_.waitForPreview(frame, last, std::chrono::milliseconds(500))) {
            std::ostringstream oss;
            oss << "HTTP/1.1 200 OK\r\n"
                << "Content-Type: image/x-portable-pixmap\r\n"
                << "Content-Length: " << frame.size() << "\r\n"
                << "Cache-Control: no-cache\r\n"
                << "Connection: close\r\n\r\n";
            std::string header = oss.str();
            ::send(fd, header.data(), header.size(), 0);
            if (!frame.empty())
                ::send(fd, reinterpret_cast<const char *>(frame.data()), frame.size(), 0);
        } else {
            sendNotFound(fd);
        }
    } else {
        std::string header =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: multipart/x-mixed-replace; boundary=frame\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: close\r\n\r\n";
        ::send(fd, header.data(), header.size(), 0);
        while (backend_.isRunning()) {
            std::vector<uint8_t> frame;
            if (!backend_.waitForPreview(frame, last, std::chrono::milliseconds(1000)))
                continue;
            std::ostringstream part;
            part << "--frame\r\n"
                 << "Content-Type: image/x-portable-pixmap\r\n"
                 << "Content-Length: " << frame.size() << "\r\n\r\n";
            std::string hdr = part.str();
            if (::send(fd, hdr.data(), hdr.size(), 0) <= 0)
                break;
            if (::send(fd, reinterpret_cast<const char *>(frame.data()), frame.size(), 0) <= 0)
                break;
            const char *crlf = "\r\n";
            if (::send(fd, crlf, 2, 0) <= 0)
                break;
        }
        const char *end = "--frame--\r\n";
        ::send(fd, end, std::strlen(end), 0);
    }
}

void HttpServer::handleStatus(int fd)
{
    std::string body = backend_.statusJson();
    sendResponse(fd, "200 OK", "application/json", body);
}

void HttpServer::handleCapture(int fd)
{
    backend_.requestSingleCapture();
    sendResponse(fd, "202 Accepted", "application/json", "{\"status\":\"queued\"}");
}

void HttpServer::handleSettings(int fd, const std::map<std::string, std::string> &params)
{
    std::string message;
    if (backend_.updateSettings(params, message))
        sendResponse(fd, "200 OK", "application/json", message);
    else
        sendResponse(fd, "400 Bad Request", "application/json",
                     std::string("{\"error\":\"") + message + "\"}");
}

void HttpServer::sendResponse(int fd, const std::string &status,
                              const std::string &contentType, const std::string &body)
{
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status << "\r\n"
        << "Content-Type: " << contentType << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n"
        << body;
    std::string resp = oss.str();
    ::send(fd, resp.data(), resp.size(), 0);
}

void HttpServer::sendNotFound(int fd)
{
    sendResponse(fd, "404 Not Found", "text/plain", "not found\n");
}

struct ProgramOptions {
    bool showHelp{false};
    bool showVersion{false};
    bool daemon{false};
    std::string listen{"0.0.0.0:8080"};
    std::string outputDir{"./frames"};
    CaptureSettings settings{};
    int shots{-1};
    unsigned previewEvery{1};
    unsigned previewDownscale{2};
    bool previewEnabled{true};
    std::optional<std::pair<unsigned, unsigned>> sizeOverride;
};

static void print_help()
{
    std::cout << "Usage: dng-recorder [options] [output_directory]\n\n"
              << "Options:\n"
              << "  --daemon                 run as background backend service\n"
              << "  --listen host:port       HTTP listen address (default 0.0.0.0:8080)\n"
              << "  --fps <fps>              target frames per second\n"
              << "  --shutter <microsec>     exposure time in microseconds\n"
              << "  --gain <gain>            analogue gain\n"
              << "  --iso <iso>              ISO value (mapped to analogue gain)\n"
              << "  --ae <0|1>               disable auto-exposure if set to 1\n"
              << "  --awb <0|1>              disable auto white balance if set to 1\n"
              << "  --shots <n>              capture N frames then exit (CLI mode)\n"
              << "  --preview-every <n>      update preview every n-th frame\n"
              << "  --preview-scale <n>      preview downscale factor (>=2)\n"
              << "  --no-preview             disable preview generation (CLI only)\n"
              << "  --size WxH               request RAW size\n"
              << "  --help, -h               show this help and exit\n"
              << "  --version, -v            show version information and exit\n";
}

static bool parse_size(const std::string &value, std::pair<unsigned, unsigned> &out)
{
    unsigned w = 0, h = 0;
    if (std::sscanf(value.c_str(), "%ux%u", &w, &h) == 2 && w > 0 && h > 0) {
        out = {w, h};
        return true;
    }
    return false;
}

static ProgramOptions parse_options(int argc, char **argv, bool &earlyExit, int &exitCode)
{
    ProgramOptions opt;
    earlyExit = false;
    exitCode = 0;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_help();
            earlyExit = true;
            return opt;
        } else if (arg == "--version" || arg == "-v") {
            std::cout << "dng-recorder version " << DNG_RECORDER_VERSION << std::endl;
            earlyExit = true;
            return opt;
        } else if (arg == "--daemon") {
            opt.daemon = true;
            opt.previewEnabled = true;
            opt.shots = 0;
        } else if (arg == "--listen" && i + 1 < argc) {
            opt.listen = argv[++i];
        } else if (arg == "--fps" && i + 1 < argc) {
            try {
                opt.settings.fps = std::stod(argv[++i]);
            } catch (const std::exception &) {
                std::cerr << "Invalid fps value\n";
                exitCode = 1;
                earlyExit = true;
                return opt;
            }
        } else if (arg == "--shutter" && i + 1 < argc) {
            try {
                opt.settings.exposureUs = std::stoll(argv[++i]);
            } catch (const std::exception &) {
                std::cerr << "Invalid shutter value\n";
                exitCode = 1;
                earlyExit = true;
                return opt;
            }
        } else if (arg == "--gain" && i + 1 < argc) {
            try {
                opt.settings.gain = std::stof(argv[++i]);
            } catch (const std::exception &) {
                std::cerr << "Invalid gain value\n";
                exitCode = 1;
                earlyExit = true;
                return opt;
            }
        } else if (arg == "--iso" && i + 1 < argc) {
            try {
                opt.settings.iso = std::stof(argv[++i]);
            } catch (const std::exception &) {
                std::cerr << "Invalid ISO value\n";
                exitCode = 1;
                earlyExit = true;
                return opt;
            }
        } else if (arg == "--ae" && i + 1 < argc) {
            int v = std::atoi(argv[++i]);
            opt.settings.autoExposure = (v == 0);
        } else if (arg == "--awb" && i + 1 < argc) {
            int v = std::atoi(argv[++i]);
            opt.settings.autoWhiteBalance = (v == 0);
        } else if (arg == "--shots" && i + 1 < argc) {
            opt.shots = std::atoi(argv[++i]);
        } else if (arg == "--preview-every" && i + 1 < argc) {
            int v = std::max(1, std::atoi(argv[++i]));
            opt.previewEvery = static_cast<unsigned>(v);
        } else if (arg == "--preview-scale" && i + 1 < argc) {
            int v = std::max(2, std::atoi(argv[++i]));
            opt.previewDownscale = static_cast<unsigned>(v);
        } else if (arg == "--no-preview") {
            opt.previewEnabled = false;
        } else if (arg == "--size" && i + 1 < argc) {
            std::pair<unsigned, unsigned> size;
            if (parse_size(argv[++i], size))
                opt.sizeOverride = size;
        } else if (arg.rfind("--", 0) == 0) {
            std::cerr << "Unknown option: " << arg << std::endl;
            exitCode = 1;
            earlyExit = true;
            return opt;
        } else {
            opt.outputDir = arg;
        }
    }

    if (opt.daemon) {
        opt.previewEnabled = true;
    }

    return opt;
}

} // namespace

int run_raw_recorder(int argc, char **argv)
{
    bool earlyExit = false;
    int exitCode = 0;
    ProgramOptions opt = parse_options(argc, argv, earlyExit, exitCode);
    if (earlyExit)
        return exitCode;

    CameraBackend::Config cfg;
    cfg.outputDir = opt.outputDir;
    cfg.daemonMode = opt.daemon;
    cfg.previewEvery = opt.previewEvery;
    cfg.previewDownscale = opt.previewDownscale;
    cfg.previewEnabled = opt.previewEnabled;
    cfg.shotLimit = opt.shots;
    cfg.continuous = !opt.daemon;
    cfg.initialSettings = opt.settings;
    cfg.sizeOverride = opt.sizeOverride;

    CameraBackend backend(cfg);
    if (!backend.init())
        return 1;
    if (!backend.start())
        return 1;

    static std::atomic<bool> globalRunning{true};
    globalRunning.store(true);
    auto signalHandler = [](int) {
        globalRunning = false;
    };
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    if (opt.daemon) {
        HttpServer server(backend, opt.listen);
        if (!server.start()) {
            backend.stop();
            return 1;
        }
        while (globalRunning.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        server.stop();
    } else {
        backend.setContinuous(true, opt.shots);
        backend.runCliLoop(globalRunning);
    }

    backend.signalStop();
    backend.stop();
    return 0;
}
