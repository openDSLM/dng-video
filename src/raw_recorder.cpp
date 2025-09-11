// raw2dng_libcamera_preview.cpp — high-throughput RAW capture → DNG + optional HW NV12 preview
// - Default capture is packed 12-bit (SRGGB12_CSI2P) if available; env RAW_FMT can override.
// - Async DNG writer with pre-allocated image pool (no per-frame allocations).
// - Hardware NV12 preview (Viewfinder) only, fully droppable; push every Nth frame via PREVIEW_EVERY.
// - StreamRole for RAW selectable via feature flag or env (default: VideoRecording).
// - Per-frame metadata logging (exposure / frame duration) to diagnose FPS caps.
// - Env overrides:
//     PREVIEW=1                  (enable NV12 preview stream → GStreamer)
//     PREVIEW_SIZE=960x540       (preview stream size; default 960x540)
//     PREVIEW_EVERY=1            (push every Nth preview frame; default 1 i.e. every frame)
//     PREVIEW_SINK=gl|kmssink    (default gl → glimagesink; "kmssink" for DRM/KMS)
//     AE=1                       (disable AE if control is supported)
//     AWB=1                      (disable AWB if control is supported)
//     FPS=25                     (target fps)
//     EXP_US=20000               (exposure in microseconds)
//     AGAIN=16.0                 (analogue gain)
//     SIZE=3856x2180             (RAW stream size)
//     BUFCOUNT=24                (minimum buffers per stream; default 12)
//     SAVE_POOL=64               (# of preallocated DNG image buffers; default 32)
//     RAW_FMT=SRGGB12_CSI2P|SRGGB12|SRGGB16|12|12P|16
// - RPi quirk: FrameDurationLimits must be set in MICROseconds.
//
// Build (with preview):
//   g++ -O3 -march=native -DNDEBUG raw2dng_libcamera_preview.cpp -o raw2dng_libcamera \
//       $(pkg-config --cflags --libs libcamera) \
//       $(pkg-config --cflags --libs gstreamer-1.0 gstreamer-app-1.0) -ltiff -lpthread
//
// Build (without preview support):
//   g++ -O3 -march=native -DNDEBUG -DNO_GST raw2dng_libcamera_preview.cpp -o raw2dng_libcamera \
//       $(pkg-config --cflags --libs libcamera) -ltiff -lpthread
//
// Example:
//   RAW_FMT=SRGGB12_CSI2P AE=1 FPS=25 EXP_US=20000 AGAIN=16.0 SIZE=3856x2180 \
//   PREVIEW=1 PREVIEW_SIZE=480x270 PREVIEW_EVERY=2 \
//   ./raw2dng_libcamera /ssd/frames

#include <libcamera/libcamera.h>
#include <tiffio.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <deque>
#include <cctype>
#include <array>
#include <algorithm>

#ifndef NO_GST
#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#endif

using namespace libcamera;
using SteadyClock = std::chrono::steady_clock;

// ===== Feature flag: default to VideoRecording unless overridden =====
#ifndef USE_VIDEO_ROLE
#define USE_VIDEO_ROLE 1
#endif

// ===== Globals =====
static std::atomic<bool> g_running{true};
static std::atomic<bool> g_accept_requeue{true};
static std::atomic<uint64_t> g_frames{0}, g_saved{0};

// Optional env-driven overrides
static std::atomic<int64_t> g_targetFrameDurNs{-1}; // FPS=25 -> 40,000,000 ns
static std::atomic<int32_t> g_targetExposureUs{-1}; // EXP_US=20000 (µs)
static std::atomic<int>     g_aeOff{0};             // AE=1 disables auto-exposure (if supported)
static std::atomic<int>     g_awbOff{0};            // AWB=1 disables auto white balance (if supported)
static std::atomic<float>   g_again{0.0f};          // AGAIN=16.0 (analogue gain)

static std::atomic<int>     g_preview{0};           // PREVIEW=1 enables NV12 preview
static int                  g_prevW=960, g_prevH=540; // PREVIEW_SIZE
static std::string          g_prevSink = "gl";      // PREVIEW_SINK
static std::atomic<int>     g_prevEvery{1};         // PREVIEW_EVERY=N

static void sigint_handler(int) { g_running = false; }

// ===== RAW12 → 16 Bit (stride-aware) =====
static void unpack_raw12_to_u16(const uint8_t *in, uint16_t *out,
                                unsigned width, unsigned height, unsigned strideBytes)
{
    for (unsigned y = 0; y < height; ++y) {
        const uint8_t *row = in + size_t(y) * strideBytes;
        unsigned i = 0, j = 0;
        while (i < width) {
            uint8_t b0 = row[j+0], b1 = row[j+1], b2 = row[j+2];
            uint8_t b3 = row[j+3], b4 = row[j+4], b5 = row[j+5];
            uint16_t p0 = uint16_t(b0 | ((b1 & 0x0F) << 8));
            uint16_t p1 = uint16_t((b1 >> 4) | (b2 << 4));
            uint16_t p2 = uint16_t(b3 | ((b4 & 0x0F) << 8));
            uint16_t p3 = uint16_t((b4 >> 4) | (b5 << 4));
            const size_t base = size_t(y) * width + i;
            out[base + 0] = p0;
            if (i + 1 < width) out[base + 1] = p1;
            if (i + 2 < width) out[base + 2] = p2;
            if (i + 3 < width) out[base + 3] = p3;
            i += 4; j += 6;
        }
    }
}

// ===== Minimal DNG/TIFF writer (single-strip, 16-bit) =====
static bool write_dng16(const std::string &path,
                        const uint16_t *data,
                        uint32_t width,
                        uint32_t height,
                        const std::string &cfa,
                        uint16_t /*blackLevel*/ = 0,
                        uint32_t /*whiteLevel*/ = 0)
{
    TIFF *tif = TIFFOpen(path.c_str(), "w");
    if (!tif) { std::perror("TIFFOpen"); return false; }

    TIFFSetField(tif, TIFFTAG_IMAGEWIDTH, width);
    TIFFSetField(tif, TIFFTAG_IMAGELENGTH, height);
    TIFFSetField(tif, TIFFTAG_SAMPLESPERPIXEL, 1);
    TIFFSetField(tif, TIFFTAG_BITSPERSAMPLE, 16);
    TIFFSetField(tif, TIFFTAG_COMPRESSION, COMPRESSION_NONE);
    TIFFSetField(tif, TIFFTAG_PHOTOMETRIC, PHOTOMETRIC_CFA);
    TIFFSetField(tif, TIFFTAG_PLANARCONFIG, PLANARCONFIG_CONTIG);
    TIFFSetField(tif, TIFFTAG_ORIENTATION, ORIENTATION_TOPLEFT);

    // CFA pattern (2x2)
    uint8_t patt[4] = {0,1,1,2}; // default RGGB
    std::string f = cfa;
    for (char &ch : f) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    if      (f == "RGGB") { const uint8_t p[4] = {0,1,1,2}; std::memcpy(patt, p, 4); }
    else if (f == "BGGR") { const uint8_t p[4] = {2,1,1,0}; std::memcpy(patt, p, 4); }
    else if (f == "GRBG") { const uint8_t p[4] = {1,0,2,1}; std::memcpy(patt, p, 4); }
    else if (f == "GBRG") { const uint8_t p[4] = {1,2,0,1}; std::memcpy(patt, p, 4); }

    uint16_t dim[2] = {2, 2};
    TIFFSetField(tif, TIFFTAG_CFAREPEATPATTERNDIM, dim);
    TIFFSetField(tif, TIFFTAG_CFAPATTERN, 4, patt);

    // One big strip → fewer syscalls
    TIFFSetField(tif, TIFFTAG_ROWSPERSTRIP, height);
    const tsize_t bytes = tsize_t(width) * tsize_t(height) * 2;
    if (TIFFWriteEncodedStrip(tif, 0, (void*)data, bytes) < 0) {
        std::fprintf(stderr, "TIFFWriteEncodedStrip failed\n");
        TIFFClose(tif);
        return false;
    }

    TIFFClose(tif);
    return true;
}

static std::string cfa_from_fourcc(const PixelFormat &pf)
{
    const std::string s = pf.toString();
    if (s.find("RGGB") != std::string::npos || s.find("rggb") != std::string::npos) return "RGGB";
    if (s.find("BGGR") != std::string::npos) return "BGGR";
    if (s.find("GRBG") != std::string::npos) return "GRBG";
    if (s.find("GBRG") != std::string::npos) return "GBRG";
    return "RGGB";
}

// ===== mmap helpers =====
struct MappedPlane {
    void*    base{};        // page-aligned mmap base
    size_t   map_len{};     // length actually mapped (includes delta)
    off_t    aligned_off{}; // aligned offset passed to mmap
    int      fd{-1};
    uint8_t* data{};        // plane start (base + delta to original offset)
    size_t   data_len{};    // original plane length
};
struct MappedBuffer {
    std::vector<MappedPlane> planes;
    ~MappedBuffer() {
        for (auto &p : planes)
            if (p.base && p.map_len) ::munmap(p.base, p.map_len);
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
        off_t aligned = off & ~static_cast<off_t>(page - 1); // page-align down
        size_t delta = static_cast<size_t>(off - aligned);
        size_t map_len = static_cast<size_t>(pl.length) + delta;

        void *base = ::mmap(nullptr, map_len, PROT_READ, MAP_SHARED, fd, aligned);
        if (base == MAP_FAILED) { std::perror("mmap"); return false; }

        map.planes[i].base        = base;
        map.planes[i].map_len     = map_len;
        map.planes[i].aligned_off = aligned;
        map.planes[i].fd          = fd;
        map.planes[i].data        = static_cast<uint8_t*>(base) + delta;
        map.planes[i].data_len    = static_cast<size_t>(pl.length);
    }
    return true;
}

// ===== context =====
struct Ctx {
    std::shared_ptr<Camera> cam;
    const Stream *streamRaw{};     // RAW stream
    const Stream *streamPv{};      // optional preview stream (NV12)
    Size sizeRaw{};
    Size sizePv{};
    PixelFormat pixRaw{};
    PixelFormat pixPv{};
    std::string cfa{};
    bool rawIsPacked12{false};
    bool rawIs16{false};
    std::string outDir;
    FrameBufferAllocator *alloc{};
    std::map<const FrameBuffer*, std::unique_ptr<MappedBuffer>> mappings;
    bool haveAeEnable{false};
    bool haveAwbEnable{false};
};
static Ctx *g_ctx = nullptr;

// ===== async writer + image pool =====
static std::mutex qmtx;
static std::condition_variable qcv;
// queue holds (poolSlot, frameIndex)
static std::queue<std::pair<size_t, uint64_t>> q;
static std::atomic<bool> writer_run{true};

// Pool of pre-allocated DNG images
static std::vector<std::vector<uint16_t>> g_imgPool;
static std::mutex pool_mtx;
static std::queue<size_t> pool_free;

// Track dropped-saves (RAW never drops; we only skip saving if pool is full)
static std::atomic<uint64_t> g_dropSaved{0};

static void init_image_pool(size_t poolN, uint32_t w, uint32_t h)
{
    std::lock_guard<std::mutex> lk(pool_mtx);
    g_imgPool.resize(poolN);
    while (!pool_free.empty()) pool_free.pop();
    for (size_t i = 0; i < poolN; ++i) {
        g_imgPool[i].assign(size_t(w) * size_t(h), 0);
        pool_free.push(i);
    }
}

static void writer_thread(std::string outDir, uint32_t w, uint32_t h, std::string cfa)
{
    for (;;) {
        std::pair<size_t, uint64_t> job;
        {
            std::unique_lock<std::mutex> lk(qmtx);
            qcv.wait(lk, []{ return !q.empty() || !writer_run.load(); });
            if (q.empty()) {
                if (!writer_run.load()) break;
                continue;
            }
            job = q.front();
            q.pop();
        }

        const size_t slot = job.first;
        const uint64_t idx = job.second;

        const auto &img = g_imgPool[slot];
        char name[256];
        std::snprintf(name, sizeof(name), "frame_%06llu.dng", (unsigned long long)idx);
        const std::string path = (std::filesystem::path(outDir) / name).string();
        if (write_dng16(path, img.data(), w, h, cfa))
            ++g_saved;

        // Return slot to pool
        {
            std::lock_guard<std::mutex> lk(pool_mtx);
            pool_free.push(slot);
        }
    }
}

// ===== GStreamer preview (optional, hardware NV12, fully droppable) =====
#ifndef NO_GST
static GstElement *g_pipeline = nullptr;
static GstElement *g_appsrc   = nullptr;
static GMainLoop  *g_gst_loop = nullptr;
static std::thread g_gst_thread;

// Small worker that pushes to appsrc so the camera callback never blocks.
static std::mutex pv_mtx;
static std::condition_variable pv_cv;
static std::deque<std::vector<uint8_t>> pv_q;
static std::atomic<bool> pv_run{false};
static std::thread pv_worker;
static size_t pv_q_max = 2;  // keep at most 2 tight NV12 frames queued

static void gst_loop_thread() {
    g_gst_loop = g_main_loop_new(nullptr, FALSE);
    g_main_loop_run(g_gst_loop);
}

static void pv_worker_loop() {
    while (true) {
        std::vector<uint8_t> buf;
        {
            std::unique_lock<std::mutex> lk(pv_mtx);
            pv_cv.wait(lk, []{ return !pv_run.load() || !pv_q.empty(); });
            if (!pv_run.load() && pv_q.empty()) break;
            buf = std::move(pv_q.front());
            pv_q.pop_front();
        }
        if (!g_appsrc || buf.empty()) continue;

        GstBuffer *gstbuf = gst_buffer_new_allocate(nullptr, buf.size(), nullptr);
        if (!gstbuf) continue;
        GstMapInfo map;
        if (gst_buffer_map(gstbuf, &map, GST_MAP_WRITE)) {
            std::memcpy(map.data, buf.data(), buf.size());
            gst_buffer_unmap(gstbuf, &map);
            (void)gst_app_src_push_buffer(GST_APP_SRC(g_appsrc), gstbuf); // may drop, never block
        } else {
            gst_buffer_unref(gstbuf);
        }
    }
}

static bool preview_start(int width, int height, double fps) {
    if (!g_preview.load()) return false;
    if (g_pipeline) return true;

    gst_init(nullptr, nullptr);

    // appsrc (non-blocking) → tiny leaky queue → sink
    std::string sink = g_prevSink;
    std::string sinkElem = (sink == "kmssink") ? "kmssink" : "glimagesink"; // default
    std::string launch =
        "appsrc name=src is-live=true format=time do-timestamp=true "
        "! queue max-size-buffers=2 max-size-bytes=0 max-size-time=0 leaky=downstream "
        "! " + sinkElem + " sync=false";

    GError *err = nullptr;
    g_pipeline = gst_parse_launch(launch.c_str(), &err);
    if (err) {
        std::fprintf(stderr, "GStreamer parse error: %s\n", err->message);
        g_error_free(err);
        return false;
    }

    g_appsrc = gst_bin_get_by_name(GST_BIN(g_pipeline), "src");
    if (!g_appsrc) { std::fprintf(stderr, "Failed to get appsrc\n"); return false; }

    // Make appsrc itself non-blocking and cap its internal queue.
    guint maxbytes = static_cast<guint>(width * height * 3 / 2 * 2); // ~2 frames
    g_object_set(G_OBJECT(g_appsrc),
                 "block", FALSE,
                 "max-bytes", maxbytes,
                 nullptr);

    GstCaps *caps = gst_caps_new_simple("video/x-raw",
        "format",  G_TYPE_STRING, "NV12",
        "width",   G_TYPE_INT,    width,
        "height",  G_TYPE_INT,    height,
        "framerate", GST_TYPE_FRACTION, (int)fps, 1,
        nullptr);
    g_object_set(G_OBJECT(g_appsrc), "caps", caps, NULL);
    gst_caps_unref(caps);

    // Start GLib main loop for sinks that need it
    g_gst_thread = std::thread(gst_loop_thread);
    gst_element_set_state(g_pipeline, GST_STATE_PLAYING);

    // Start the non-blocking push worker
    pv_run = true;
    pv_worker = std::thread(pv_worker_loop);
    return true;
}

static void preview_stop() {
    // Stop worker first
    pv_run = false;
    pv_cv.notify_all();
    if (pv_worker.joinable()) pv_worker.join();
    {
        std::lock_guard<std::mutex> lk(pv_mtx);
        pv_q.clear();
    }

    if (g_pipeline) {
        gst_element_set_state(g_pipeline, GST_STATE_NULL);
    }
    if (g_gst_loop) g_main_loop_quit(g_gst_loop);
    if (g_gst_thread.joinable()) g_gst_thread.join();

    if (g_appsrc) { gst_object_unref(g_appsrc); g_appsrc = nullptr; }
    if (g_pipeline) { gst_object_unref(g_pipeline); g_pipeline = nullptr; }
    if (g_gst_loop) { g_main_loop_unref(g_gst_loop); g_gst_loop = nullptr; }
}

/**
 * Queue a tight NV12 buffer for the preview worker.
 * If the queue is full, we DROP IMMEDIATELY (no gst calls in the callback).
 */
static inline void preview_push(const uint8_t *y, unsigned strideY,
                                const uint8_t *uv, unsigned strideUV,
                                unsigned w, unsigned h)
{
    if (!g_appsrc) return;

    // Fast drop if the queue is full to protect RAW cadence.
    {
        std::lock_guard<std::mutex> lk(pv_mtx);
        if (pv_q.size() >= pv_q_max) return;
    }

    const size_t ySize  = size_t(w) * size_t(h);
    const size_t uvSize = size_t(w) * size_t(h/2);
    const size_t total  = ySize + uvSize;

    std::vector<uint8_t> tight(total);

    // Copy with strides into tight NV12 buffer (YYYY.. UVUV..)
    uint8_t *dstY = tight.data();
    for (unsigned row = 0; row < h; ++row)
        std::memcpy(dstY + size_t(row) * w, y + size_t(row) * strideY, w);

    uint8_t *dstUV = tight.data() + ySize;
    for (unsigned row = 0; row < h/2; ++row)
        std::memcpy(dstUV + size_t(row) * w, uv + size_t(row) * strideUV, w);

    {
        std::lock_guard<std::mutex> lk(pv_mtx);
        if (pv_q.size() < pv_q_max) {
            pv_q.emplace_back(std::move(tight));
            pv_cv.notify_one();
        }
        // else: queue filled while we were copying — drop this frame
    }
}
#endif

// Only requeue when allowed
static inline void safe_requeue(Request *req) {
    if (g_accept_requeue.load(std::memory_order_relaxed) && g_ctx && g_ctx->cam)
        g_ctx->cam->queueRequest(req);
}

// ===== callback =====
static void on_request_completed(Request *req)
{
    if (!g_ctx) return;
    if (req->status() == Request::RequestCancelled) return;

    const uint64_t idx = ++g_frames;

    // --- RAW buffer path ---
    auto itRaw = g_ctx->streamRaw ? req->buffers().find(g_ctx->streamRaw) : req->buffers().end();
    if (itRaw == req->buffers().end()) {
        std::fprintf(stderr, "REQ%llu: no RAW buffer for stream\n", (unsigned long long)idx);
        req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
    }
    const FrameBuffer *fbRaw = itRaw->second;
    auto itMapRaw = g_ctx->mappings.find(fbRaw);
    if (itMapRaw == g_ctx->mappings.end()) {
        std::fprintf(stderr, "REQ%llu: no RAW mapping found\n", (unsigned long long)idx);
        req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
    }

    const MappedBuffer &mbRaw = *itMapRaw->second;
    if (fbRaw->planes().empty() || mbRaw.planes.empty() || !mbRaw.planes[0].data) {
        std::fprintf(stderr, "REQ%llu: invalid RAW planes\n", (unsigned long long)idx);
        req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
    }

    const FrameBuffer::Plane &plR = fbRaw->planes()[0];
    const uint8_t *srcR = mbRaw.planes[0].data;
    const unsigned wR = g_ctx->sizeRaw.width;
    const unsigned hR = g_ctx->sizeRaw.height;
    const size_t planeLenR = plR.length;
    if (planeLenR == 0 || hR == 0) {
        std::fprintf(stderr, "REQ%llu: bad RAW plane len/h\n", (unsigned long long)idx);
        req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
    }

    const unsigned strideBytesR = unsigned(planeLenR / hR);

    // --- Per-frame metadata (first few frames) ---
    if (idx <= 10) {
        const ControlList &md = req->metadata();
        if (auto exp = md.get(controls::ExposureTime)) {
            std::fprintf(stderr, "MD%llu: ExposureTime=%d us\n",
                         (unsigned long long)idx, *exp);
        }
        if (auto fd = md.get(controls::FrameDuration)) {
            long long v = (long long)*fd;         // expected ns; some stacks report µs
            if (v < 1'000'000) v *= 1000;         // µs → ns if too small
            double fps = v > 0 ? (1e9 / (double)v) : 0.0;
            std::fprintf(stderr, "MD%llu: FrameDuration=%lld ns (%.2f fps)\n",
                         (unsigned long long)idx, v, fps);
        }
    }

    if (idx <= 3)
        std::fprintf(stderr, "REQ%llu: RAW w=%u h=%u planeLen=%zu stride=%u fmt=%s\n",
                     (unsigned long long)idx, wR, hR, planeLenR, strideBytesR, g_ctx->pixRaw.toString().c_str());

    // ---- Acquire pool slot (skip save if pool is temporarily full) ----
    size_t slot = SIZE_MAX;
    {
        std::lock_guard<std::mutex> lk(pool_mtx);
        if (!pool_free.empty()) { slot = pool_free.front(); pool_free.pop(); }
    }

    if (slot != SIZE_MAX) {
        uint16_t *dst = g_imgPool[slot].data();

        if (g_ctx->rawIs16) {
            const unsigned needBytes = wR * 2;
            if (strideBytesR < needBytes) {
                std::fprintf(stderr, "REQ%llu: RAW stride too small (%u<%u)\n",
                             (unsigned long long)idx, strideBytesR, needBytes);
                std::lock_guard<std::mutex> lk(pool_mtx);
                pool_free.push(slot);
                req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
            }
            for (unsigned y = 0; y < hR; ++y) {
                const uint8_t *srcRow = srcR + size_t(y) * strideBytesR;
                std::memcpy(reinterpret_cast<uint8_t *>(dst + size_t(y) * wR), srcRow, needBytes);
            }
        } else if (g_ctx->rawIsPacked12) {
            unpack_raw12_to_u16(srcR, dst, wR, hR, strideBytesR);
            for (size_t i = 0, n = size_t(wR) * size_t(hR); i < n; ++i) dst[i] <<= 4; // 12→16
        } else {
            std::fprintf(stderr, "REQ%llu: unsupported RAW packing\n", (unsigned long long)idx);
            std::lock_guard<std::mutex> lk(pool_mtx);
            pool_free.push(slot);
            req->reuse(Request::ReuseBuffers); safe_requeue(req); return;
        }

        // Enqueue save job (slot, frameIdx)
        {
            std::lock_guard<std::mutex> lk(qmtx);
            q.emplace(slot, idx);
        }
        qcv.notify_one();
    } else {
        // No free pool slot → skip saving this frame (but DO NOT block RAW)
        ++g_dropSaved;
    }

#ifndef NO_GST
    // --- Preview path (hardware viewfinder) ---
    if (g_preview.load() && g_ctx->streamPv) {
        // Only push every Nth preview frame to reduce CPU
        if ((idx % g_prevEvery.load()) == 0) {
            auto itPv = req->buffers().find(g_ctx->streamPv);
            if (itPv != req->buffers().end()) {
                const FrameBuffer *fbPv = itPv->second;
                auto itMapPv = g_ctx->mappings.find(fbPv);
                if (itMapPv != g_ctx->mappings.end()) {
                    const MappedBuffer &mbPv = *itMapPv->second;
                    if (fbPv->planes().size() >= 2 && mbPv.planes.size() >= 2) {
                        const FrameBuffer::Plane &plY  = fbPv->planes()[0];
                        const FrameBuffer::Plane &plUV = fbPv->planes()[1];
                        const uint8_t *srcY  = mbPv.planes[0].data;
                        const uint8_t *srcUV = mbPv.planes[1].data;
                        unsigned w = g_ctx->sizePv.width;
                        unsigned h = g_ctx->sizePv.height;
                        unsigned strideY  = (h ? unsigned(plY.length / h) : 0);
                        unsigned strideUV = (h ? unsigned(plUV.length / (h/2)) : 0);
                        if (strideY && strideUV) {
                            preview_push(srcY, strideY, srcUV, strideUV, w, h);
                        }
                    }
                }
            }
        }
    }
#endif

    // Recycle request (keep buffers)
    req->reuse(Request::ReuseBuffers);

    // ---- Optional per-request overrides (from env) ----
    if (g_awbOff.load() == 1 && g_ctx->haveAwbEnable)
        req->controls().set(controls::AwbEnable, false);

    if (g_targetExposureUs.load() > 0)
        req->controls().set(controls::ExposureTime, g_targetExposureUs.load());

    if (g_again.load() > 0.0f)
        req->controls().set(controls::AnalogueGain, g_again.load());

    if (g_targetFrameDurNs.load() > 0) {
        // RPi expects microseconds for FrameDurationLimits (min,max)
        int64_t fd_us = g_targetFrameDurNs.load() / 1000;  // ns → µs
        if (fd_us < 1) fd_us = 1;
        int64_t lim[2] = { fd_us, fd_us };
        req->controls().set(controls::FrameDurationLimits, Span<const int64_t, 2>(lim));
    }

    safe_requeue(req);
}

// ===== helpers =====
static StreamRole choose_role_from_env_or_flag()
{
    const char *env = std::getenv("STREAM_ROLE");
    if (env) {
        std::string s(env);
        for (auto &c : s) c = std::tolower(c);
        if (s == "still" || s == "stillcapture") return StreamRole::StillCapture;
        if (s == "video" || s == "videorecording") return StreamRole::VideoRecording;
    }
#if USE_VIDEO_ROLE
    return StreamRole::VideoRecording;
#else
    return StreamRole::StillCapture;
#endif
}

int run_raw_recorder(int argc, char **argv)
{
    std::signal(SIGINT, sigint_handler);
    std::signal(SIGTERM, sigint_handler);

    const std::string outDir = (argc > 1) ? argv[1] : "./frames";
    std::filesystem::create_directories(outDir);

    int wantedWidth = 0, wantedHeight = 0;
    if (const char *env = std::getenv("SIZE")) {
        int w=0,h=0; if (std::sscanf(env, "%dx%d", &w, &h) == 2) { wantedWidth=w; wantedHeight=h; }
    }

    // Optional FPS / Exposure / AE/AWB / Gain / Preview overrides via env
    if (const char *fpsEnv = std::getenv("FPS")) {
        double fps = std::atof(fpsEnv);
        if (fps > 0.1) g_targetFrameDurNs = (int64_t)(1e9 / fps);
    }
    if (const char *expEnv = std::getenv("EXP_US")) {
        int exp = std::atoi(expEnv);
        if (exp > 0) g_targetExposureUs = exp;
    }
    if (const char *ae = std::getenv("AE"))   g_aeOff  = (std::atoi(ae)  == 0 ? 0 : 1);
    if (const char *aw = std::getenv("AWB"))  g_awbOff = (std::atoi(aw)  == 0 ? 0 : 1);
    if (const char *ag = std::getenv("AGAIN")) g_again = std::atof(ag);

    if (const char *pv = std::getenv("PREVIEW")) g_preview = (std::atoi(pv) == 0 ? 0 : 1);
    if (const char *pvs = std::getenv("PREVIEW_SIZE")) { int w=0,h=0; if (std::sscanf(pvs, "%dx%d", &w, &h)==2) { g_prevW=w; g_prevH=h; }}
    if (const char *pvsnk = std::getenv("PREVIEW_SINK")) { std::string s=pvsnk; for (auto &c:s) c=std::tolower(c); g_prevSink=s; }
    if (const char *pe = std::getenv("PREVIEW_EVERY")) {
        int v = std::max(1, std::atoi(pe));
        g_prevEvery = v;
    }

    CameraManager cm;
    if (cm.start()) { std::fprintf(stderr, "CameraManager start failed\n"); return 1; }
    if (cm.cameras().empty()) { std::fprintf(stderr, "No cameras found\n"); return 1; }
    std::shared_ptr<Camera> cam = cm.cameras()[0];
    if (cam->acquire()) { std::fprintf(stderr, "Camera acquire failed\n"); return 1; }

    // StreamRole (feature flag + runtime override)
    StreamRole roleRaw = choose_role_from_env_or_flag();

    std::unique_ptr<CameraConfiguration> config;
    if (g_preview.load()) config = cam->generateConfiguration({ roleRaw, StreamRole::Viewfinder });
    else                  config = cam->generateConfiguration({ roleRaw });

    if (!config || config->size() < 1) { std::fprintf(stderr, "generateConfiguration failed\n"); return 1; }

    // RAW cfg is always index 0
    StreamConfiguration &cfgRaw = config->at(0);

    const StreamFormats fmtsRaw = cfgRaw.formats();
    std::vector<PixelFormat> availableRaw = fmtsRaw.pixelformats();
    auto hasFmtRaw = [&](const PixelFormat &pf){ for (auto &x: availableRaw) if (x == pf) return true; return false; };

    std::fprintf(stderr, "Available RAW pixelformats (stream 0):\n");
    for (auto &pf : availableRaw) std::fprintf(stderr, "  - %s\n", pf.toString().c_str());

    // --- RAW pixel format selection ---
    PixelFormat forcedPf{};
    if (const char *rf = std::getenv("RAW_FMT")) {
        std::string s = rf;
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        if (s == "12" || s == "SRGGB12")
            forcedPf = formats::SRGGB12;
        else if (s == "12P" || s == "PACKED12" || s == "SRGGB12_CSI2P")
            forcedPf = formats::SRGGB12_CSI2P;
        else if (s == "16" || s == "SRGGB16")
            forcedPf = formats::SRGGB16;
    }

    if (forcedPf.isValid() && hasFmtRaw(forcedPf)) {
        cfgRaw.pixelFormat = forcedPf;
    } else {
        // Prefer 12-bit to reduce bandwidth (packed if available)
        if (hasFmtRaw(formats::SRGGB12_CSI2P))      cfgRaw.pixelFormat = formats::SRGGB12_CSI2P;
        else if (hasFmtRaw(formats::SRGGB12))       cfgRaw.pixelFormat = formats::SRGGB12;
        else if (hasFmtRaw(formats::SRGGB16))       cfgRaw.pixelFormat = formats::SRGGB16;
    }

    if (wantedWidth > 0 && wantedHeight > 0) cfgRaw.size = { (unsigned)wantedWidth, (unsigned)wantedHeight };

    // Optional preview stream (index 1)
    StreamConfiguration *cfgPv = nullptr;
    if (g_preview.load() && config->size() >= 2) {
        cfgPv = &config->at(1);
        // Pick NV12 if available, else any YUV420
        const StreamFormats fPv = cfgPv->formats();
        std::vector<PixelFormat> availPv = fPv.pixelformats();
        auto hasFmtPv = [&](const PixelFormat &pf){ for (auto &x: availPv) if (x == pf) return true; return false; };
        if (hasFmtPv(formats::NV12)) cfgPv->pixelFormat = formats::NV12;
        else if (hasFmtPv(formats::YUV420)) cfgPv->pixelFormat = formats::YUV420;
        cfgPv->size = { (unsigned)g_prevW, (unsigned)g_prevH };
    }

    // Allow more in-flight requests so copy/save never starves capture
    unsigned bufCount = 12;
    if (const char *bc = std::getenv("BUFCOUNT")) {
        int v = std::atoi(bc);
        if (v >= 4 && v <= 64) bufCount = (unsigned)v;
    }
    cfgRaw.bufferCount = std::max(cfgRaw.bufferCount, bufCount);
    if (cfgPv) cfgPv->bufferCount = std::max(cfgPv->bufferCount, bufCount);

    if (config->validate() == CameraConfiguration::Invalid) { std::fprintf(stderr, "Configuration invalid\n"); return 1; }

    // Reject PiSP compressed formats explicitly on RAW
    const std::string willFmt = cfgRaw.pixelFormat.toString();
    if (willFmt.find("PISP_COMP") != std::string::npos || willFmt.find("PC1") != std::string::npos) {
        std::fprintf(stderr, "ERROR: compressed PiSP RAW selected (%s). Use SRGGB16 or SRGGB12_CSI2P.\n", willFmt.c_str());
        return 1;
    }

    if (cam->configure(config.get())) { std::fprintf(stderr, "Camera configure failed\n"); return 1; }

    const Stream *streamRaw = cfgRaw.stream();
    const Size sizeRaw = cfgRaw.size;
    const PixelFormat pixRaw = cfgRaw.pixelFormat;
    const std::string cfa = cfa_from_fourcc(pixRaw);
    const bool isPacked12 = (pixRaw == formats::SRGGB12_CSI2P) || (pixRaw == formats::SRGGB12);
    const bool is16 = (pixRaw == formats::SRGGB16);

    const Stream *streamPv = nullptr;
    Size sizePv{}; PixelFormat pixPv{};
    if (cfgPv) {
        streamPv = cfgPv->stream();
        sizePv   = cfgPv->size;
        pixPv    = cfgPv->pixelFormat;
    }

    std::printf("Configured RAW stream: %s, %ux%u  CFA=%s\n",
                pixRaw.toString().c_str(), sizeRaw.width, sizeRaw.height, cfa.c_str());
    if (streamPv)
        std::printf("Configured PREVIEW stream: %s, %ux%u\n",
                    pixPv.toString().c_str(), sizePv.width, sizePv.height);

    // Pre-allocate save pool (default 32 frames; tunable via SAVE_POOL)
    size_t poolN = 32;
    if (const char *sp = std::getenv("SAVE_POOL")) {
        long long n = std::atoll(sp);
        if (n >= 8 && n <= 512) poolN = (size_t)n;
    }
    init_image_pool(poolN, sizeRaw.width, sizeRaw.height);

    FrameBufferAllocator alloc(cam);
    if (alloc.allocate(const_cast<Stream *>(streamRaw)) < 0) {
        std::fprintf(stderr, "FrameBufferAllocator failed (RAW)\n"); return 1;
    }
    if (streamPv) {
        if (alloc.allocate(const_cast<Stream *>(streamPv)) < 0) {
            std::fprintf(stderr, "FrameBufferAllocator failed (PREVIEW)\n"); return 1;
        }
    }

    auto &bufsRaw = alloc.buffers(const_cast<Stream *>(streamRaw));
    std::vector<std::unique_ptr<FrameBuffer>> emptyPv;
    auto &bufsPv  = streamPv ? alloc.buffers(const_cast<Stream *>(streamPv)) : emptyPv;

    std::fprintf(stderr, "Allocated %zu RAW buffers%s\n", bufsRaw.size(), streamPv ? "" : "");
    if (bufsRaw.empty()) { std::fprintf(stderr, "No RAW buffers allocated!\n"); return 1; }

    std::vector<std::unique_ptr<Request>> requests;

    std::map<const FrameBuffer *, std::unique_ptr<MappedBuffer>> mappings;
    // Number of requests = min(RAW, PREVIEW) when preview is enabled
    const size_t nReq = streamPv ? std::min(bufsRaw.size(), bufsPv.size()) : bufsRaw.size();

    for (size_t i = 0; i < nReq; ++i) {
        auto req = cam->createRequest();
        if (!req) { std::fprintf(stderr, "createRequest failed\n"); return 1; }
        if (req->addBuffer(streamRaw, bufsRaw[i].get()) < 0) { std::fprintf(stderr, "addBuffer RAW failed\n"); return 1; }
        if (streamPv) {
            if (req->addBuffer(streamPv, bufsPv[i].get()) < 0) { std::fprintf(stderr, "addBuffer PREVIEW failed\n"); return 1; }
        }
        requests.emplace_back(std::move(req));

        auto mapR = std::make_unique<MappedBuffer>();
        if (!mmapFrameBuffer(bufsRaw[i].get(), *mapR)) { std::fprintf(stderr, "mmap RAW failed\n"); return 1; }
        mappings.emplace(bufsRaw[i].get(), std::move(mapR));
        if (streamPv) {
            auto mapP = std::make_unique<MappedBuffer>();
            if (!mmapFrameBuffer(bufsPv[i].get(), *mapP)) { std::fprintf(stderr, "mmap PREVIEW failed\n"); return 1; }
            mappings.emplace(bufsPv[i].get(), std::move(mapP));
        }
    }

    Ctx ctx;
    ctx.cam = cam;
    ctx.streamRaw = streamRaw;
    ctx.streamPv  = streamPv;
    ctx.sizeRaw = sizeRaw;
    ctx.sizePv  = sizePv;
    ctx.pixRaw = pixRaw;
    ctx.pixPv  = pixPv;
    ctx.cfa = cfa;
    ctx.rawIsPacked12 = isPacked12;
    ctx.rawIs16 = is16;
    ctx.outDir = outDir;
    ctx.alloc = &alloc;
    ctx.mappings = std::move(mappings);

    // Discover if AeEnable/AwbEnable controls are actually supported → avoid warnings
    const ControlInfoMap &cinfo = cam->controls();
    ctx.haveAeEnable  = (cinfo.find(&controls::AeEnable)  != cinfo.end());
    ctx.haveAwbEnable = (cinfo.find(&controls::AwbEnable) != cinfo.end());

    g_ctx = &ctx;

    cam->requestCompleted.connect(&on_request_completed);

    // spin up writer thread
    std::thread wt(writer_thread, ctx.outDir, sizeRaw.width, sizeRaw.height, cfa);

#ifndef NO_GST
    // Start preview pipeline before camera to reduce startup latency
    double fpsForCaps = (g_targetFrameDurNs.load() > 0) ? (1e9 / (double)g_targetFrameDurNs.load()) : 30.0;
    if (streamPv) preview_start(sizePv.width, sizePv.height, fpsForCaps);
#endif

    // Start camera WITH initial controls (one-shot)
    ControlList initCtrls(cinfo);
    if (g_aeOff.load() == 1 && ctx.haveAeEnable)
        initCtrls.set(controls::AeEnable, false);
    if (g_awbOff.load() == 1 && ctx.haveAwbEnable)
        initCtrls.set(controls::AwbEnable, false);

    // Lock frame duration & exposure at startup (RPi wants µs for limits)
    if (g_targetFrameDurNs.load() > 0) {
        int64_t fd_us = g_targetFrameDurNs.load() / 1000;
        if (fd_us < 1) fd_us = 1;
        int64_t lim[2] = { fd_us, fd_us };
        initCtrls.set(controls::FrameDurationLimits, Span<const int64_t, 2>(lim));
    }
    if (g_targetExposureUs.load() > 0)
        initCtrls.set(controls::ExposureTime, g_targetExposureUs.load());
    if (g_again.load() > 0.0f)
        initCtrls.set(controls::AnalogueGain, g_again.load());

    if (cam->start(&initCtrls)) {
        std::fprintf(stderr, "Camera start failed\n");
        writer_run = false; qcv.notify_all();
        wt.join();
#ifndef NO_GST
        preview_stop();
#endif
        return 1;
    }

    // Queue all requests and log success count
    size_t queued = 0;
    for (auto &req : requests) {
        if (cam->queueRequest(req.get()) == 0) queued++;
        else std::fprintf(stderr, "queueRequest failed for one request\n");
    }
    std::fprintf(stderr, "Initially queued %zu requests\n", queued);

    auto last = SteadyClock::now();
    uint64_t lastF = 0, lastS = 0, lastD = 0;
    std::printf("Running… Writing DNGs to: %s\n", ctx.outDir.c_str());
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto now = SteadyClock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count();
        if (ms >= 1000) {
            uint64_t f = g_frames.load(), s = g_saved.load(), d = g_dropSaved.load();
            double inFps   = (f - lastF) * 1000.0 / ms;
            double saveFps = (s - lastS) * 1000.0 / ms;
            uint64_t dropped = d - lastD;
            std::printf("[1s] in=%.2f fps, saved=%.2f fps, droppedSave=%llu (total f=%llu, saved=%llu)\n",
                        inFps, saveFps, (unsigned long long)dropped,
                        (unsigned long long)f, (unsigned long long)s);
            last = now; lastF = f; lastS = s; lastD = d;
        }
    }

    // graceful shutdown
    g_accept_requeue = false;
    cam->stop();
    cam->release();
    cm.stop();

    writer_run = false;
    qcv.notify_all();
    wt.join();

#ifndef NO_GST
    preview_stop();
#endif

    return 0;
}
