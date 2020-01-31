///
/// \author Adam Wegrzynek
///

#ifndef INFLUXDATA_INFLUXDB_H
#define INFLUXDATA_INFLUXDB_H

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include <thread>
#include <functional>
#include <atomic>

#include "Transport.h"
#include "Point.h"

namespace influxdb
{

class InfluxDB
{
  public:
    /// Disable copy constructor
    InfluxDB & operator=(const InfluxDB&) = delete;

    /// Disable copy constructor
    InfluxDB(const InfluxDB&) = delete;

    /// Constructor required valid transport
    InfluxDB(std::unique_ptr<Transport> transport);

    /// Flushes buffer
    ~InfluxDB();

    /// Writes a metric
    /// \param metric
    void write(Point&& metric);

    /// Queries InfluxDB database
    std::vector<Point> query(const std::string& query);

    /// Flushes metric buffer (this can also happens when buffer is full)
    void flushBuffer();

    /// Enables metric buffering. If timeout is defined, buffer autoflushing is activated. It leads to having injection
    /// both for reaching batch size or timeout.
    /// Batching size and timeout can be changed dynamically but it can not be deactivated (by now)
    /// \param size
    /// \param timeout
    void batchOf(const std::size_t size = 32,
                 const std::chrono::milliseconds& timeout = std::chrono::milliseconds(0));

    /// Adds a global tag
    /// \param name
    /// \param value
    void addGlobalTag(std::string_view name, std::string_view value);

    /// Set the callback called when a transmission fails
    /// \param name
    void onTransmissionFailed(std::function<void()> callback);

    /// Set the callback called when a transmission succeedes
    /// \param callback
    void onTransmissionSucceeded(std::function<void()> callback);


  private:
    void addLineProtocolToBuffer(std::string&& lineProtocol);
    static void doPeriodicFlushBuffer(InfluxDB* influxDb);
    void startBufferFlushingThread();
    void joinFlushingThread();

  private:
    /// Buffer for points
    std::deque<std::string> mBuffer;

    /// Flag stating whether point buffering is enabled
    bool mBuffering;

    /// Buffer size
    std::size_t mBufferSize;

    /// Underlying transport UDP/HTTP/Unix socket
    std::unique_ptr<Transport> mTransport;

    /// Transmits string over transport
    bool transmit(std::string&& point);

    /// List of global tags
    std::string mGlobalTags;

    /// Mutex for accessing buffer
    std::mutex mBufferMutex;

    /// Flushing timeout
    std::chrono::milliseconds mFlushingTimeout;

    /// Flushing thread
    std::unique_ptr<std::thread> mFlushingThread;

    /// Flushing thread stop flag
    std::atomic<bool> mFlushingThreadStarted;

    /// Callback called when transmission fails
    std::function<void()> mOnTransmissionFailed;

    /// Callback called when transmission success
    std::function<void()> mOnTransmissionSucceeded;

};

} // namespace influxdb

#endif // INFLUXDATA_INFLUXDB_H
