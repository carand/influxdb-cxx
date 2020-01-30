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
    InfluxDB(std::unique_ptr<Transport> transport,
        std::function<void()> onTransmissionSucceeded,
        std::function<void()> onTransmissionFailed);

    /// Flushes buffer
    ~InfluxDB();

    /// Writes a metric
    /// \param metric
    void write(Point&& metric);

    /// Queries InfluxDB database
    std::vector<Point> query(const std::string& query);

    /// Flushes metric buffer (this can also happens when buffer is full)
    void flushBuffer();

    /// Enables metric buffering
    /// \param size
    void batchOf(const std::size_t size = 32,
                 const std::chrono::milliseconds& timeout = std::chrono::milliseconds(500));

    /// Adds a global tag
    /// \param name
    /// \param value
    void addGlobalTag(std::string_view name, std::string_view value);


private:
    void addLineProtocolToBuffer(std::string&& lineProtocol);
    static void doPeriodicFlushBuffer(InfluxDB* influxDb);

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
    bool mStopFlushingThread;

    std::function<void()> mOnTransmissionFailed;
    std::function<void()> mOnTransmissionSucceeded;
};

} // namespace influxdb

#endif // INFLUXDATA_INFLUXDB_H
