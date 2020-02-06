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
    enum TransmissionResult {
      TransmissionSucceeded,
      PointsBatched,
      ServerError,
      BadRequest,
      ConnectionFailed
    };

    /// Disable copy constructor
    InfluxDB & operator=(const InfluxDB&) = delete;

    /// Disable copy constructor
    InfluxDB(const InfluxDB&) = delete;

    /// Constructor required valid transport
    InfluxDB(std::unique_ptr<Transport> transport);

    /// Flushes buffer
    ~InfluxDB();

    /// Writes a point
    /// \param metric
    TransmissionResult write(Point&& point);

    /// Writes a point
    /// \param metric
    TransmissionResult write(std::vector<Point>&& points);

    /// Queries InfluxDB database
    std::vector<Point> query(const std::string& query);

    /// Flushes points batched
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

    /// Set the callback called when a connection error raises from transport
    /// \param name
    void onConnectionError(std::function<void()> callback);

    /// Set the callback called when a bad request is responsed to transport
    /// \param callback
    void onBadRequest(std::function<void()> callback);

    /// Set the callback called when a transmission succeeded by transport
    /// \param callback
    void onTransmissionSucceeded(std::function<void()> callback);


  private:
    enum ConnectionStatus {
        Unknown,
        ConnectionSuccess,
        ConnectionError
    };



    /// Flushes line protocol batch
    void flushBatch();

    /// Transmits lineProtocol over transport
    TransmissionResult transmit(std::string&& lineProtocol);

    void startBufferFlushingThread();

    void joinFlushingThread();

    static void doPeriodicFlushBuffer(InfluxDB* influxDb);

    std::string joinLineProtocolBatch() const;

    void sendNotifications(TransmissionResult transmissionResult);

    void notifyConnectionSuccess();

    void notifyConnectionError();

    void addPointToBatch(const Point &point);
  private:
    /// Mutex for accessing batch
    std::mutex mBatchMutex;

    /// line protocol batch to be writen
    std::deque<std::string> mLineProtocolBatch;

    /// Flag stating whether point buffering is enabled
    bool mIsBatchingActivated;

    /// Buffer size
    std::size_t mBatchSize;

    /// Underlying transport UDP/HTTP/Unix socket
    std::unique_ptr<Transport> mTransport;

    /// List of global tags
    std::string mGlobalTags;

    /// Flushing timeout
    std::chrono::milliseconds mFlushingTimeout;

    /// Flushing thread
    std::unique_ptr<std::thread> mFlushingThread;

    /// Flushing thread stop flag
    std::atomic<bool> mFlushingThreadStarted;

    /// Callback called when bad request is reported from transport
    std::function<void()> mOnBadRequest;

    /// Callback called when connection error happens
    std::function<void()> mOnConnectionError;

    /// Callback called when transmission success
    std::function<void()> mOnConnectionSucceeded;

    /// last connection status
    ConnectionStatus mLastConnectionStatus;

    /// time in which last buffer flush was performed
    std::chrono::time_point<std::chrono::system_clock> mLastFlushTime;
};

} // namespace influxdb

#endif // INFLUXDATA_INFLUXDB_H
