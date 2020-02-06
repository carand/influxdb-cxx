///
/// \author Adam Wegrzynek <adam.wegrzynek@cern.ch>
///

#include "InfluxDB.h"
#include "InfluxDBException.h"

#include <iostream>
#include <memory>
#include <string>

#ifdef INFLUXDB_WITH_BOOST
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <thread>
#include <functional>
#endif

using namespace std::chrono;
using namespace std::chrono_literals;

namespace influxdb
{

InfluxDB::InfluxDB(std::unique_ptr<Transport> transport) :
  mLineProtocolBatch{},
  mIsBatchingActivated{false},
  mBatchSize{0},
  mTransport(std::move(transport)),
  mGlobalTags{},
  mFlushingThread{nullptr},
  mOnBadRequest{[]{}},
  mOnConnectionError{[]{}},
  mOnConnectionSucceeded{[]{}},
  mLastConnectionStatus{Unknown},
  mLastFlushTime{ system_clock::now() }
{
}

InfluxDB::~InfluxDB()
{
  if (!mIsBatchingActivated) {
    return;
  }

  joinFlushingThread();
  flushBatch();
}

void InfluxDB::doPeriodicFlushBuffer(InfluxDB* influxDb)
{
  auto msToWaitToFlush = influxDb->mFlushingTimeout;
  while (influxDb->mFlushingThreadStarted)
  {
    std::this_thread::sleep_for(msToWaitToFlush);
    msToWaitToFlush = influxDb->mFlushingTimeout - duration_cast<milliseconds>(system_clock::now() - influxDb->mLastFlushTime);

    if (msToWaitToFlush > 0ms)
    {
        continue;
    }

    std::scoped_lock lock(influxDb->mBatchMutex);
    influxDb->flushBatch();
    msToWaitToFlush = influxDb->mFlushingTimeout;
  }
}

void InfluxDB::batchOf(const std::size_t batchSize, const std::chrono::milliseconds& flushingTimeout)
{
  mBatchSize = batchSize;
  mIsBatchingActivated = true;
  mFlushingTimeout = flushingTimeout;
  if (flushingTimeout > 0ms)
  {
      startBufferFlushingThread();
  }
  else
  {
      joinFlushingThread();
  }
}

void InfluxDB::joinFlushingThread()
{
    if (!mFlushingThread)
        return;

    mFlushingThreadStarted = false;
    mFlushingThread->join();
    mFlushingThread.reset();
}

void InfluxDB::startBufferFlushingThread()
{
    if (mFlushingThread)
        return;

    mFlushingThreadStarted = true;
    mFlushingThread = std::make_unique<std::thread>(&InfluxDB::doPeriodicFlushBuffer, this);
}

void InfluxDB::flushBuffer() {
  std::scoped_lock lock(mBatchMutex);
  flushBatch();
}

void InfluxDB::flushBatch()
{
  mLastFlushTime = system_clock::now();
  if (!mIsBatchingActivated || mLineProtocolBatch.empty())
  {
    return;
  }

  auto transmissionResult = transmit(joinLineProtocolBatch());
  sendNotifications(transmissionResult);

  if ((transmissionResult == TransmissionSucceeded) ||
      (transmissionResult == BadRequest))
  {
    mLineProtocolBatch.clear();
  }
}

void InfluxDB::sendNotifications(TransmissionResult transmissionResult) {
  if (transmissionResult == BadRequest)
  {
    mOnBadRequest();
  }

  if (transmissionResult == TransmissionSucceeded ||
      transmissionResult == ServerError ||
      transmissionResult == BadRequest)
  {
    notifyConnectionSuccess();
  }
  else
  {
    notifyConnectionError();
  }

}

void InfluxDB::notifyConnectionError() {
  if (!(mLastConnectionStatus != ConnectionError))
    return;
  mLastConnectionStatus = ConnectionError;
  mOnConnectionError();
}

void InfluxDB::notifyConnectionSuccess() {
  if (!(mLastConnectionStatus != ConnectionSuccess))
    return;
  mLastConnectionStatus = ConnectionSuccess;
  mOnConnectionSucceeded();
}

std::string InfluxDB::joinLineProtocolBatch() const
{
  std::string joinedBatch;
  for (const auto &line : mLineProtocolBatch)
  {
    joinedBatch += line + "\n";
  }
  return joinedBatch;
}

void InfluxDB::addGlobalTag(std::string_view key, std::string_view value)
{
  if (!mGlobalTags.empty())
    mGlobalTags += ",";
  mGlobalTags += key;
  mGlobalTags += "=";
  mGlobalTags += value;
}

InfluxDB::TransmissionResult InfluxDB::transmit(std::string&& lineprotocol)
{
  TransmissionResult result = TransmissionSucceeded;
  try
  {
    mTransport->send(std::move(lineprotocol));
  }
  catch (const server_error& error)
  {
    result = ServerError;
  }
  catch (const bad_request_error& error)
  {
    result = BadRequest;
  }
  catch (const connection_error& error)
  {
    result = ConnectionFailed;
  }
  return result;
}

InfluxDB::TransmissionResult InfluxDB::write(Point&& point)
{
  TransmissionResult result;
  if (mIsBatchingActivated)
  {
    addPointToBatch(point);
    result = PointsBatched;
  }
  else
  {
    result = transmit(point.toLineProtocol());
  }
  return result;
}

InfluxDB::TransmissionResult InfluxDB::write(std::vector<Point> &&points)
{
  TransmissionResult result;
  if (mIsBatchingActivated)
  {
    for(const auto& point : points) {
      addPointToBatch(point);
    }
    result = PointsBatched;
  }
  else
  {
    std::string lineprotocol;
    for(const auto& point : points) {
      lineprotocol += point.toLineProtocol() + "\n";
    }
    result = transmit(std::move(lineprotocol));
  }
  return result;
}

void InfluxDB::addPointToBatch(const Point &point)
{
  std::scoped_lock lock(mBatchMutex);
  mLineProtocolBatch.emplace_back(point.toLineProtocol());
  if (mLineProtocolBatch.size() >= mBatchSize)
  {
    flushBatch();
  }
}

#ifdef INFLUXDB_WITH_BOOST
std::vector<Point> InfluxDB::query(const std::string&  query)
{
  auto response = mTransport->query(query);
  std::stringstream ss;
  ss << response;
  std::vector<Point> points;
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(ss, pt);

  for (auto& result : pt.get_child("results")) {
    auto isResultEmpty = result.second.find("series");
    if (isResultEmpty == result.second.not_found()) return {};
    for (auto& series : result.second.get_child("series")) {
      auto columns = series.second.get_child("columns");

      for (auto& values : series.second.get_child("values")) {
        Point point{series.second.get<std::string>("name")};
        auto iColumns = columns.begin();
        auto iValues = values.second.begin();
        for (; iColumns != columns.end() && iValues != values.second.end(); iColumns++, iValues++) {
          auto value = iValues->second.get_value<std::string>();
          auto column = iColumns->second.get_value<std::string>();
          if (!column.compare("time")) {
            std::tm tm = {};
            std::stringstream ss;
            ss << value;
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
            point.setTimestamp(std::chrono::system_clock::from_time_t(std::mktime(&tm)));
            continue;
          }
          // cast all values to double, if strings add to tags
          try { point.addField(column, boost::lexical_cast<double>(value)); }
          catch(...) { point.addTag(column, value); }
        }
        points.push_back(std::move(point));
      }
    }
  }
  return points;
}

void InfluxDB::onConnectionError(std::function<void ()> callback)
{
  mOnConnectionError=std::move(callback);
  if (mLastConnectionStatus == ConnectionError)
  {
    mOnConnectionError();
  }
}

void InfluxDB::onBadRequest(std::function<void ()> callback)
{
  mOnBadRequest=std::move(callback);
}

void InfluxDB::onTransmissionSucceeded(std::function<void()> callback)
{
  mOnConnectionSucceeded=std::move(callback);
  if (mLastConnectionStatus == ConnectionSuccess)
  {
    mOnConnectionSucceeded();
  }
}



#else
std::vector<Point> InfluxDB::query(const std::string& /*query*/)
{
  throw InfluxDBException("InfluxDB::query", "Boost is required");
}
#endif

} // namespace influxdb
