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
  mBuffer{},
  mBuffering{false},
  mBufferSize{0},
  mTransport(std::move(transport)),
  mGlobalTags{},
  mFlushingThread{nullptr},
  mOnBadRequest{[]{}},
  mOnConnectionError{[]{}},
  mOnTransmissionSucceeded{[]{}},
  mLastConnectionNotification{NothingNotified},
  mLastFlushTime{ system_clock::now() }
{
}

InfluxDB::~InfluxDB()
{
    if (!mBuffering)
        return;

    joinFlushingThread();
    flushBuffer();
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

    std::scoped_lock lock(influxDb->mBufferMutex);
    influxDb->flushBuffer();
    msToWaitToFlush = influxDb->mFlushingTimeout;
  }
}

void InfluxDB::batchOf(const std::size_t size, const std::chrono::milliseconds& timeout)
{
  mBufferSize = size;
  mBuffering = true;
  mFlushingTimeout = timeout;
  if (timeout > 0ms)
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

void InfluxDB::flushBuffer()
{
  mLastFlushTime = system_clock::now();
  if (!mBuffering)
  {
    return;
  }
  if (mBuffer.empty())
  {
      return;
  }

  std::string stringBuffer{};
  for (const auto &i : mBuffer)
  {
    stringBuffer+= i + "\n";
  }
  if (transmit(std::move(stringBuffer)))
  {
      mBuffer.clear();
  }
}

void InfluxDB::addGlobalTag(std::string_view key, std::string_view value)
{
  if (!mGlobalTags.empty()) mGlobalTags += ",";
  mGlobalTags += key;
  mGlobalTags += "=";
  mGlobalTags += value;
}

bool InfluxDB::transmit(std::string&& point)
{
  bool result = true;
  try
  {
    mTransport->send(std::move(point));
    if (mLastConnectionNotification != ConnectionSuccess)
    {
      mOnTransmissionSucceeded();
      mLastConnectionNotification = ConnectionSuccess;
    }
  }
  catch (const bad_request_error& error)
  {
    mOnBadRequest();
    if (mLastConnectionNotification != ConnectionSuccess)
    {
      mOnTransmissionSucceeded();
      mLastConnectionNotification = ConnectionSuccess;
    }
  }
  catch (const connection_error& error)
  {
    if (mLastConnectionNotification != ConnectionError)
    {
      mLastConnectionNotification = ConnectionError;
      mOnConnectionError();
    }
    result = false;
  }

  return result;
}

void InfluxDB::write(Point&& point)
{
  if (mBuffering)
  {
    std::scoped_lock lock(mBufferMutex);
    mBuffer.emplace_back(point.toLineProtocol());
    if (mBuffer.size() >= mBufferSize)
    {
      flushBuffer();
    }
  }
  else
  {
    transmit(point.toLineProtocol());
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
    if (mLastConnectionNotification == ConnectionError)
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
    mOnTransmissionSucceeded=std::move(callback);
    if (mLastConnectionNotification == ConnectionSuccess)
    {
        mOnTransmissionSucceeded();
    }
}


#else
std::vector<Point> InfluxDB::query(const std::string& /*query*/)
{
  throw InfluxDBException("InfluxDB::query", "Boost is required");
}
#endif

} // namespace influxdb
