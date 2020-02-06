///
/// \author Adam Wegrzynek
///

#ifndef INFLUXDATA_TRANSPORTINTERFACE_H
#define INFLUXDATA_TRANSPORTINTERFACE_H

#include <string>
#include <stdexcept>

namespace influxdb
{
class bad_request_error : public std::runtime_error
{
public:
  bad_request_error(const std::string& message): runtime_error(message){};
};

class server_error : public std::runtime_error
{
public:
  server_error(const std::string& message): runtime_error(message){};
};

class connection_error : public std::runtime_error
{
public:
  connection_error(const std::string& message): runtime_error(message){};
};

/// \brief Transport interface
class Transport
{
  public:
    Transport() = default;

    virtual ~Transport() = default;

    /// Sends string blob
    virtual void send(std::string&& message) = 0;

    /// Sends s request
    virtual std::string query(const std::string& /*query*/) {
      throw std::runtime_error("Queries are not supported in the selected transport");
    }
};

} // namespace influxdb

#endif // INFLUXDATA_TRANSPORTINTERFACE_H
