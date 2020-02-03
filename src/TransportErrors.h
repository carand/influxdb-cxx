///
/// \author Adam Wegrzynek
///

#ifndef INFLUXDATA_TRANSPORTERRORS_H
#define INFLUXDATA_TRANSPORTERRORS_H

#include <string>
#include <stdexcept>

namespace transports
{
class bad_request_error: public std::runtime_error
{
public:
    bad_request_error(const std::string &message)
        : runtime_error(message)
    {};
};

class connection_error: public std::runtime_error
{
public:
    connection_error(const std::string &message)
        : runtime_error(message)
    {};
};
}
#endif