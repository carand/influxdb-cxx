#define BOOST_TEST_MODULE Test InfluxDB batch flushing
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <thread>
#include <chrono>

#include "../include/InfluxDBFactory.h"

namespace influxdb{ namespace test {

// To run these test you must run a local InfluxDb listening at 8086 port

BOOST_AUTO_TEST_CASE(With_InfluxDb_down_after_timeout_on_failed_callback_is_called)
{
    int failedTransmissions{0};
    int succedeedTransmissions{0};


    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8081?db=test",
                                                [&]{succedeedTransmissions++;},
                                                [&]{failedTransmissions++;});
    influxdb->batchOf(100, std::chrono::milliseconds(1000));

    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, failedTransmissions);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    BOOST_CHECK_EQUAL(1, failedTransmissions);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    BOOST_CHECK_EQUAL(2, failedTransmissions);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

}

BOOST_AUTO_TEST_CASE(With_InfluxDb_down_after_timeout_on_succeded_callback_is_called)
{
    int failedTransmissions{0};
    int succedeedTransmissions{0};


    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8086?db=test",
        [&]{succedeedTransmissions++;},
        [&]{failedTransmissions++;});

    influxdb->batchOf(100, std::chrono::milliseconds(1000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, failedTransmissions);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    BOOST_CHECK_EQUAL(0, failedTransmissions);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);

    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    BOOST_CHECK_EQUAL(0, failedTransmissions);
    BOOST_CHECK_EQUAL(2, succedeedTransmissions);
}

}}
