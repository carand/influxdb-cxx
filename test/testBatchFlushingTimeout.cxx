#define BOOST_TEST_MODULE Test InfluxDB batch flushing
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <thread>
#include <chrono>
#include <atomic>

#include "../include/InfluxDBFactory.h"

namespace influxdb{ namespace test {

// To run these test you must run a local InfluxDb listening at 8086 port

BOOST_AUTO_TEST_CASE(With_InfluxDb_down_after_timeout_connection_error_callback_is_called)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};
    std::atomic<int>badRequests{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8081?db=test");
    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});
    influxdb->onBadRequest([&]{badRequests++;});

    influxdb->batchOf(100, std::chrono::milliseconds(1000));

    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(1, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(1, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);
}

BOOST_AUTO_TEST_CASE(After_timeout_send_is_performed_to_influx)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};
    std::atomic<int>badRequests{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8086?db=test");
    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});
    influxdb->onBadRequest([&]{badRequests++;});

    influxdb->batchOf(100, std::chrono::milliseconds(1000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);

    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);
}


BOOST_AUTO_TEST_CASE(Badrequests_callback_is_called_everytime_a_request_is_ill_formed)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};
    std::atomic<int>badRequests{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8086?db=test");
    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});
    influxdb->onBadRequest([&]{badRequests++;});
    influxdb->batchOf(100, std::chrono::milliseconds(1000));

    influxdb->write(Point{ "test" }.addField("value", 2.0e-310).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(1, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);

    influxdb->write(Point{ "test" }.addField("value", 3.10e-320).addTag("host", "localhost"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(2, badRequests);
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);
}

BOOST_AUTO_TEST_CASE(Dynamic_deactivate_flushing_timeout)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};
    std::atomic<int>badRequests{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8086?db=test");
    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});
    influxdb->onBadRequest([&]{badRequests++;});

    auto points = influxdb->query("SELECT * from test");
    int nbOfPointsAtBegginning = points.size();

    influxdb->batchOf(100, std::chrono::milliseconds(1000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);
    points = influxdb->query("SELECT * from test");
    int nbOfPointsAfterTimeout = points.size();
    BOOST_CHECK_EQUAL(nbOfPointsAtBegginning + 1, nbOfPointsAfterTimeout);

    // deactivate flushing setting timeout to 0
    influxdb->batchOf(100, std::chrono::milliseconds(0));

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);
    points = influxdb->query("SELECT * from test");
    int nbOfPointsDeactivatedAutoFlushing = points.size();
    BOOST_CHECK_EQUAL(nbOfPointsAtBegginning + 1, nbOfPointsDeactivatedAutoFlushing);
}

BOOST_AUTO_TEST_CASE(When_transmission_was_ok_before_callback_registering_it_is_notified_at_callback_registering)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8086?db=test");

    influxdb->batchOf(100, std::chrono::milliseconds(1000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});

    BOOST_CHECK_EQUAL(0, connectionErrors);
    BOOST_CHECK_EQUAL(1, succedeedTransmissions);
}

BOOST_AUTO_TEST_CASE(When_connection_error_happened_before_callback_registering_it_is_notified_at_callback_registering)
{
    std::atomic<int>succedeedTransmissions{0};
    std::atomic<int>connectionErrors{0};

    auto influxdb = influxdb::InfluxDBFactory::Get("http://localhost:8081?db=test");

    influxdb->batchOf(100, std::chrono::milliseconds(1000));
    influxdb->write(Point{ "test" }.addField("value", 10).addTag("host", "localhost"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    influxdb->onTransmissionSucceeded([&]{succedeedTransmissions++;});
    influxdb->onConnectionError([&]{connectionErrors++;});

    BOOST_CHECK_EQUAL(1, connectionErrors);
    BOOST_CHECK_EQUAL(0, succedeedTransmissions);
}
}}
