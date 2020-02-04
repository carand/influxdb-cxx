#define BOOST_TEST_MODULE Test InfluxDB Point
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>

#include "../include/InfluxDBFactory.h"

namespace influxdb {
namespace test {


std::vector<std::string> getVector(const Point& point)
{
  std::istringstream result(point.toLineProtocol());
  return std::vector<std::string>{std::istream_iterator<std::string>{result},
                      std::istream_iterator<std::string>{}};
}

BOOST_AUTO_TEST_CASE(add_integer_fields_to_line_protocol_properly)
{
  auto point = Point{"test"}
    .addField("value", 10LL);

  auto result = getVector(point);

  BOOST_CHECK_EQUAL(result[0], "test");
  BOOST_CHECK_EQUAL(result[1], "value=10i");
}

BOOST_AUTO_TEST_CASE(add_double_fields_to_line_protocol_properly)
{
  auto point = Point{"test"}
    .addField("value", 10LL)
    .addField("dvalue", 10.10);

  auto result = getVector(point);

  BOOST_CHECK_EQUAL(result[0], "test");
  BOOST_CHECK_EQUAL(result[1], "value=10i,dvalue=10.1");
}

BOOST_AUTO_TEST_CASE(add_string_fields_to_line_protocol_properly)
{
    auto point = Point{"test"}
        .addField("string_field", "a_string_value");

    auto result = getVector(point);

    BOOST_CHECK_EQUAL(result[0], "test");
    BOOST_CHECK_EQUAL(result[1], "string_field=\"a_string_value\"");
}

BOOST_AUTO_TEST_CASE(adds_tags_to_line_protocol_properly)
{
  auto point = Point{"test"}
    .addField("value", 10LL)
    .addField("dvalue", 10.10)
    .addTag("tag", "tagval");

  auto result = getVector(point);

  BOOST_CHECK_EQUAL(result[0], "test,tag=tagval");
  BOOST_CHECK_EQUAL(result[1], "value=10i,dvalue=10.1");
}

BOOST_AUTO_TEST_CASE(generates_properly_lineprotocol_timestamp)
{
  auto point = Point{"test"}
    .addField("value", 10)
    .addField("value", 100LL)
    .setTimestamp(std::chrono::time_point<std::chrono::system_clock>(std::chrono::milliseconds(1572830914)));

  auto result = getVector(point);
  BOOST_CHECK_EQUAL(result[2], "1572830914000000");
}

BOOST_AUTO_TEST_CASE(does_not_add_tags_with_empty_key_or_empty_value)
{
    auto point = Point{"test"}
        .addTag("", "tag_val")
        .addTag("tag_name", "")
        .setTimestamp(std::chrono::time_point<std::chrono::system_clock>(std::chrono::milliseconds(0)));

    auto result = getVector(point);

    BOOST_CHECK_EQUAL(result[0], "test");
    BOOST_CHECK_EQUAL(result[1], "0");
}

BOOST_AUTO_TEST_CASE(does_not_add_fields_with_empty_key_or_empty_string_value)
{
    auto point = Point{"test"}
        .addField("", "field_value")
        .addField("field_name", "")
        .setTimestamp(std::chrono::time_point<std::chrono::system_clock>(std::chrono::milliseconds(0)));

    auto result = getVector(point);

    BOOST_CHECK_EQUAL(result[0], "test");
    BOOST_CHECK_EQUAL(result[1], "0");
}

} // namespace test
} // namespace influxdb
