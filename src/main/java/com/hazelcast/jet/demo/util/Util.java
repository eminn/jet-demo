package com.hazelcast.jet.demo.util;

import com.hazelcast.com.eclipsesource.json.JsonValue;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_LIST;

/**
 * date: 1/8/18
 * author: emindemirci
 */
public class Util {

    public static double IST_LON_EAST = 29.5;
    public static double IST_LON_WEST = 28.2;
    public static double IST_LAT_NORTH = 41.2;
    public static double IST_LAT_SOUTH = 40.8;


    public static double LON_LON_EAST = 0.2927;
    public static double LON_LON_WEST = -0.5372;
    public static double LON_LAT_NORTH = 51.7283;
    public static double LON_LAT_SOUTH = 51.3305;



    public static boolean inIstanbul(float lon, float lat) {
        return !(lon > IST_LON_EAST || lon < IST_LON_WEST) &&
                !(lat > IST_LAT_NORTH || lat < IST_LAT_SOUTH);
    }
    public static boolean inLondon(float lon, float lat) {
        return !(lon > LON_LON_EAST || lon < LON_LON_WEST) &&
                !(lat > LON_LAT_NORTH || lat < LON_LAT_SOUTH);
    }

    public static double asDouble(JsonValue value) {
        return value == null ? -1.0 : value.asDouble();
    }

    public static float asFloat(JsonValue value) {
        return value == null ? -1.0f : value.asFloat();
    }

    public static int asInt(JsonValue value) {
        return value == null || !value.isNumber() ? -1 : value.asInt();
    }

    public static long asLong(JsonValue value) {
        return value == null ? -1 : value.asLong();
    }

    public static String asString(JsonValue value) {
        return value == null ? "" : value.asString();
    }

    public static boolean asBoolean(JsonValue value) {
        return value != null && value.asBoolean();
    }

    public static String[] asStringArray(JsonValue value) {
        if (value == null) {
            return new String[]{""};
        } else {
            List<JsonValue> valueList = value.asArray().values();
            List<String> strings = valueList.stream().map(JsonValue::asString).collect(Collectors.toList());
            return strings.toArray(new String[strings.size()]);
        }
    }

    public static List<Double> asDoubleList(JsonValue value) {
        if (value == null) {
            return EMPTY_LIST;
        } else {
            List<JsonValue> valueList = value.asArray().values();
            return valueList.stream().filter(JsonValue::isNumber).map(JsonValue::asDouble).collect(Collectors.toList());
        }
    }

}
