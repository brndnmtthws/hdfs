package org.apache.mesos.collections;

import com.google.common.base.Predicate;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 */
public class Maps {

  public static Map<String, String> filterMapKeysThatStartWith(Map<String, String> map, final String startWithString) {
    return com.google.common.collect.Maps.filterKeys(map, new Predicate<String>() {
      @Override
      public boolean apply(@Nonnull String s) {
        return s.startsWith(startWithString);
      }
    });
  }

  public static Map<String, String> propertyMapThatStartWith(Properties properties, String startWithString) {
    if (properties == null) {
      return new HashMap<>();
    }

    return filterMapKeysThatStartWith(com.google.common.collect.Maps.fromProperties(properties), startWithString);
  }
}
