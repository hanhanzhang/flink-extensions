package com.sdu.flink.utils;

import java.util.Random;

public class RandomUtils {

  private static final Random RANDOM = new Random();

  private RandomUtils() {}

  public static String createUUID(String prefix, int range, int base) {
    int id = RANDOM.nextInt(range) + base;
    return String.format("%s_%d", prefix, id);
  }

  public static int nextInt(int range, int base) {
    return RANDOM.nextInt(range) + base;
  }

  public static int nextInt(int range) {
    return RANDOM.nextInt(range);
  }

}
