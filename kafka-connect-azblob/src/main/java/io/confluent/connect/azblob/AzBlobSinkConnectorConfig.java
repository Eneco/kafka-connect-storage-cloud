/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.azblob;

import io.confluent.connect.azblob.storage.AzBlobStorage;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.azblob.format.json.JsonFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator;
import io.confluent.connect.storage.hive.schema.TimeBasedSchemaGenerator;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

public class AzBlobSinkConnectorConfig extends StorageSinkConnectorConfig {

  public static final String AZ_STORAGEACCOUNT_CONNECTION_STRING = "azblob.storageaccount.connectionstring";
  public static final String AZ_STORAGE_CONTAINER_NAME = "azblob.containername";

  public static final String PART_SIZE_CONFIG = "s3.part.size";
  public static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

  public static final String WAN_MODE_CONFIG = "s3.wan.mode";
  private static final boolean WAN_MODE_DEFAULT = false;

  public static final String REGION_CONFIG = "s3.region";
  public static final String REGION_DEFAULT = "FIXME-DEFAULT";

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender SCHEMA_GENERATOR_CLASS_RECOMMENDER =
      new GenericRecommender();

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(AzBlobStorage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(AvroFormat.class, JsonFormat.class)
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );

    SCHEMA_GENERATOR_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultSchemaGenerator.class,
            TimeBasedSchemaGenerator.class
        )
    );

  }



    public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(FORMAT_CLASS_RECOMMENDER);
    {
      final String group = "S3";
      int orderInGroup = 0;

      configDef.define(
          REGION_CONFIG,
          Type.STRING,
          REGION_DEFAULT,
          Importance.MEDIUM,
          "The AWS region to be used the connector.",
          group,
          ++orderInGroup,
          Width.LONG,
          "AWS region"
      );

      configDef.define(
          PART_SIZE_CONFIG,
          Type.INT,
          PART_SIZE_DEFAULT,
          new PartRange(),
          Importance.HIGH,
          "The Part Size in S3 Multi-part Uploads.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Part Size"
      );

      configDef.define(
          WAN_MODE_CONFIG,
          Type.BOOLEAN,
          WAN_MODE_DEFAULT,
          Importance.MEDIUM,
          "Use S3 accelerated endpoint.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 accelerated endpoint enabled"
      );

      configDef.define(
          AVRO_CODEC_CONFIG,
          Type.STRING,
          AVRO_CODEC_DEFAULT,
          Importance.LOW,
          "The Avro compression codec to be used for output files. Available values: null, "
              + "deflate, snappy and bzip2 (codec source is org.apache.avro.file.CodecFactory)",
          group,
          ++orderInGroup,
          Width.LONG,
          "Avro compression codec"
      );
    }
    return configDef;
  }

  public AzBlobSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected AzBlobSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(
        PARTITIONER_CLASS_RECOMMENDER,
        SCHEMA_GENERATOR_CLASS_RECOMMENDER
    );
    partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());

    this.name = parseName(originalsStrings());
    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public int getPartSize() {
    return getInt(PART_SIZE_CONFIG);
  }

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "S3-sink";
  }

  public String getName() {
    return name;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    return map;
  }

  public String getStorageConnectionString() {
    return getString(AZ_STORAGEACCOUNT_CONNECTION_STRING);
  }

    public String getContainerName() {
        return getString(AZ_STORAGE_CONTAINER_NAME);
    }

    private static class PartRange implements ConfigDef.Validator {
    // S3 specific limit
    final int min = 5 * 1024 * 1024;
    // Connector specific
    final int max = Integer.MAX_VALUE;

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, "Part size must be non-null");
      }
      Number number = (Number) value;
      if (number.longValue() < min) {
        throw new ConfigException(name, value, "Part size must be at least: " + min + " bytes (5MB)");
      }
      if (number.longValue() > max) {
        throw new ConfigException(name, value, "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)");
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
  }




  public static ConfigDef getConfig() {
    Map<String, ConfigDef.ConfigKey> everything = new HashMap<>(newConfigDef().configKeys());
    everything.putAll(StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER).configKeys());
    everything.putAll(
        PartitionerConfig.newConfigDef(
            PARTITIONER_CLASS_RECOMMENDER,
            SCHEMA_GENERATOR_CLASS_RECOMMENDER
        ).configKeys()
    );

    Set<String> blacklist = new HashSet<>();
    blacklist.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    ConfigDef visible = new ConfigDef();
    for (ConfigDef.ConfigKey key : everything.values()) {
      if(!blacklist.contains(key.name)) {
        visible.define(key);
      }
    }
    return visible;
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}
