Embulk::JavaPlugin.register_filter(
  "murmur2_partitioner", "org.embulk.filter.murmur2_partitioner.Murmur2PartitionerFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
