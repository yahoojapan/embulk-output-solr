Embulk::JavaPlugin.register_output(
  "solr", "org.embulk.output.solr.SolrOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
