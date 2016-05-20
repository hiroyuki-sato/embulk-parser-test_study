Embulk::JavaPlugin.register_parser(
  "test_study", "org.embulk.parser.test_study.TestStudyParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
