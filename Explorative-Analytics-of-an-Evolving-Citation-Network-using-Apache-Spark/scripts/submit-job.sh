spark-submit --deploy-mode cluster --class CitationGraph --master yarn --supervise target/scala-2.12/citationgraphscala_2.12-0.1.jar /input /output
