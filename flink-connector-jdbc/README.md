# flink-jdbc-connector

The fork of [flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc) (fork had been made even before the
connector was separated from the main flink repo).

The fork was created for several reasons:

- Backport of [filter pushdown](https://github.com/apache/flink/pull/20140) feature to Flink 1.15.1.
- Custom Elastic Dialect. There is a [Flink Jira ticket](https://issues.apache.org/jira/browse/FLINK-30702) to add it to
  the official connector repo.
- Minor adjustments were needed to implement Flink Catalog for Elastic (it is also planned to be contributed to the
  official repo).

Eventually, we want to get rid of this fork and use the official jars.
