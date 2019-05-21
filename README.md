# filter-kafka-streamapp

Kafka Stream app to filter messages based on the filtering criteria in another kafka topic. The filtering topic materialized as a KTable and data available via Rest API endpoints.

This app would work for any kafka topic with key,value pair messages. Messages published using string serdes but with JSON format.

For the incoming topic specify the fields required in the outgoing topic by configuring "message.fields" property. This is a comma seperated list.

For the Filtering to work key needs to be selected using the incoming message fields, these keys needs to be specified in "Key.fields" property. This is a comma seperated list as well.

