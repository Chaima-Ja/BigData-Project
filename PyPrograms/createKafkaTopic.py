from kafka.admin import KafkaAdminClient, NewTopic


admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

topic_list = []
topic_list.append(NewTopic(name="test", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="urgent_data", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="normal_data", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="sepsis_data", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="sepsis_before_admission", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="sepsis_after_admission", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="no_sepsis_data", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="laboratory_values_data", num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name="demographics_data", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
