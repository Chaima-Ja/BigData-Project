from kafka import KafkaConsumer
import notify2

def sendmessage(title, message):
    notify2.init("E-HEALTH")
    notice = notify2.Notification(title, message, icon = '/home/hduser/Desktop/Projet/PyPrograms/emergency.png')
    notice.set_urgency(notify2.URGENCY_CRITICAL)
    notice.show()
    return

topic_name = 'urgent_data'
consumer = KafkaConsumer(topic_name, group_id='new-consumer-group-topic1', auto_offset_reset= "earliest",bootstrap_servers= 'localhost:9092')
for msg in consumer:
	print("Nurse: urgent data")
	print(msg.value)
	sendmessage('EMERGENCY',str(msg.value))

