import json

from multiprocessing import Process
from google.cloud import pubsub

_PROJECT = 'v2-pipeline-prototype'
_TOPIC = 'sample'
_PROCESS_COUNT = 5
_BATCH_SIZE = 20

def publish(message_str, ps_batch):
    count = 0
    while True:
        count += 1
        ps_batch.publish(message_str)
        if count >= _BATCH_SIZE :
            ps_batch.commit()
            count = 0

def main():
    json_message = {'id': 1,'first_name': 'Karim','last_name': 'Bourdice','email': 'kbourdice0@purevolume.com','gender': 'Male','ip_address': '76.108.207.137','field1': '1MCwggd9g267ZBfq8p6KUR8RzXh1UARcBM','field2': '14MbkGizhAuYY9fjzA7UQ1x5sHfPEearMy','field3': '1BC7imHVSwT2xKmZBE6xVwGExXte4M87L7','field4': '1H2CPcuv9JvmnKV3Q9zxtZqZkvn86yyqFz','field5': '1GkanWfbWXxmriDt4vdDq5pb2wySL1nDiZ','field6': '1VKh8m3VcEt5xkfqj9BcCJfuL5nVc3nKZ','field7': '1NuruyCxMeL6YM8ouzD9aqAqFDfHzb6pak','field8': '1DB3Ac7uR5NJziSfJVLM1G2F6tW27SkZqN','field9': '1LdM4nGXPQkSKPTLkZFTbKHN9sAUpBGtSf','field10': '1LLCjdpVYy97u7kBdhHJcgycAZbdZ3R1xF','field11': '16FaG4qFomWMmv4Zv4F8MMUBkcJcEHHijY','field12': '1PXkwtCc4CWSp8BkecAvQubMs12GCvqdVu','field13': '1BqnENPGGDXn9S1FHn2pvwLtDDYu1QTjHL','field14': '1KckpoksBagHxANikRrRBC428srpxAqY5C','field15': '17Z3yPdFgkxEn29JaT89HshvVF4RbtMsMq','field16': '14QpaoakehbRnckiQd7Zte2nSRRGX695Wk','field17': '1wGqgYDSmfE9Q1ZH67GwLqNQmH2u7viRs','field18': '134KjLvr8ugwZK19wzL3V9i8fvmaBBjHQM','field19': '1JrUeWAvufRHXbMLSQeGLkjHJNx3XFM77A','field20': '19Botyc2ikY7V9XqRkJbnoJHWk2eVRxni5','field29': '1DjHXt1yecWxjuNPDMQDaAiCjBvDoEwZr1','field21': '1AZejYQWuuQi9DsGp27tHt7PdCRyBtLD8L','field22': '1AzK3J6vwwh8Xog5TJA48Uh223m5LwqRxz','field23': '12rSBAhFvgFMWbssYmSF3zCpDxmpSPxJV6','field24': '1C1QxVG8QphiRD2uzF6Mg86vRdwTARXpeW','field25': '1GRCqpUZ3oejbic59z16XWUYbw8GYdiq6f','field26': '1CH2yzHjehfuyVW9vw9NPTWX3yW5v2vTFg','field27': '1CCoa5aQPJ4Ya1FDKWrMceRReyptKcWV1N','field28': '1LJSmvzGWwDBLc1YTPaT2k1uAkiX8GjGpz'}
    message_str = json.dumps(json_message)

    ps = pubsub.Client()
    ps_topic = ps.topic(_TOPIC)
    ps_batch = ps_topic.batch()
    print("Pushing messages to {0}".format(_TOPIC))

    processes = []
    for i in range(1, _PROCESS_COUNT):
        p = Process(target=publish, args=(message_str, ps_batch))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
