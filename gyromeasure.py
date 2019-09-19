import smbus
import time
from timeit import default_timer as timer
import csv
from confluent_kafka import Producer

bootstrap = 'pkc-4kgmg.us-west-2.aws.confluent.cloud:9092'
kfus = 'EQDIKMXZGURYBP47'
kfps = 'ZClMvl2n7PI6+rdkuuZ2Fhwera5pnUwzMfcfSMwDRLlclIm6DxYK4CwU66ZS/xWX'

p = Producer({
        'bootstrap.servers': bootstrap,
        'broker.version.fallback': '0.10.0.0',
        'api.version.fallback.ms': 0,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': kfus,
        'sasl.password': kfps,
        'ssl.ca.location': '/etc/ssl/certs',
#        'queue.buffering.max.ms': '5000',
#        'message.max.bytes': '1000000',
#        'batch.num.messages': '10000',
         })

def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print("failed to deliver message: {}".format(err.str()))
    else:
        print("produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))


bus = smbus.SMBus(1)
address = 0x68

bus.write_byte_data(address, 0x6b, 0)  # Wakes up MPU-6050

ACCEL_CONFIG = bus.read_byte_data(address, 0x1c)
ACCEL_CONFIG = ACCEL_CONFIG & 0xe7  # Clear AFS_SEL
#AFS_SEL Full Scale Range
#0 ± 2g
#1 ± 4g
#2 ± 8g
#3 ± 16g
ACCEL_CONFIG = ACCEL_CONFIG | (3 << 3)
bus.write_byte_data(address, 0x1c, ACCEL_CONFIG)  # Wakes up MPU-6050

vv = []
i = 0
t0 = timer()
running = True
while running:
    v = bus.read_i2c_block_data(address, 0x3b, 14)
    msg = ",".join(str(i) for i in v)
    #print(msg)
    p.produce('engine', value=msg, callback=acked)
    p.poll(0)

    continue

    vv.append(v)
    i += 1
    if i % 10000 == 0:
        x = timer()
        print(10000 / (x - t0))

        with open('measurements.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            for v in vv:
                spamwriter.writerow(v)
        vv = []
        running = False
        t0 = x

#gyroskop_xout = read_word_2c(0x43)
#gyroskop_yout = read_word_2c(0x45)
#gyroskop_zout = read_word_2c(0x47)
 
#beschleunigung_xout = read_word_2c(0x3b)
#beschleunigung_yout = read_word_2c(0x3d)
#beschleunigung_zout = read_word_2c(0x3f)


