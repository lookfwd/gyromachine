import smbus
import time
from timeit import default_timer as timer
from confluent_kafka import Producer
import csv

from pwmmotor import PWM
from accel import Accelerometer


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

pwm = PWM(bus)
pwm.set()

acc = Accelerometer(bus)

write_to_file = False
stream_to_kafka = False
if write_to_file:
    vv = []

i = 0
t0 = timer()
running = True
while running:
    v = acc.read()

    msg = ",".join(str(i) for i in v)
    print(msg)

    if stream_to_kafka:
        p.produce('engine', value=msg, callback=acked)
        p.poll(0)

    if write_to_file:
        vv.append(v)
    i += 1
    if i % 10000 == 0:

        x = timer()
        print(10000 / (x - t0))

        if write_to_file:
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


