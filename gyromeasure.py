import smbus
import time
import math
from timeit import default_timer as timer
from confluent_kafka import Producer
from lastrun import save_last_run, load_last_run
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
        pass
        #print("produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))


bus = smbus.SMBus(1)

pwm = PWM(bus)

acc = Accelerometer(bus)

write_to_file = False
stream_to_kafka = True
if write_to_file:
    vv = []

BASE = 4095
RATIO = 0.90

RATIOS = [0.90, 0.95, 0.98]
SPEEDS = [2., 3., 5., 7., 8., 10.]


pwm.set(BASE)
time.sleep(1)

i = 0
t0 = timer()
running = True
cycle = 0
conf = 0
last_x_sign = 0
run = load_last_run() + 1
while running:
    x, y, z = acc.read()

    t = timer()

    x_sign = 1 if x >= 0 else 0
    if x_sign and not last_x_sign:
        cycle += 1

        if (cycle % 500) == 0:
            run += 1
            save_last_run(run)
            conf = (conf + 1) % (len(RATIOS) * len(SPEEDS))
            print("running on conf: ", conf)

        ratio = RATIOS[conf % len(RATIOS)]
        speed = SPEEDS[conf // len(RATIOS)]

        xv = int((ratio * BASE) +
                 ((1. - ratio) * BASE * math.sin(float(cycle) / speed)))
        pwm.set(xv)
    last_x_sign = x_sign

    msg = "%f:%d:%d:%d:%f,%f,%f" % (t, cycle, conf, run, x, y, z)

    if stream_to_kafka:
        p.produce('engine', value=msg, callback=acked)
        p.poll(0)

    if write_to_file:
        vv.append(msg)

    i += 1
    if i % 10000 == 0:

        print(10000 / (t - t0))

        if write_to_file:
            with open('measurements.csv', 'w') as f:
                for v in vv:
                    f.write(v)
                    f.write("\n")
            vv = []
            running = False

        t0 = t
        i = 0

