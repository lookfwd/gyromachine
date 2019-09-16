import smbus
import time
from timeit import default_timer as timer
import csv

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


