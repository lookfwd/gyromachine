import smbus
import time

bus = smbus.SMBus(1)
address = 0x40

LED0_ON_L = 6
PCA9685_MODE1 = 0
PCA9685_PRESCALE = 0xfe

bus.write_byte_data(address, PCA9685_MODE1, 0x80)

oldmode = bus.read_byte_data(address, PCA9685_MODE1)
newmode = (oldmode & 0x7f) | 0x10
bus.write_byte_data(address, PCA9685_MODE1, newmode)
bus.write_byte_data(address, PCA9685_PRESCALE, 6)
bus.write_byte_data(address, PCA9685_MODE1, oldmode)
time.sleep(0.005)
bus.write_byte_data(address, PCA9685_MODE1, oldmode | 0xa0)
oldmode = bus.read_byte_data(address, PCA9685_MODE1)

on = 0
off = 3395

data = [on, on >> 8, off, off >> 8]

channel = 4
bus.write_i2c_block_data(address, LED0_ON_L + (channel * 4), data)

#bear1 = bus.read_byte_data(address, 2)
# bus.write_byte_data(address, 0, value)

#time.sleep(3)

