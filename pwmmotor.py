import smbus
import time

class PWM(object):

    def __init__(self, bus):
        self.bus = bus
        address = 0x40

        LED0_ON_L = 6
        PCA9685_MODE1 = 0
        PCA9685_PRESCALE = 0xfe

        self.bus.write_byte_data(address, PCA9685_MODE1, 0x80)

        oldmode = self.bus.read_byte_data(address, PCA9685_MODE1)
        newmode = (oldmode & 0x7f) | 0x10
        self.bus.write_byte_data(address, PCA9685_MODE1, newmode)
        self.bus.write_byte_data(address, PCA9685_PRESCALE, 6)
        self.bus.write_byte_data(address, PCA9685_MODE1, oldmode)
        time.sleep(0.005)
        self.bus.write_byte_data(address, PCA9685_MODE1, oldmode | 0xa0)

        #oldmode = self.bus.read_byte_data(address, PCA9685_MODE1)

    def set(self, value):
        on = 0
        off = value

        data = [on, on >> 8, off, off >> 8]

        channel = 4
        self.bus.write_i2c_block_data(address, LED0_ON_L + (channel * 4), data)
