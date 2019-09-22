import smbus


address = 0x68


class Accelerometer(object):
    def __init__(self, bus):
        self.bus = bus

        self.bus.write_byte_data(address, 0x6b, 0)  # Wakes up MPU-6050

        ACCEL_CONFIG = bus.read_byte_data(address, 0x1c)
        ACCEL_CONFIG = ACCEL_CONFIG & 0xe7  # Clear AFS_SEL
        self.AFS_SEL = 3
        #AFS_SEL Full Scale Range
        # 0: Full-range +/- 2g
        # 1: Full-range +/- 4g
        # 2: Full-range +/- 8g
        # 3: Full-range +/- 16g
        AFS_SCALER = [2 / 32768, 4 / 32768, 8 / 32768, 16 / 32768]
        self.scale = AFS_SCALER[self.AFS_SEL]
        ACCEL_CONFIG = ACCEL_CONFIG | (self.AFS_SEL << 3)
        # Wakes up MPU-6050
        self.bus.write_byte_data(address, 0x1c, ACCEL_CONFIG)


    def read(self):
        v = self.bus.read_i2c_block_data(address, 0x3b, 6)
        (xout_h, xout_l, yout_h, yout_l, zout_h, zout_l) = v

        x = self.twos_comp(xout_h * 256 + xout_l) * self.scale
        y = self.twos_comp(yout_h * 256 + yout_l) * self.scale
        z = self.twos_comp(zout_h * 256 + zout_l) * self.scale

        return x, y, z

    @staticmethod
    def twos_comp(val):
        bits = 16
        """compute the 2's complement of int value val"""
        if (val & (1 << (bits - 1))) != 0: # if sign bit is set e.g., 8bit: 128-255
            val = val - (1 << bits)        # compute negative value
        return val
