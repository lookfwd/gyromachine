# gyromachine

```
ssh pi@192.168.1.124

sudo apt-get install python3-smbus
sudo apt-get install i2c-tools
sudo i2cdetect -y 1

(env) pi@donkeypi:~ $ sudo i2cdetect -y 1
     0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f
00:          -- -- -- -- -- -- -- -- -- -- -- -- -- 
10: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
20: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
30: -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
40: 40 -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
50: -- -- -- -- -- -- -- -- 58 -- -- -- -- -- -- -- 
60: -- -- -- -- -- -- -- -- 68 -- -- -- -- -- -- -- 
70: 70 -- -- -- -- -- -- --                   



from adafruit_servokit import ServoKit
kit = ServoKit(channels=16)



40 - PCA9685 16-servo
58 - ATECC508A CryptoAuthentication Device
68 - MPU6050 Accelerometer + Gyroscope
70 - I2C mulitplexer/overlay



prescaleval = (25000000 / (4096 * 1600)) - 1 = 2.814697265625
prescale = floor(prescaleval + 0.5) = 3


  uint8_t oldmode = read8(PCA9685_MODE1);
  uint8_t newmode = (oldmode & 0x7F) | 0x10; // sleep
  write8(PCA9685_MODE1, newmode);            // go to sleep
  write8(PCA9685_PRESCALE, prescale);        // set the prescaler
  write8(PCA9685_MODE1, oldmode);
  delay(5);
  write8(PCA9685_MODE1,
         oldmode |
             0xa0); //  This sets the MODE1 register to turn on auto increment.

LED0_ON_L = 0x6 /**< LED0 output and brightness control byte 0 */

on = 0
off = 1024 # 1/4
  _i2c->write(LED0_ON_L + 4 * num);
  _i2c->write(on);
  _i2c->write(on >> 8);
  _i2c->write(off);
  _i2c->write(off >> 8);
  _i2c->endTransmission();
  

#define PCA9685_MODE1 0x0 /**< Mode Register 1 */
#define PCA9685_MODE2 0x1 /**< Mode Register 2 */
#define PCA9685_PRESCALE 0xFE /**< Prescaler for PWM output frequency */

 
void loop() {
  // Drive each PWM in a 'wave'
  for (uint16_t i=0; i<4096; i += 8)
  {
    for (uint8_t pwmnum=0; pwmnum < 16; pwmnum++)
    {
      pwm.setPWM(pwmnum, 0, (i + (4096/16)*pwmnum) % 4096 
      
  uint8_t oldmode = read8(PCA9685_MODE1);
  uint8_t newmode = (oldmode & 0x7F) | 0x10; // sleep
  write8(PCA9685_MODE1, newmode);            // go to sleep
  write8(PCA9685_PRESCALE, prescale);        // set the prescaler
  write8(PCA9685_MODE1, oldmode);


Original sampling rate was ~400 samples/sec
      
Increase I2C bus speed:

https://www.raspberrypi-spy.co.uk/2018/02/change-raspberry-pi-i2c-bus-speed/


Sampling rate with new speed:

1471.577871412922
1478.9629499961572
1476.0896866778182
1475.14384828361


-> The ADC sample

Three-Axis MEMS Gyroscope with 16-bit ADCs and Signal Conditioning

rate is programmable from 8,000 samples per second, down to 3.9 samples per second, and user-selectable
low-pass filters enable a wide range of cut-off frequencies.


Gyroscope Sample Rate, Fast DLPFCFG=0 SAMPLERATEDIV = 0 -> 8 kHz
Gyroscope Sample Rate, Slow DLPFCFG=1,2,3,4,5, or 6 SAMPLERATEDIV = 0 -> 1 kHz
Accelerometer Sample Rate -> 1 kHz
```
