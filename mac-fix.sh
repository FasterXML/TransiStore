# By default MacOS only hands ports over 49152 as ephemeral;
# this is too little for rates above 1k/sec so:

sudo sysctl -w net.inet.ip.portrange.first=24000
sudo sysctl -w net.inet.ip.portrange.hifirst=24000
