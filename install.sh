sudo mkdir /usr/local/bin/sonomkr/storage

sudo cp storage-manager.py /usr/local/bin/sonomkr/storage/

python3 -m pip install -r requirements.txt
sudo python3 -m pip install -r requirements.txt

sudo cp storage.conf /etc/sonomkr/

sudo cp SonoMKR_Storage.service /etc/systemd/system/

sudo mkdir /var/sonomkr
sudo mkdir /var/sonomkr/data
sudo mkdir /var/sonomkr/data/channel_1
sudo mkdir /var/sonomkr/data/channel_2

sudo chown -R pi:pi /var/sonomkr
