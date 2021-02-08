Installation requirements:

1. Make sure to download and use the correct Chrome Driver: https://sites.google.com/a/chromium.org/chromedriver/downloads
 - Install Chromium browser: apt-get install chromium-browser
 - location for driver: prereqs
 
2. Install Postgres and create DB defined in database.sql
 - apt-get install postgresql
 - apt-get install libpq-dev
 - apt-get install python3-psycopg2
 
3. Other installations:
 - apt-get install xvfb xserver-xephyr vnc4server

4. Python libraries
 pip3 install selenium
 pip3 install pyvirtualdisplay
 pip3 install psycopg2
 pip3 install tldextract
 pip3 install pandas
 pip3 install beautifulsoup4
 pip3 install matplotlib
 pip3 install haralyzer
 pip3 install lxml
 pip3 install Pillow
 pip3 install dhash
 pip3 install distance
 pip3 install mitmproxy
 pip3 install dnspython
 pip3 install seaborn
 pip3 install networkx

5. Set up runConfig.txt to your liking

6. Run main.py 
