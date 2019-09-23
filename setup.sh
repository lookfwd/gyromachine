wget https://bbdemos.s3-us-west-2.amazonaws.com/jdk1.8_1.8.0221-1_amd64.deb
sudo dpkg -i jdk1.8_1.8.0221-1_amd64.deb

wget https://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
tar -xzf spark-2.4.4-bin-hadoop2.7.tgz
sudo mv spark-2.4.4-bin-hadoop2.7 /usr/local
sudo ln -s /usr/local/spark-2.4.4-bin-hadoop2.7/ /usr/local/spark
echo 'export SPARK_HOME=/usr/local/spark' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=jupyter' >> ~/.bashrc
echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip=\"*\"'" >> ~/.bashrc

rm jdk1.8_1.8.0221-1_amd64.deb  spark-2.4.4-bin-hadoop2.7.tgz

source ~/.bashrc

sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 10

sudo apt-get update
sudo apt  install -y jupyter-core jupyter-notebook
python -m pip install pandas matplotlib scipy boto3 matplotlib
   
jupyter notebook --generate-config
sed -i "s/#c.NotebookApp.password = ../c.NotebookApp.password = 'sha1:74c6a7941e19:7bbc4066e4b20014431e3e0195b6b76921718d07'/" /home/ubuntu/.jupyter/jupyter_notebook_config.py

IP=`ifconfig | grep "inet " | head -1 | awk '{print $2}'`
target_url=`curl http://169.254.169.254/latest/user-data`
echo "$IP ${target_url}" | sudo tee -a /etc/hosts

# Move jars to dir
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.1/hadoop-aws-2.7.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.4/spark-sql-kafka-0-10_2.11-2.4.4.jar
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar
sudo mkdir /usr/local/jars
sudo mv *.jar /usr/local/jars/

wget https://bbdemos.s3-us-west-2.amazonaws.com/notebooks.tar.gz
tar -xzf notebooks.tar.gz
rm notebooks.tar.gz

sudo mv jupyter.service /etc/systemd/system/jupyter.service
sudo systemctl enable jupyter.service
sudo systemctl daemon-reload
sudo systemctl restart jupyter.service

