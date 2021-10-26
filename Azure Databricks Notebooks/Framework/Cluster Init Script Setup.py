# MAGIC  
# MAGIC %md
# MAGIC # Cluster Init Script Setup
# MAGIC ###### Author: Eddie Edgeworth 4/17/19
# MAGIC ###### Modified for CLA: Mike Sherrill 6/12/19
# MAGIC 
# MAGIC #### Usage
# MAGIC Create a cluster init script for the pyodbc python library which is required to communicate with the framework database
# MAGIC 
# MAGIC This init script must be attached to the databricks cluster in order to be able to import the pyodbc library
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC #### Details

# COMMAND ----------

# MAGIC %sh lsb_release -a

# COMMAND ----------

dbutils.fs.put("/databricks/pyodbc-install.sh","""
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc
""", True)

# COMMAND ----------

dbutils.fs.ls("/databricks/pyodbc-install.sh")

# COMMAND ----------

dbutils.fs.put("/databricks/postal-install.sh","""
sudo apt install -y autoconf automake libtool pkg-config
git clone https://github.com/openvenues/libpostal
cd libpostal
./bootstrap.sh
./configure --datadir=/srv/postal/datadir
make
sudo make install
sudo ldconfig""", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc