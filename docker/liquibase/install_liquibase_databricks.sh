#! /bin/bash

apt update
apt install -y unzip wget

# liquibase-databricks connector
wget -P /liquibase/lib https://github.com/liquibase/liquibase-databricks/releases/download/v1.4.1/liquibase-databricks-1.4.1.jar

# databricks-jdbc
wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.7.1/DatabricksJDBC42-2.7.1.1004.zip
unzip DatabricksJDBC42-2.7.1.1004.zip -d /liquibase/lib
rm DatabricksJDBC42-2.7.1.1004.zip
