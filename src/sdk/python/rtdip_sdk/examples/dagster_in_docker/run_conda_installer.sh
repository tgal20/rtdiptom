#!/usr/bin/env bash
# Dependencies: x86_64 Architecture, Linux (Tested on Ubuntu >= 20.04 LTS) curl, unzip
start_time=`date +%s`
echo "PATH: $PATH"
CONDA_CHECK_CMD="conda"
CONDA_CMD_DESCRIPTION=$CONDA_CHECK_CMD
CONDA_INSTALLER_NAME="Miniconda3-latest-Linux-x86_64.sh"
CONDA_INSTALLER_URL="https://repo.anaconda.com/miniconda/$CONDA_INSTALLER_NAME"
DEPLOYER_TMP_DIR=$(echo ${TMPDIR:-/tmp}"/DEPLOYER")
MINICONDA_NAME=miniconda
MINICONDA_PATH=$HOME/$MINICONDA_NAME/
PATH=$MINICONDA_PATH/bin:$PATH
CONDA_ENV="lfenergy"
CONDA_ENV_HOME=$(pwd)/apps/$CONDA_ENV
mkdir -p $CONDA_ENV_HOME
CWD=$(pwd)
echo "Current Working Directory: $CWD"
echo "CONDA ENV HOME: $CONDA_ENV_HOME"
echo "DEPLOYER TMP Dir: $DEPLOYER_TMP_DIR"
if ! command -v $CONDA_CHECK_CMD &> /dev/null
then
    echo "Current dir:"
    echo "$CONDA_CMD_DESCRIPTION could not be found. Going to Install it"
    mkdir -p $DEPLOYER_TMP_DIR
    echo "Working Dir to download conda:"
    cd $DEPLOYER_TMP_DIR
    pwd
    curl -O --url $CONDA_INSTALLER_URL
    chmod +x $DEPLOYER_TMP_DIR/*.sh
    bash $CONDA_INSTALLER_NAME -b -p $HOME/miniconda
fi
cd $CONDA_ENV_HOME
echo "Current Dir:"
pwd

echo "Updating Conda"
conda update -n base conda -y
echo "Installing Mamba Solver"
conda install -n base conda-libmamba-solver -y
echo "Setting Solver to libmama"
conda config --set solver libmamba

# RTDIP
export RTDIP_FILE_NAME="InnowattsRelease.zip"
export RTDIP_DOWNLOAD_URL="https://github.com/vbayon/core/archive/refs/heads/$RTDIP_FILE_NAME"
export RTDIP_DIR="core-InnowattsRelease"

echo "Installing RTDIP ***********************************"
rm -rf ./$RTDIP_DIR
rm -rf ./api
rm -rf ./sdk
curl -L -o $RTDIP_FILE_NAME $RTDIP_DOWNLOAD_URL
unzip -o ./$RTDIP_FILE_NAME  > /dev/null
cp -r ./$RTDIP_DIR/src/sdk/python/* .
rm  ./$RTDIP_FILE_NAME


echo "Creating the environment with [CONDA]: CONDA LIBMAMBA SOLVER"
## Copying the env file
rm ./environment.yml
cp ./$RTDIP_DIR/environment.yml ./
find ./environment.yml -type f -exec sed -i 's/rtdip-sdk/lfenergy/g' {} \;
conda  env create -f environment.yml

#
# JDK
echo "JDK jdk-17.0.2 ***********************************" 
export JAVA_VERSION="jdk-17.0.2"
export JDK_FILE_NAME="openjdk-17.0.2_linux-x64_bin.tar.gz"
export JDK_DOWNLOAD_URL="https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/$JDK_FILE_NAME"

if [ -f "$CONDA_ENV/$JDK_FILE_NAME" ]; then
  echo "$CONDA_ENV/$JDK_FILE_NAME Exists"
  echo "Removing JDK: $JDK_FILE_NAME"
  rm -rf $CONDA_ENV/$JAVA_VERSION
  unlink $HOME/JDK
fi

if test -f "$JDK_FILE_NAME";
then
  echo "$JDK_FILE_NAME exists"
else
  echo "$JDK_FILE_NAME does not exists. Downloading it from $JDK_DOWNLOAD_URL"
  curl -o $JDK_FILE_NAME $JDK_DOWNLOAD_URL
fi

tar xvfz $JDK_FILE_NAME > /dev/null
ln -s $CONDA_ENV_HOME/$JAVA_VERSION $HOME/JDK
export JAVA_HOME=$HOME/JDK
export PATH=$HOME/JDK/bin:$PATH

# SPARK 3.4.1
echo "Installing SPARK 3.4.1***********************************"
export SPARK_VERSION="spark-3.4.1-bin-hadoop3"
export SPARK_FILE_NAME="spark-3.4.1-bin-hadoop3.tgz"
export SPARK_DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-3.4.1/$SPARK_FILE_NAME"


if [ -f "$CONDA_ENV/$SPARK_VERSION" ]; then
  echo "$CONDA_ENV/$SPARK_FILE_NAME Exists"
  echo "Removing Spark: $SPARK_FILE_NAME"
  rm -rf $CONDA_ENV/$SPARK_VERSION
  unlink $HOME/SPARK
fi

if test -f "$SPARK_FILE_NAME";
then
  echo "$SPARK_FILE_NAME exists"
else
  echo "$SPARK_FILE_NAME does not exists. Downloading it from $SPARK_DOWNLOAD_URL"
  curl -o $SPARK_FILE_NAME $SPARK_DOWNLOAD_URL
fi


tar xvfz $SPARK_FILE_NAME > /dev/null
ln -s $CONDA_ENV_HOME/$SPARK_VERSION $HOME/SPARK
export SPARK_HOME=$HOME/SPARK


# Extra libraries
echo "Installing Extra Libs ***********************************"
export AWS_JAVA_SDK_BUNDLE_JAR_FILE_NAME="aws-java-sdk-bundle-1.11.1026.jar"
export AWS_JAVA_SDK_BUNDLE_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/$AWS_JAVA_SDK_BUNDLE_JAR_FILE_NAME"

export HADOOP_AWS_JAR_FILE_NAME="hadoop-aws-3.3.2.jar"
export HADOOP_AWS_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/$HADOOP_AWS_JAR_FILE_NAME"

export HADOOP_COMMON_JAR_FILE_NAME="hadoop-common-3.3.2.jar"
export HADOOP_COMMON_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.2/$HADOOP_COMMON_JAR_FILE_NAME"

export HADOOP_HDFS_JAR_FILE_NAME="hadoop-hdfs-3.3.2.jar"
export HADOOP_HDFS_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/3.3.2/$HADOOP_HDFS_JAR_FILE_NAME"

export WOODSTOCK_CORE_JAR_FILE_NAME="woodstox-core-6.5.1.jar"
export WOODSTOCK_CORE_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.5.1/$WOODSTOCK_CORE_JAR_FILE_NAME"

export STAX2_API_JAR_FILE_NAME="stax2-api-4.2.1.jar"
export STAX2_API__JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/$STAX2_API_JAR_FILE_NAME"

export COMMONS_CONFIGURATION_JAR_FILE_NAME="commons-configuration2-2.9.0.jar"
export COMMONS_CONFIGURATION_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.9.0/$COMMONS_CONFIGURATION_JAR_FILE_NAME"

export RE2J_JAR_FILE_NAME="re2j-1.7.jar"
export RE2J_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/com/google/re2j/re2j/1.7/$RE2J_JAR_FILE_NAME"

export AZURE_EVENTHUBS_SPARK_JAR_FILE_NAME="azure-eventhubs-spark_2.12-2.3.22.jar"
export AZURE_EVENTHUBS_SPARK_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/com/microsoft/azure/azure-eventhubs-spark_2.12/2.3.22/$AZURE_EVENTHUBS_SPARK_JAR_FILE_NAME"

export AZURE_EVENTHUBS_JAR_FILE_NAME="azure-eventhubs-3.3.0.jar"
export AZURE_EVENTHUBS_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/com/microsoft/azure/azure-eventhubs/3.3.0/$AZURE_EVENTHUBS_JAR_FILE_NAME"

export SCALA_JAVA8_COMPAT_JAR_FILE_NAME="scala-java8-compat_2.12-1.0.2.jar"
export SCALA_JAVA8_COMPAT_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/scala-lang/modules/scala-java8-compat_2.12/1.0.2/$SCALA_JAVA8_COMPAT_JAR_FILE_NAME"

export PROTON_J_JAR_FILE_NAME="proton-j-0.34.1.jar"
export PROTON_J_JAR_DOWNLOAD_URL="https://repo1.maven.org/maven2/org/apache/qpid/proton-j/0.34.1/$PROTON_J_JAR_FILE_NAME"

curl -o $AWS_JAVA_SDK_BUNDLE_JAR_FILE_NAME $AWS_JAVA_SDK_BUNDLE_JAR_DOWNLOAD_URL
mv $AWS_JAVA_SDK_BUNDLE_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $HADOOP_AWS_JAR_FILE_NAME $HADOOP_AWS_JAR_DOWNLOAD_URL
mv $HADOOP_AWS_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $HADOOP_COMMON_JAR_FILE_NAME $HADOOP_COMMON_JAR_DOWNLOAD_URL
mv $HADOOP_COMMON_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $HADOOP_HDFS_JAR_FILE_NAME $HADOOP_HDFS_JAR_DOWNLOAD_URL
mv $HADOOP_HDFS_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $WOODSTOCK_CORE_JAR_FILE_NAME $WOODSTOCK_CORE_JAR_DOWNLOAD_URL
mv $WOODSTOCK_CORE_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $STAX2_API_JAR_FILE_NAME $STAX2_API__JAR_DOWNLOAD_URL
mv  $STAX2_API_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $COMMONS_CONFIGURATION_JAR_FILE_NAME $COMMONS_CONFIGURATION_JAR_DOWNLOAD_URL
mv  $COMMONS_CONFIGURATION_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $RE2J_JAR_FILE_NAME $RE2J_JAR_DOWNLOAD_URL
mv  $RE2J_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $AZURE_EVENTHUBS_SPARK_JAR_FILE_NAME $AZURE_EVENTHUBS_SPARK_JAR_DOWNLOAD_URL
mv  $AZURE_EVENTHUBS_SPARK_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $AZURE_EVENTHUBS_JAR_FILE_NAME $AZURE_EVENTHUBS_JAR_DOWNLOAD_URL
mv  $AZURE_EVENTHUBS_JAR_FILE_NAME $SPARK_HOME/jars

curl -o $SCALA_JAVA8_COMPAT_JAR_FILE_NAME $SCALA_JAVA8_COMPAT_JAR_DOWNLOAD_URL
mv $SCALA_JAVA8_COMPAT_JAR_FILE_NAME  $SPARK_HOME/jars

curl -o $PROTON_J_JAR_FILE_NAME $PROTON_J_JAR_DOWNLOAD_URL
mv $PROTON_J_JAR_FILE_NAME $SPARK_HOME/jars

echo "Finished INSTALLING $JAVA_VERSION and $SPARK_VERSION and Extra Libraries"

 Password Generator
apt-get install pwgen
##
echo "Installing MySQL"
# Generate random passwords
MYSQL_ROOT_PASSWORD=$(pwgen -s -c -n 10)
MYSQL_DAGSTER_PASSWORD=$(pwgen -s -c -n 10)

# MySQL Config for MySQL Clients connecting to MySQL Server
export MYSQL_HOSTNAME="localhost"
export MYSQL_PORT="3306"
export MYSQL_DAGSTER_DATABASE_NAME="dagster"
export MYSQL_VOLUME="/tmp"
export MYSQL_DAGSTER_USERNAME="dagster"

apt-get install debconf -y
debconf-set-selections <<< "mysql-server mysql-server/root_password password $MYSQL_ROOT_PASSWORD"
debconf-set-selections <<< "mysql-server mysql-server/root_password_again password $MYSQL_ROOT_PASSWORD"


apt-get install -y mysql-server
apt-get install -y mysql-client
service mysql --full-restart
mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "CREATE DATABASE ${MYSQL_DAGSTER_DATABASE_NAME} CHARACTER SET utf8 COLLATE utf8_unicode_ci;;"
mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "CREATE USER ${MYSQL_DAGSTER_USERNAME}@localhost IDENTIFIED BY '${MYSQL_DAGSTER_PASSWORD}';"
mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "GRANT ALL PRIVILEGES ON ${MYSQL_DAGSTER_DATABASE_NAME}.* TO '${MYSQL_DAGSTER_USERNAME}'@'localhost';"
mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "FLUSH PRIVILEGES;"

export DAGSTER_HOME=$CONDA_ENV_HOME

sed -i "s/DAGSTER_MYSQL_HOST/$MYSQL_HOSTNAME/" $DAGSTER_HOME/dagster.yaml
sed -i "s/DAGSTER_MYSQL_USER/$MYSQL_DAGSTER_USERNAME/" $DAGSTER_HOME/dagster.yaml
sed -i "s/DAGSTER_MYSQL_PASSWORD/$MYSQL_DAGSTER_PASSWORD/" $DAGSTER_HOME/dagster.yaml
sed -i "s/DAGSTER_MYSQL_DB/$MYSQL_DAGSTER_DATABASE_NAME/" $DAGSTER_HOME/dagster.yaml
sed -i "s/DAGSTER_MYSQL_PORT/$MYSQL_PORT/" $DAGSTER_HOME/dagster.yaml

eval "$(conda shell.bash hook)"
conda config --set default_threads 4
conda env list
# Load by default conda environment vars when running in container
# To avoid error: CommandNotFoundError: Your shell has not been properly configured to use 'conda activate'.
# source $HOME/$MINICONDA_NAME/etc/profile.d/conda.sh
##
conda install -y conda-build
conda activate $CONDA_ENV

# Adding source code to the lib path
conda develop $CONDA_ENV_HOME

conda info
echo "Finished Installing Conda [Mamba] Env $CONDA_ENV"
end_time=`date +%s`
runtime=$((end_time-start_time))
echo "Total Installation Runtime: $runtime [seconds]"
echo "Test environment not intended for using in production. Backup any changes made to this environment"
#
export DAGSTER_PORT="3000"
export HOST="0.0.0.0"
CONDA_ENVIRONMENT_FILE_NAME="conda_environment_$CONDA_ENV.sh"
echo "#!/usr/bin/env bash" > $CONDA_ENVIRONMENT_FILE_NAME
echo "export PATH=$PATH" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export JAVA_HOME=$JAVA_HOME" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export SPARK_HOME=$SPARK_HOME" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export DAGSTER_HOME=$DAGSTER_HOME" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export HOST=$HOST" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export DAGSTER_PORT=$DAGSTER_PORT" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export MYSQL_DAGSTER_USERNAME=$MYSQL_DAGSTER_USERNAME" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "export MYSQL_DAGSTER_PASSWORD=$MYSQL_DAGSTER_PASSWORD" >> $CONDA_ENVIRONMENT_FILE_NAME
echo "source $HOME/$MINICONDA_NAME/etc/profile.d/conda.sh" >> $CONDA_ENVIRONMENT_FILE_NAME
chmod +x $CONDA_ENVIRONMENT_FILE_NAME
echo "export SPARK_HOME=$SPARK_HOME"
echo "NOTEBOOK_PORT: $NOTEBOOK_PORT"
# Install and Run Notebook
## conda install -y notebook=6.5.4
export NOTEBOOK_PORT="8080"
# Install and run Dagster
echo "Going to install dagster"
conda install -y dagster=1.5.6
conda install -y dagster-mysql=1.5.6
echo "Going to install dagster-webserver"
yes | pip install dagster-webserver==1.5.6
## echo "Going to run Jupyter on host:$HOST/port:$NOTEBOOK_PORT"
## jupyter notebook --no-browser --port=$NOTEBOOK_PORT --ip=$HOST --NotebookApp.token='' --NotebookApp.password=''  --allow-root &
echo "Going to run Dagster on host:$HOST/port:$DAGSTER_PORT "
echo "Running Dagster daemon"
dagster-daemon run > dagster_daemon_logs.txt 2>&1 &
echo "Allowing for dagster-daemon to start running"
sleep 60
echo "Running Webserver"
dagster-webserver -h $HOST -p $DAGSTER_PORT > dagster_webserver_logs.txt 2>&1 &
tail -f *.txt
sleep infinity

