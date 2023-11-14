FROM ubuntu:22.04
RUN apt-get update && apt-get upgrade -y
RUN apt-get install curl -y
RUN apt-get install unzip -y
RUN apt-get autoclean -y
RUN apt-get autoremove -y
RUN useradd -rm -d /home/rtdip -s /bin/bash -g root -G sudo -u 1001 rtdip
COPY run_conda_installer.sh /home/rtdip/
RUN mkdir -p /home/rtdip/apps/local_scheduled
COPY environment.yml /home/rtdip/apps/local_scheduled/
COPY dagster_src /home/rtdip/apps/local_scheduled/
COPY dagster.yaml /home/rtdip/apps/local_scheduled/
COPY workspace.yaml /home/rtdip/apps/local_scheduled/
COPY check_output.ipynb /home/rtdip/apps/local_scheduled/
RUN chmod +x /home/rtdip/run_conda_installer.sh
RUN mkdir -p /home/rtdip/apps/local_scheduled/edgex_test_warehouse
WORKDIR /home/rtdip
ENTRYPOINT ["/home/rtdip/run_conda_installer.sh"]
