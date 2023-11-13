FROM ubuntu:22.04
RUN apt-get update && apt-get upgrade -y
RUN apt-get install curl -y
RUN apt-get install unzip -y
RUN apt-get autoclean -y
RUN apt-get autoremove -y
RUN useradd -rm -d /home/rtdip -s /bin/bash -g root -G sudo -u 1001 rtdip
COPY run_conda_installer.sh /home/rtdip/
RUN mkdir -p /home/rtdip/apps/lfenergy
COPY environment.yml /home/rtdip/apps/lfenergy/
COPY MISO_pipeline_sample*.* /home/rtdip/apps/lfenergy/
COPY dagster.yaml /home/rtdip/apps/lfenergy/
COPY workspace.yaml /home/rtdip/apps/lfenergy/
RUN chmod +x /home/rtdip/run_conda_installer.sh
WORKDIR /home/rtdip
ENTRYPOINT ["/home/rtdip/run_conda_installer.sh"]
