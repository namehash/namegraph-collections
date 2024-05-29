FROM apache/airflow:2.6.3-python3.10
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libgflags-dev \
         libsnappy-dev \
         zlib1g-dev \
         libbz2-dev \
         liblz4-dev \
         libzstd-dev \
         git-core \
         wget \
         lbzip2 \
         g++ \
         pv \
         pigz \
         apt-transport-https \
         ca-certificates \
         curl \
         gnupg2 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo echo $(lsb_release -cs)
RUN echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         docker-ce \
         docker-compose-plugin \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY ./requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
USER root
RUN mkdir /namerank
ADD ./namerank/ /namerank
USER airflow
RUN pip install /namerank/
# Force cache creation, could be simplified by just in a python one-liner importing the same thing as in create_merged.py
RUN mkdir /home/airflow/data
USER root
RUN chmod g+w /home/airflow/data
RUN chmod g+w /home/airflow/.local/lib/python3.10/site-packages/label_inspector/common/cache
USER airflow
RUN python -m label_inspector.common.generate_cache
USER root
COPY sql_parser.c /home/airflow
RUN g++ -o /home/airflow/.local/bin/sql_parser /home/airflow/sql_parser.c
USER airflow
