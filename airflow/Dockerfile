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
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY ./requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
USER root
RUN mkdir /label-inspector
ADD ./label-inspector/ /label-inspector
USER airflow
RUN pip install /label-inspector/
# Force cache creation, could be simplified by just in a python one-liner importing the same thing as in create_merged.py
RUN mkdir /home/airflow/data
USER root
RUN chmod g+w /home/airflow/data
RUN chmod g+w /home/airflow/.local/lib/python3.10/site-packages/namehash_common/
USER airflow
RUN python -m namehash_common.generate_cache
