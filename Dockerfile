FROM rocker/r-base

RUN apt-get update \
	&& apt-get install -y \
		libxml2-dev \
		libcurl4-gnutls-dev \
		python \
		sqlite3 \
		libsqlite3-dev \
		python-pip

COPY /01-ObtainData.R .

CMD ["Rscript", "01-ObtainData.R"]
