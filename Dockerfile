FROM r-base

RUN apt-get update && apt-get install -y --no-install-recommends \
		curl \
		libssl-dev \
		libcurl4-openssl-dev \
		libxml2-dev \
	&& rm -r /var/lib/apt/lists/*

RUN Rscript -e 'install.packages(c("devtools", "testthat", "xml2", "Rcpp", "base64enc"))'
RUN Rscript -e 'install.packages(c("testthat", "aws.s3", "AzureStor"))'

COPY . /code/
WORKDIR /code/

RUN R CMD build .
#RUN R CMD check "keboola.sapi.r.client_0.4.0.tar.gz" --as-cran --no-manual
