FROM r-base

RUN apt-get update && apt-get install -y -y --no-install-recommends \
		curl \
		libssl-dev \
		libcurl4-openssl-dev \
		libxml2-dev \
	&& rm -r /var/lib/apt/lists/*

RUN Rscript -e 'install.packages(c("devtools", "testthat", "xml2", "Rcpp", "base64enc"))'
RUN Rscript -e 'install.packages("testthat")'
# the CRAN version of aws.s3 does not work, but the master does work, i do not wish to refer to 'master' here for repeatability, so 4738bb refers to last commit to master at the time of this writing, there is no significane to it.
RUN Rscript -e 'devtools::install_github("cloudyr/aws.s3", build_vignettes = FALSE, keep_source = TRUE, force = TRUE, ref = "3d2b2e17b92255a73935514ee19fc7553fd80fae")'

COPY . /code/
WORKDIR /code/

RUN R CMD build .
#RUN R CMD check "keboola.sapi.r.client_0.4.0.tar.gz" --as-cran --no-manual
