ARG SLIM_BUILD
ARG MAYBE_BASE_BUILD=${SLIM_BUILD:+ingest-base-slim}
ARG BASE_BUILD=${MAYBE_BASE_BUILD:-ingest-base}

################### Base
FROM metacpan/metacpan-base:main-20250531-090128 AS ingest-base
FROM metacpan/metacpan-base:main-20250531-090129-slim AS ingest-base-slim

################### CPAN Prereqs
FROM ingest-base AS build-cpan-prereqs
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

WORKDIR /app/

COPY cpanfile cpanfile.snapshot ./
RUN \
    --mount=type=cache,target=/root/.perl-cpm,sharing=private \
<<EOT
    cpm install --show-build-log-on-failure --resolver=snapshot
EOT

################### Server
# false positive
# hadolint ignore=DL3006
FROM ${BASE_BUILD} AS ingest
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

WORKDIR /app/

COPY log4perl* metacpan_ingest.* metacpan_ingest_local.* ./
COPY conf conf
COPY scripts scripts
COPY lib lib

RUN mkdir -p var && chown metacpan var

COPY --from=build-cpan-prereqs /app/local local

ENV PERL5LIB="/app/local/lib/perl5" \
    PATH="/app/local/bin:${PATH}" \
    METACPAN_INGEST_HOME=/app

VOLUME /CPAN

USER metacpan

CMD [ \
  "/bin/sh", "-c", "echo 'MetaCPAN Ingest Image'" \
]

################### Dev Prereqs
FROM build-cpan-prereqs AS build-dev-prereqs
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

USER root

RUN \
    --mount=type=cache,target=/root/.perl-cpm \
<<EOT
    cpm install --show-build-log-on-failure --resolver=snapshot --with-develop --with-test
EOT

COPY bin/install-precious /tmp/install-precious
RUN /tmp/install-precious /usr/local/bin

################### Development
FROM ingest AS ingest-dev

COPY --from=build-dev-prereqs /app/local local
COPY --from=build-dev-prereqs /usr/local/bin/precious /usr/local/bin/omegasort /usr/local/bin/

COPY .perlcriticrc .perltidyrc perlimports.toml precious.toml .editorconfig metacpan_ingest_testing.* ./
COPY t t
COPY test_data test_data

USER root
RUN chown -R metacpan /app/local
USER metacpan

ENV PERL5LIB="/app/lib:${PERL5LIB}" \
    PATH="/app/local/scripts:${PATH}"
