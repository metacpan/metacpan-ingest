################### Base
# hadolint ignore=DL3007
FROM metacpan/metacpan-base:latest AS base
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

# nothing needed for now
#RUN \
#    --mount=type=cache,target=/var/cache/apt,sharing=private \
#    --mount=type=cache,target=/var/lib/apt/lists,sharing=private \
#<<EOT
#    apt-get update
#    apt-get satisfy -y -f --no-install-recommends 'some-package (>= 1.2.3)'
#EOT

WORKDIR /app/

COPY cpanfile cpanfile.snapshot ./
RUN \
    --mount=type=cache,target=/root/.perl-cpm,sharing=private \
<<EOT
    cpm install --show-build-log-on-failure --resolver=snapshot
EOT

COPY bin bin
COPY lib lib
COPY *.conf .

ENV PERL5LIB="/app/local/lib/perl5:/app/lib" PATH="/app/local/bin:${PATH}"

USER metacpan

CMD [ \
  "/bin/sh", "-c", "echo 'MetaCPAN Ingest Image'" \
]

################### Test
FROM base AS test
ENV COLUMNS="${COLUMNS:-120}"

USER root

RUN \
    --mount=type=cache,target=/root/.perl-cpm \
<<EOT
    cpm install --show-build-log-on-failure --resolver=snapshot --with-develop --with-test
    chown -R metacpan:users ./
EOT

COPY .perlcriticrc .perltidyrc perlimports.toml tidyall.ini ./
COPY t t

USER metacpan

CMD [ "prove", "-lr", "-j", "2", "t" ]

################### Production
FROM base AS production
