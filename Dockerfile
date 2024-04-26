# hadolint ignore=DL3007
FROM metacpan/metacpan-base:latest AS ingest
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

RUN \
    --mount=type=cache,target=/var/cache/apt,sharing=private \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=private \
<<EOT
    apt-get update
    # nothing needed for now
    # apt-get satisfy -y -f --no-install-recommends 'some-package (>= 1.2.3)'
EOT

WORKDIR /metacpan-ingest/

COPY cpanfile cpanfile.snapshot ./
RUN \
    --mount=type=cache,target=/root/.perl-cpm,sharing=private \
<<EOT /bin/bash -euo pipefail
    cpm install --show-build-log-on-failure
EOT

COPY bin bin
COPY lib lib

ENV PERL5LIB="/metacpan-ingest/local/lib/perl5:/metacpan-ingest/lib" PATH="/metacpan-ingest/local/bin:${PATH}"

FROM ingest AS test
SHELL [ "/bin/bash", "-euo", "pipefail", "-c" ]

RUN \
    --mount=type=cache,target=/root/.perl-cpm \
<<EOT /bin/bash -euo pipefail
    cpm install --show-build-log-on-failure --with-test
EOT

COPY .perlcriticrc .perltidyrc perlimports.toml tidyall.ini ./
COPY t t

USER metacpan
ENTRYPOINT [ "prove", "-lr" ]
CMD [ "t" ]

# 
FROM ingest AS ingest-author

USER metacpan

CMD [ "bin/author.pl" ]

FROM ingest AS ingest-permission

USER metacpan

CMD [ "bin/permission.pl" ]
