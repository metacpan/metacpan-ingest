NAME
    metacpan-ingest - Ingest CPAN metadata into Elasticsearch for MetaCPAN

DESCRIPTION
    metacpan-ingest processes CPAN distributions and indexes their metadata
    into Elasticsearch. It extracts archive contents, parses distribution
    metadata (META.json/META.yml), identifies Perl modules and their POD,
    checks PAUSE permissions, and maintains the Elasticsearch indices that
    power MetaCPAN search and the MetaCPAN API.

PREREQUISITES
    * Perl 5.36 or later
    * Elasticsearch 8.x
    * A local CPAN mirror (e.g. via minicpan or rsync)
    * Docker and docker-compose (recommended for development)

    Key Perl dependencies (see cpanfile for the full list):
        Archive::Any
        CPAN::DistnameInfo
        Cpanel::JSON::XS
        Module::Metadata
        Parse::CPAN::Packages::Fast
        PAUSE::Permissions
        Path::Tiny
        Search::Elasticsearch

INSTALLATION
    Using Docker (recommended):

        docker-compose build
        docker-compose run ingest env PLACK_ENV=dev prove -lv t/

    Without Docker, install dependencies with cpanm:

        cpanm --installdeps .

CONFIGURATION
    metacpan_ingest.yaml
        Main configuration file. Contains Elasticsearch node addresses,
        CPAN mirror paths, and API keys. The relevant keys are:

            cpan              Path to the local CPAN mirror
            cpan_test         Path used when INGEST_TEST=1
            elasticsearch_servers:
              node            Default ES node
              test_node       Node used when PLACK_ENV=dev
              production_node Node used when METACPAN_INGEST_ES_PROD=1
              client          Search::Elasticsearch client class

    Environment variables:
        PLACK_ENV=dev
            Use the test Elasticsearch node and test CPAN data.

        INGEST_TEST=1
            Use test_data/fakecpan/ instead of the real CPAN mirror.

        METACPAN_INGEST_ES_PROD=1
            Connect to the production Elasticsearch node.

RUNNING TESTS
    Against a running Elasticsearch instance (e.g. inside Docker):

        prove -lv t/

    Via Docker (spins up Elasticsearch automatically):

        docker-compose build
        docker-compose run ingest env PLACK_ENV=dev prove -lv t/

LINTING AND FORMATTING
    Code style is enforced by precious (https://github.com/houseabsolute/precious).
    Install it via bin/install-precious, then run:

        precious tidy --all     # reformat all files
        precious lint --all     # check without modifying

    Tools configured:
        perltidy     78-char line limit, 4-space indents (.perltidyrc)
        perlcritic   Strict linting (.perlcriticrc)
        perlimports  Organised import lists (perlimports.toml)
        omegasort    Sorted .gitignore files

ARCHITECTURE
    Data flow:

        CPAN Distributions (.tar.gz / .zip)
          -> scripts/release.pl
          -> MetaCPAN::Archive     (extracts archives)
          -> MetaCPAN::Release     (parses META.json/META.yml, finds modules)
          -> MetaCPAN::File        (indexes individual files, extracts POD)
          -> MetaCPAN::ES          (Elasticsearch client)
          -> ES indices: release, file, author, permission, ...

    Elasticsearch index mappings and settings live under conf/es/<index>/.
    There is one subdirectory per index (release, file, author, cover, cve,
    distribution, favorite, mirror, package, permission, account, session,
    contributor, etc.) each containing mapping.json and settings.json.

MODULES
    lib/MetaCPAN/Archive.pm
        Wraps Archive::Any to extract distribution archives and compute
        MD5/SHA-256 checksums. Prefers /mnt/scratch_disk for extraction.

    lib/MetaCPAN/Contributor.pm
        Resolves and indexes distribution contributors. Deduplicates by
        email, matches contributors to PAUSE IDs, and upserts records into
        the contributor index.

    lib/MetaCPAN/ES.pm
        Thin wrapper around Search::Elasticsearch bound to a named index.
        Provides helpers for searching, scrolling, bulk indexing, and
        waiting for cluster availability.

    lib/MetaCPAN/File.pm
        Represents a single file from an extracted distribution. Handles
        MIME detection, Perl file identification, POD extraction for the
        documentation name, and PAUSE authorization checking.

    lib/MetaCPAN/Ingest.pm
        Shared utility library. Provides configuration loading, CPAN file
        I/O (00whois, 02packages, 06perms, RECENT files), version
        normalisation, and miscellaneous helpers. All functions are
        exportable.

    lib/MetaCPAN/Mapper.pm
        Manages the Elasticsearch index lifecycle: creation, deletion, and
        mapping updates. Reads mappings and settings from conf/es/.

    lib/MetaCPAN/Release.pm
        Extracts a distribution archive, loads its CPAN::Meta metadata,
        discovers Perl modules (via Module::Metadata or Parse::PMFile), and
        builds Elasticsearch document structures for the release and its
        files.

SCRIPTS
    scripts/author.pl
        Ingests CPAN author records from authors/00whois.xml into the
        author index.

    scripts/backpan.pl
        Marks releases that have been removed from CPAN as "backpan" status
        by comparing against the find-ls.gz file map.

    scripts/backup.pl
        Backs up Elasticsearch index data to JSON.gz files under var/backup,
        or restores from a previous backup. Also supports purging old backups.

    scripts/check.pl
        Validates indexed module data against 02packages.details.txt.gz,
        reporting inconsistencies.

    scripts/checksum.pl
        Backfills missing MD5 and SHA-256 checksums on release records by
        reading the actual archive files from the CPAN mirror.

    scripts/contributor.pl
        Indexes CPAN author contributors for distributions, matching
        contributor email addresses to PAUSE IDs.

    scripts/cover.pl
        Fetches CPAN Cover test coverage data from cpancover.com and updates
        the cover index.

    scripts/cpan_testers_api.pl
        Fetches CPAN Testers pass/fail summary data from the CPAN Testers
        API and updates matching release records.

    scripts/cve.pl
        Fetches CPAN security advisory (CVE) data from the cpansa-feed and
        indexes it into the cve index.

    scripts/favorite.pl
        Ingests MetaCPAN user favourite counts into the distribution and
        release indices.

    scripts/first.pl
        Sets the "first" release date on distribution records, indicating
        when a distribution first appeared on CPAN.

    scripts/latest.pl
        Marks the latest release for each distribution by reading
        02packages.details.txt.gz and updating the status field.

    scripts/mapping.pl
        Creates or deletes an Elasticsearch index and applies its mapping
        and settings from conf/es/<index>/.

    scripts/mirrors.pl
        Ingests the CPAN mirror list from indices/mirrors.json into the
        mirror index.

    scripts/package.pl
        Ingests 02packages.details.txt.gz line by line into the package
        index, tracking module-to-distribution mappings.

    scripts/permission.pl
        Ingests PAUSE upload permissions from modules/06perms.txt into the
        permission index.

    scripts/purge.pl
        Removes all release and file documents for a given PAUSE author
        (or a specific release) from Elasticsearch.

    scripts/queue.pl
        Enqueues distribution archive files or directories for ingestion
        via the Minion job queue.

    scripts/release.pl
        Main ingestion script. Accepts paths, files, or URLs to distribution
        archives and indexes them end-to-end: extraction, metadata parsing,
        module discovery, permission checking, and ES indexing.

    scripts/restart.pl
        Requests a rolling restart of the Elasticsearch cluster.

    scripts/river.pl
        Fetches the CPAN River of Code dataset (distribution dependency
        counts) from neilb.org and updates the distribution index.

    scripts/session.pl
        Purges MetaCPAN user session records older than one month from the
        session index.

    scripts/snapshot.pl
        Manages Elasticsearch snapshots to an AWS S3 bucket. Supports
        repository setup, creating snapshots, listing, restoring, and
        purging old snapshots.

    scripts/suggest.pl
        Populates the Elasticsearch autocomplete suggestion field on file
        documents, either for a date range or all documents.

    scripts/tickets.pl
        Fetches open issue counts from RT (rt.cpan.org) and GitHub for
        each distribution and stores them in the distribution index.

    scripts/watcher.pl
        Watches CPAN RECENT-*.json files for new and deleted releases and
        triggers ingestion or BackPAN marking accordingly. Runs in a
        continuous loop.

TEST DATA
    test_data/fakecpan/ is a minimal mock CPAN mirror used when
    INGEST_TEST=1:

        authors/00whois.xml                    Mock author database
        modules/02packages.details.txt.gz      Mock package index
        modules/06perms.txt                    Mock PAUSE permissions
        authors/id/O/OA/OALDERS/               Sample distribution archives

CI
    GitHub Actions (.github/workflows/build-container.yml):
        Builds the test Docker image, runs the test suite against
        Elasticsearch, and pushes the production image to Docker Hub on
        merges to master, staging, and prod branches.

    CircleCI (.circleci/config.yml):
        Full docker-compose integration tests with Devel::Cover code
        coverage, uploading results to Codecov.
