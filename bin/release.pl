use strict;
use warnings;
use v5.36;

use CPAN::DistnameInfo ();
use File::Find::Rule   ();
use File::stat         ();
use Getopt::Long;
use List::Util qw< uniq >;
use Path::Tiny qw< path >;
use Try::Tiny  qw< catch try >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Archive;
use MetaCPAN::Contributor qw< update_release_contirbutors >;
use MetaCPAN::ES;
use MetaCPAN::File;
use MetaCPAN::Ingest qw<
    cpan_file_map
    digest
    handle_error
    minion
    read_02packages_fh
    read_06perms_fh
    tmp_dir
    ua
>;

use MetaCPAN::Release;

my @skip_dists = (

    # specific dists to skip.  it's ugly to have all of these hard coded, but
    # it works for now
    qw<
        AEPAGE/perl5.00502Tk800.011-win32-586.zip
        ANDYD/perl5.002b1h.tar.gz
        BMIDD/perl5.004_02-AlphaNTPreComp.tar.gz
        BMIDD/perl5.00402-bindist04-msvcAlpha.tar.gz
        BMIDD/perl5.00402-bindist05-msvcAlpha.tar.gz
        GRABZIT/perl.2.3.0.zip
        GSAR/perl5.00401-bindist02-bc.tar.gz
        GSAR/perl5.00401-bindist-bc.tar.gz
        GSAR/perl5.00402-bindist03-bc.tar.gz
        GSAR/perl5.00402-bindist04-bc.tar.gz
        GSAR/perl5.00402-bindist04-bc.zip
        HOOO/perl-0.0017.tar.gz
        JBAKER/perl-5.005_02+apache1.3.3+modperl-1.16-bin-bindist1-i386-win32-vc5.zip
        KRISHPL/perl-5.6-info.tar.gz
        LMOLNAR/perl5.00402-bindist01-dos-djgpp.zip
        LMOLNAR/perl5.00503-bin-1-dos-djgpp.zip
        MSCHWERN/perl-1.0_15.tar.gz
        RCLAMP/perl-1.0_16.tar.gz
        SREZIC/perl-5.005-basicmods-bin-0-arm-linux.tar.gz
        SREZIC/perl-5.005-minimal-bin-0-arm-linux.tar.gz
        SREZIC/perl-5.005-minimal-bin-1-arm-linux.tar.gz
        SREZIC/perl-5.005-Tk-800.023-bin-0-arm-linux.tar.gz
    >,

# ILYAZ has lots of old weird os2 files that don't fit as dists or perl releases
    qr{/ILYAZ/os2/[^/]+/perl_\w+\.zip\z},
    qr{/ILYAZ/os2/perl[^/]+\.zip\z},

    # Strip off any files in a Perl6 or Raku folder
    # e.g. http://www.cpan.org/authors/id/J/JD/JDV/Perl6/
    # As here we are indexing perl5 only
    qr{/(?:Perl6|Raku)/},
);

my ($SKIP_MATCH) = map qr/$_/, join '|',
    map +( ref $_ ? $_ : qr{/\Q$_\E\z} ), @skip_dists;

my @always_no_index_dirs = (

    # Always ignore the same dirs as PAUSE (lib/PAUSE/dist.pm):
    ## skip "t" - libraries in ./t are test libraries!
    ## skip "xt" - libraries in ./xt are author test libraries!
    ## skip "inc" - libraries in ./inc are usually install libraries
    ## skip "local" - somebody shipped his carton setup!
    ## skip 'perl5" - somebody shipped her local::lib!
    ## skip 'fatlib' - somebody shipped their fatpack lib!
    qw< t xt inc local perl5 fatlib >,

    # and add a few more
    qw< example blib examples eg >,
);

# args
my ( $age, $bulk_size, $detect_backpan, $force_authorized, $latest, $queue,
    $skip, $status );
GetOptions(
    "age=i"            => \$age,
    "bulk_size=i"      => \$bulk_size,
    "detect_backpan"   => \$detect_backpan,
    "force_authorized" => \$force_authorized,
    "latest"           => \$latest,
    "queue"            => \$queue,
    "skip"             => \$skip,
    "status"           => \$status,
);
$status //= 'cpan';

# setup
my $ua = ua();
my $es = MetaCPAN::ES->new( type => "release" );

my $minion;
$minion = minion() if $queue;

# run

my $perms = _perms();

my @files;
for (@ARGV) {
    if ( -d $_ ) {
        log_info {"Looking for archives in $_"};
        my $find = File::Find::Rule->new->file->name(
            qr/\.(tgz|tbz|tar[\._-]gz|tar\.bz2|tar\.Z|zip|7z)$/);
        $find = $find->mtime( ">" . ( time - $age * 3600 ) )
            if $age;
        push( @files,
            map { $_->{file} }
                sort { $a->{mtime} <=> $b->{mtime} }
                map  { +{ file => $_, mtime => File::stat::stat($_)->mtime } }
                $find->in($_) );
    }
    elsif ( -f $_ ) {
        push( @files, $_ );
    }
    elsif ( $_ =~ /^https?:\/\//
        && CPAN::DistnameInfo->new($_)->cpanid )
    {
        my $dist = CPAN::DistnameInfo->new($_);
        my $file = tmp_dir( $dist->cpanid, $dist->filename );    # ?
        $file->parent->mkpath;
        log_info {"Downloading $_"};

        $ua->parse_head(0);
        $ua->timeout(30);
        $ua->mirror( $_, $file );
        if ( -e $file ) {
            push( @files, $file );
        }
        else {
            log_error {"Downloading $_ failed"};
        }
    }
    else {
        log_error {"Dunno what $_ is"};
    }
}

@files = grep $_ !~ $SKIP_MATCH, @files;

log_info { scalar @files, " archives found" } if ( @files > 1 );

# build here before we fork

# Going to purge everything as not sure about the 'skip' or fork
# logic - feel free to clean up so the CP::DistInfo isn't
my @module_to_purge_dists = map { CPAN::DistnameInfo->new($_) } @files;

my $cpan_file_map;
$cpan_file_map = cpan_file_map if $detect_backpan;

my @pid;

eval { DB::enable_profile() };
while ( my $file = shift @files ) {
    my $dist = CPAN::DistnameInfo->new($file);

    if ($skip) {
        my $count = $es->count(
            body => {
                query => {
                    bool => {
                        must => [
                            { term => { archive => $dist->filename } },
                            { term => { author  => $dist->cpanid } },
                        ]
                    }
                }
            },
        );

        if ( $count->{count} ) {
            log_info {"Skipping $file"};
            next;
        }
    }

    if ($queue) {
        my $job_id = $minion->enqueue(
            index_release => [ $file->{data} ],
            { attempts => 3, priority => 3 }
        );

        # This is a hack to deal with the fact that we don't know exactly
        # when 02packages gets updated.  As of 2019-04-08, 02packages is
        # updated via a cron which runs every 12 minutes, with the
        # exception of one run which is skipped, resulting in a 24 minute
        # gap.  The run usually takes less than one minute.  We could stop
        # trying once something is already "latest", but some uploads will
        # never be "latest".  Trying this X times should be fairly cheap.
        # If this doesn't work, there is a cleanup cron which can set the
        # "latest" flag, if necessary.

        if ($latest) {
            for my $delay ( 2 * 60, 7 * 60, 14 * 60, 26 * 60 ) {
                queue_latest( $dist, $delay, $job_id );
            }
        }
    }
    else {
        try { _import_archive( $file, $dist ) }
        catch {
            handle_error( 1, "$file $_[0]" );
        };
    }
}

$es->index_refresh unless $queue;

# Call Fastly to purge
# purge_cpan_distnameinfos( \@module_to_purge_dists );

# subs

sub _index_release ($document) {
    log_debug { 'Indexing release ', $document->{name} };
    $es->index(
        id   => $document->{name},
        body => $document,
    );
}

sub _index_files ($files) {
    my $es   = MetaCPAN::ES->new( type => "file" );
    my $bulk = $es->bulk( size => $bulk_size );

    log_debug { 'Indexing ', scalar(@$files), ' files' };

    for my $f (@$files) {
        $bulk->update( {
            id => digest( $f->{author}, $f->{release}, $f->{path} )
            ,    ### ???? file name
            doc           => $f->as_struct,
            doc_as_upsert => 1,
        } );
    }

    $bulk->flush;
}

sub _detect_status ( $author, $archive ) {
    return $status unless $detect_backpan;
    if ( $cpan_file_map->{$author}{$archive} ) {
        return 'cpan';
    }
    else {
        log_debug {'BackPAN detected'};
        return 'backpan';
    }
}

sub _import_archive ( $archive_path, $dist ) {
    log_debug {'Gathering modules'};

    my $author = $dist->cpanid;
    my $status
        = $detect_backpan
        ? _detect_status( $author, $archive_path )
        : $status;

    # move creation of arc_data into the module ?
    my $release = MetaCPAN::Release->new(
        always_no_index_dirs => \@always_no_index_dirs,
        archive_path         => $archive_path,
        author               => $author,
        dist_info            => $dist,
        status               => $status,
    );

    my $files    = $release->files;
    my $modules  = $release->modules;
    my $metadata = $release->{metadata};
    my $document = $release->document_release();

# check for release deprecation in abstract of release or has x_deprecated in meta
    my $deprecated = (
               $metadata->{x_deprecated}
            or $document->{abstract}
            and $document->{abstract} =~ /DEPRECI?ATED/
    ) ? 1 : 0;

    $document->{deprecated} = $deprecated;

    log_debug { sprintf( 'Indexing %d modules', scalar(@$modules) ) };

    my @release_unauthorized;
    my @provides;

    ### TODO: check the effect of not running the builder for 'indexed'
    ###       (we already set the flag in the logic creating the 'doc')

    my %associated_pod;

    # TODO: why here and not on object creation ?
    for my $file (@$files) {
        $file->add_documentation();
    }

    for my $file ( grep { $_->{documentation} && $_->{indexed} } @$files ) {
        $associated_pod{ $file->{documentation} }
            = [ @{ $associated_pod{ $file->{documentation} } || [] }, $file ];
    }

    for my $file (@$files) {
        _set_associated_pod( $_, \%associated_pod ) for @{ $file->{modules} };

     # NOTE: "The method returns a list of unauthorized, but indexed modules."
        push @release_unauthorized, $file->set_authorized($perms)
            if keys %$perms and !$force_authorized;

        my $file_x_deprecated = 0;

        for my $mod ( @{ $file->{modules} } ) {
            push( @provides, $mod->{name} )
                if $mod->{indexed}
                && ( $mod->{authorized} || $force_authorized );
            $file_x_deprecated = 1
                if $metadata->{provides}{ $mod->{name} }{x_deprecated};
        }

        # check for DEPRECATED/DEPRECIATED in abstract of file
        $file->set_deprecated(1)
            if $deprecated
            or $file_x_deprecated
            or ( $file->{abstract} and $file->{abstract} =~ /DEPRECI?ATED/ );

        $file->empty_modules() if $file->_is_pod_file();
        $file->set_suggest();

        log_trace {"reindexing file $file->{path}"};

        if ( !$document->{abstract} && $file->{abstract} ) {
            ( my $module = $document->{distribution} ) =~ s/-/::/g;
            $document->{abstract} = $file->{abstract};
        }
    }

    $document->{provides} = [ uniq sort @provides ]
        if scalar(@provides);

    if ( scalar(@release_unauthorized) ) {
        log_info {
            "release "
                . $document->{name}
                . " contains unauthorized modules: "
                . join( ",", map { $_->{name} } @release_unauthorized );
        };
        $document->{authorized} = 0;
    }

    # update 'first' value
    _set_first($document);

    update_release_contirbutors($document);

    _index_release($document);
    _index_files($files);

    # update 'latest' (must be done _after_ last update of the document)
    #   flag for all releases of the distribution.
    if ( $document->{latest} and !$queue ) {
        log_info {"Upding latest status"};

#     local @ARGV = ( qw< latest --distribution >, $document->{distribution} );
#     MetaCPAN::Script::Runner->run;
    }
}

sub _set_associated_pod ( $module, $associated_pod ) {
    return unless ( my $files = $associated_pod->{ $module->{name} } );

    my %_pod_score = ( pod => 50, pm => 40, pl => 30 );

    ( my $mod_path = $module->{name} ) =~ s{::}{/}g;

    my ($file) = (
        #<<<
        # TODO: adjust score if all files are in root?
        map  { $_->[1] }
        sort { $b->[0] <=> $a->[0] }    # desc
        map  {
            [ (
                # README.pod in root should rarely if ever be chosen.
                # Typically it's there for github or something and it's usually
                # a duplicate of the main module pod (though sometimes it falls
                # out of sync (which makes it even worse)).
                $_->{path} =~ /^README\.pod$/i ? -10 :

                # If the name of the package matches the name of the file,
                $_->{path} =~ m!(^lib/)?\b${mod_path}.((?i)pod|pm)$! ?
                    # Score pod over pm, and boost (most points for 'lib' dir).
                    ($1 ? 50 : 25) + $_pod_score{lc($2)} :

                # Sort files by extension: Foo.pod > Foo.pm > foo.pl.
                $_->{name} =~ /\.(pod|pm|pl)/i ? $_pod_score{lc($1)} :

                # Otherwise score unknown (near the bottom).
                -1
            ),
            $_ ]
         }
         @$files
         #>>>
    );

    $module->{associated_pod} = $file->full_path;
}

sub _set_first ($document) {
    my $count = $es->search(
        search_type => 'count',
        body        => {
            query  => { match_all => {} },
            filter => {
                and => [
                    { term => { distribution => $document->{distribution} } },
                    {
                        range => {
                            version_numified =>
                                { 'lt' => $document->{version_numified} }
                        },
                    }
                ],
            },
        },
    )->{hits}{total};

    # REINDEX: after a full reindex, the above line is to replaced with:
    # { term => { first => 1 } },
    # currently, the "first" property is not computed on all releases
    # since this feature has not been around when last reindexed

    $document->{first} = ( $count > 0 ? 0 : 1 );
}

sub queue_latest ( $dist, $delay, $job_id ) {
    $minion->enqueue(
        index_latest => [ '--distribution', $dist->dist ] => {
            attempts => 3,
            delay    => $delay,
            parents  => [$job_id],
            priority => 2,
        }
    );
}

sub _perms () {
    my $fh_perms = read_06perms_fh();
    my %authors;

    log_debug {"Reading 06perms"};
    while ( my $line = <$fh_perms> ) {
        my ( $module, $author, $type ) = split( /,/, $line );
        next unless ($type);
        $authors{$module} ||= [];
        push( @{ $authors{$module} }, $author );
    }
    close $fh_perms;

    log_debug {"Reading 02packages"};
    my $fh_packages = read_02packages_fh();
    while ( my $line = <$fh_packages> ) {
        next unless $line =~ /^(.+?)\s+.+?\s+\S\/\S+\/(\S+)\//;
        $authors{$1} ||= [];
        push( @{ $authors{$1} }, $2 );
    }
    close $fh_packages;

    return \%authors;
}

1;

__END__

=head1 SYNOPSIS

 # bin/release ~/cpan/authors/id/A
 # bin/release ~/cpan/authors/id/A/AB/ABRAXXA/DBIx-Class-0.08127.tar.gz
 # bin/release http://cpan.cpantesters.org/authors/id/D/DA/DAGOLDEN/CPAN-Meta-2.110580.tar.gz

 # bin/release ~/cpan --age 24 --latest

=head1 DESCRIPTION

This is the workhorse of MetaCPAN. It accepts a list of folders, files or urls
and indexes the releases. Adding C<--latest> will set the status to C<latest>
for the indexed releases If you are indexing more than one release, running
L<latest> afterwards is probably faster.

C<--age> sets the maximum age of the file in hours. Will be ignored when processing
individual files or an url.

If an url is specified the file is downloaded to C<var/tmp/http/>. This folder is not
cleaned up since L<MetaCPAN::Plack::Source> depends on it to extract the source of
a file. If the archive cannot be find in the cpan mirror, it tries the temporary
folder. After a rsync this folder can be purged.

=cut
