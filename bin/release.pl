use strict;
use warnings;

use CPAN::Meta         ();
use CPAN::DistnameInfo ();
use File::Find::Rule   ();
use File::stat         ();
use Getopt::Long;
use List::Util qw< uniq >;
use Path::Tiny qw< path >;
use PerlIO::gzip;
use Try::Tiny qw< catch try >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Archive;
use MetaCPAN::ES;
use MetaCPAN::Ingest qw<
    author_dir
    cpan_dir
    cpan_file_map
    digest
    fix_version
    handle_error
    minion
    tmp_dir
    ua
>;

use MetaCPAN::Release;

#use MetaCPAN::Model::Release  ();
#use MetaCPAN::Script::Runner  ();

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
my $ua   = ua();
my $cpan = cpan_dir();
my $es   = MetaCPAN::ES->new( type => "release" );

#my $bulk = $es->bulk( size => $bulk_size );

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
            map      { $_->{file} }
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

        my $file = tmp_dir( author_dir( $dist->cpanid ), $dist->filename, );
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
            index_release => [$file],
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
                $minion->enqueue(
                    index_latest => [ '--distribution', $dist->dist ] => {
                        attempts => 3,
                        delay    => $delay,
                        parents  => [$job_id],
                        priority => 2,
                    }
                );
            }
        }

    }
    else {
        try { _import_archive( $file, $dist ) }
        catch {
            handle_error( "$file $_[0]", 1 );
        };
    }
}
$es->index_refresh unless $queue;

# Call Fastly to purge
# purge_cpan_distnameinfos( \@module_to_purge_dists );

# subs

sub _index_files {
    my ($files) = @_;

    my $es   = MetaCPAN::ES->new( type => "file" );
    my $bulk = $es->bulk( size => $bulk_size );

    log_debug { 'Indexing ', scalar(@$files), ' files' };

    for my $f (@$files) {
        $bulk->update( {
            id => digest( $f->{author}, $f->{release}, $f->{path} )
            ,    ### ???? file name
            doc           => $f,
            doc_as_upsert => 1,
        } );
    }

    $bulk->flush;
}

sub _perms {
    my $file = $cpan->child(qw< modules 06perms.txt >);
    my %authors;
    if ( -e $file ) {
        log_debug { "parsing ", $file };
        my $fh = $file->openr;
        while ( my $line = <$fh> ) {
            my ( $module, $author, $type ) = split( /,/, $line );
            next unless ($type);
            $authors{$module} ||= [];
            push( @{ $authors{$module} }, $author );
        }
        close $fh;
    }
    else {
        log_warn {"$file could not be found."};
    }

    my $packages = $cpan->child(qw< modules 02packages.details.txt.gz >);
    if ( -e $packages ) {
        log_debug { "parsing ", $packages };
        open my $fh, "<:gzip", $packages;
        while ( my $line = <$fh> ) {
            if ( $line =~ /^(.+?)\s+.+?\s+\S\/\S+\/(\S+)\// ) {
                $authors{$1} ||= [];
                push( @{ $authors{$1} }, $2 );
            }
        }
        close $fh;
    }
    return \%authors;
}

sub _detect_status {
    my ( $author, $archive ) = @_;
    return $status unless $detect_backpan;
    if ( $cpan_file_map->{$author}{$archive} ) {
        return 'cpan';
    }
    else {
        log_debug {'BackPAN detected'};
        return 'backpan';
    }
}

sub _import_archive {
    my ( $archive_path, $dist ) = @_;

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
    my $metadata = $release->{metadata};
    my $document = $release->document_release();

    _index_files($files);

    use DDP;
    &p( [ $files->[0] ] );
    exit;
}

__END__

sub import_archive {
    my ( $archive_path ) = @_;

    my $model = $self->_get_release_model( $archive_path );

    log_debug {'Gathering modules'};

# DONE
    my $files    = $model->files;
    my $modules  = $model->modules;
    my $meta     = $model->metadata;
    my $document = $model->document;

# DONE
    # foreach my $file ( @$files ) {
    #     $file->set_indexed($meta);
    # }

    my %associated_pod;
    for ( grep { $_->indexed && $_->documentation } @$files ) {

        # $file->clear_documentation to force a rebuild
        my $documentation = $_->clear_documentation;
        $associated_pod{$documentation}
            = [ @{ $associated_pod{$documentation} || [] }, $_ ];
    }

# check for release deprecation in abstract of release or has x_deprecated in meta
    my $deprecated = (
               $meta->{x_deprecated}
            or $document->has_abstract
            and $document->abstract =~ /DEPRECI?ATED/
    ) ? 1 : 0;

    $document->_set_deprecated($deprecated);

    log_debug { 'Indexing ', scalar @$modules, ' modules' };
    my $perms = $self->perms;
    my @release_unauthorized;
    my @provides;
    foreach my $file ( @$files ) {
        $_->set_associated_pod( \%associated_pod ) for ( @{ $file->module } );

     # NOTE: "The method returns a list of unauthorized, but indexed modules."
        push( @release_unauthorized, $file->set_authorized($perms) )
            if ( keys %$perms and !$force_authorized );

        my $file_x_deprecated = 0;

        for ( @{ $file->module } ) {
            push( @provides, $_->name )
                if $_->indexed
                && ( $_->authorized || $force_authorized );
            $file_x_deprecated = 1
                if $meta->{provides}{ $_->name }{x_deprecated};
        }

        # check for DEPRECATED/DEPRECIATED in abstract of file
        $file->_set_deprecated(1)
            if $deprecated
            or $file_x_deprecated
            or $file->abstract and $file->abstract =~ /DEPRECI?ATED/;

        $file->clear_module if ( $file->is_pod_file );
        $file->documentation;
        $file->suggest;

        log_trace {"reindexing file $file->{path}"};
        $bulk->put($file);
        if ( !$document->has_abstract && $file->abstract ) {
            ( my $module = $document->distribution ) =~ s/-/::/g;
            $document->_set_abstract( $file->abstract );
            $document->put;
        }
    }
    if (@provides) {
        $document->_set_provides( [ uniq sort @provides ] );
        $document->put;
    }
    $bulk->commit;

    if (@release_unauthorized) {
        log_info {
            "release "
                . $document->name
                . " contains unauthorized modules: "
                . join( ",", map { $_->name } @release_unauthorized );
        };
        $document->_set_authorized(0);
        $document->put;
    }

    # update 'first' value
    $document->set_first;
    $document->put;

    # update 'latest' (must be done _after_ last update of the document)
    if ( $self->latest and !$self->queue ) {
        local @ARGV = ( qw< latest --distribution >, $document->distribution );
        MetaCPAN::Script::Runner->run;
    }

    my $contrib_data = $self->get_cpan_author_contributors( $document->author,
        $document->name, $document->distribution );
    $self->update_release_contirbutors($contrib_data);
}


1;

__END__

=head1 SYNOPSIS

 # bin/metacpan ~/cpan/authors/id/A
 # bin/metacpan ~/cpan/authors/id/A/AB/ABRAXXA/DBIx-Class-0.08127.tar.gz
 # bin/metacpan http://cpan.cpantesters.org/authors/id/D/DA/DAGOLDEN/CPAN-Meta-2.110580.tar.gz

 # bin/metacpan ~/cpan --age 24 --latest

=head1 DESCRIPTION

This is the workhorse of MetaCPAN. It accepts a list of folders, files or urls
and indexes the releases. Adding C<--latest> will set the status to C<latest>
for the indexed releases If you are indexing more than one release, running
L<MetaCPAN::Script::Latest> afterwards is probably faster.

C<--age> sets the maximum age of the file in hours. Will be ignored when processing
individual files or an url.

If an url is specified the file is downloaded to C<var/tmp/http/>. This folder is not
cleaned up since L<MetaCPAN::Plack::Source> depends on it to extract the source of
a file. If the archive cannot be find in the cpan mirror, it tries the temporary
folder. After a rsync this folder can be purged.

=cut
