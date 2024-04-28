package MetaCPAN::Release;

use strict;
use warnings;
use v5.36;

use DateTime         ();
use File::Find::Rule ();
use File::Spec       ();
use List::AllUtils qw< any >;
use Module::Metadata 1.000012 ();    # Improved package detection.
use Path::Tiny qw< path >;
use Plack::MIME ();
use Try::Tiny qw< catch try >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Ingest qw<
    download_url
    fix_version
    numify_version
    strip_pod
>;

my @NOT_PERL_FILES = qw(SIGNATURE);

sub new ( $class, %args ) {
    my $author       = $args{author}       or die "Missing author\n";
    my $archive_path = $args{archive_path} or die "Missing archive_path\n";
    my $dist_info    = $args{dist_info}    or die "Missing dist_info\n";
    my $status       = $args{status}       or die "Missing status\n";

    my $always_no_index_dirs = [
        $args{always_no_index_dirs}
        ? @{ $args{always_no_index_dirs} }
        : ()
    ];

    my $archive  = _extract_archive( $archive_path, $dist_info );
    my $metadata = _metadata( $archive->{extract_dir},
        $dist_info, $always_no_index_dirs );

    return bless {
        author       => $author,
        archive      => $archive,
        archive_path => path($archive_path),
        dist_info    => $dist_info,
        download_url => download_url( $author, $archive_path ),
        extract_dir  => $archive->{extract_dir},
        metadata     => $metadata,
        status       => $status,
        version      => fix_version( $dist_info->version ),
    }, $class;
}

sub _extract_archive ( $archive_path, $dist ) {
    my $archive = MetaCPAN::Archive->new( file => $archive_path );
    log_debug {'Extracting archive to filesystem'};

    $archive->extract;

    return $archive;
}

sub _metadata ( $extract_dir, $dist, $always_no_index_dirs ) {
    return _load_meta_file( $extract_dir, $always_no_index_dirs )
        || CPAN::Meta->new( {
        license  => 'unknown',
        name     => $dist->dist,
        no_index => { directory => [@$always_no_index_dirs] },
        version  => $dist->version || 0,
        } );
}

sub _load_meta_file ( $extract_dir, $always_no_index_dirs ) {
    my @files;
    for (
        qw< */META.json */META.yml */META.yaml META.json META.yml META.yaml >)
    {
        # scalar context globbing (without exhausting results) produces
        # confusing results (which caused existsing */META.json files to
        # get skipped).  using list context seems more reliable.
        my ($path) = <$extract_dir/$_>;
        push( @files, $path ) if ( $path && -e $path );
    }
    return unless (@files);

    my $last;
    for my $file (@files) {
        try {
            $last = CPAN::Meta->load_file($file);
        }
        catch {
            log_warn {"META file ($file) could not be loaded: $_"};
        };

        last if $last;
    }

    if ($last) {
        push( @{ $last->{no_index}->{directory} }, $always_no_index_dirs );
        return $last;
    }

    log_warn {'No META files could be loaded'};
}

sub files ($self) {
    return $self->{files} if $self->{files} and @{ $self->{files} };

    my $dist = $self->{dist_info};

    my @files;
    File::Find::find(
        sub {
            my $child = path($File::Find::name);
            return if $self->_is_broken_file($File::Find::name);
            my $relative = $child->relative( $self->{extract_dir} );
            my $stat     = do {
                my $s = $child->stat;
                +{ map { $_ => $s->$_ } qw< mode size mtime > };
            };
            return if ( $relative eq q{.} );
            ( my $fpath = "$relative" ) =~ s/^.*?\///;
            my $filename = $fpath;
            $child->is_dir
                ? $filename =~ s/^(.*\/)?(.+?)\/?$/$2/
                : $filename =~ s/.*\///;
            $fpath = q{}
                if $relative !~ /\// && !$self->{archive}->is_impolite;

            my $file = $self->document_file(
                child    => $child,
                dist     => $dist,
                filename => $filename,
                fpath    => $fpath,
                stat     => $stat
            );

            push( @files, $file );
        },
        $self->{extract_dir}
    );

    $self->{files} = \@files;

    # add modules
    if ( keys %{ $self->{metadata}->provides } ) {
        $self->add_modules_from_meta;
    }
    else {
        $self->add_modules_from_files;
    }

    return \@files;
}

sub add_modules_from_meta ($self) {
    my $provides = $self->{metadata}->provides;
    my $files    = $self->{files};

    my @modules;

    foreach my $module_name ( sort keys %$provides ) {
        my $data = $provides->{$module_name};
        my $path = File::Spec->canonpath( $data->{file} );

        # Obey no_index and take the shortest path if multiple files match.
        my ($file) = sort { length( $a->{path} ) <=> length( $b->{path} ) }
            grep { $_->{indexed} && $_->{path} =~ /\Q$path\E$/ } @$files;

        next unless $file;

        my $module = $self->document_module(
            name    => $module_name,
            version => $data->{version},
            indexed =>
                1, # modules explicitly listed in 'provides' should be indexed
        );

        $file->{modules} //= [];
        push @{ $file->{modules} }, $module;
        push @modules, $file;
    }

    $self->{modules} = \@modules;
    return;
}

sub add_modules_from_files ($self) {
    my $meta = $self->{metadata};

    my @modules;

    my @perl_files = grep { $_->{name} =~ m{(?:\.pm|\.pm\.PL)\z} }
        grep { $_->{indexed} } @{ $self->{files} };

    foreach my $file (@perl_files) {
        if ( $file->{name} =~ m{\.PL\z} ) {
            my $parser = Parse::PMFile->new( $meta->as_struct );

            # FIXME: Should there be a timeout on this
            # (like there is below for Module::Metadata)?
            my $info = $parser->parse( $file->{local_path} );
            next if !$info;

            foreach my $module_name ( keys %{$info} ) {
                my $indexed
                    = $self->_should_index_module( $module_name, $file );

                my $module = $self->document_module(
                    name => $module_name,
                    (
                        defined $info->{$module_name}->{version}
                        ? ( version => $info->{$module_name}->{version} )
                        : ()
                    ),
                    indexed => $indexed,
                );

                $file->{modules} //= [];
                push @{ $file->{modules} }, $module;
                push @modules, $file;
            }
        }
        else {
            eval {
                local $SIG{'ALRM'} = sub {
                    log_error {'Call to Module::Metadata timed out '};
                    die;
                };
                alarm(50);
                my $info;
                {
                    local $SIG{__WARN__} = sub { };
                    $info = Module::Metadata->new_from_file(
                        $file->{local_path} );
                }

          # Ignore packages that people cannot claim.
          # https://github.com/andk/pause/blob/master/lib/PAUSE/pmfile.pm#L236
                for my $pkg ( grep { $_ ne 'main' && $_ ne 'DB' }
                    $info->packages_inside )
                {
                    my $version = $info->version($pkg);

                    my $indexed = $self->_should_index_module( $pkg, $file );

                    my $module = $self->document_module(
                        name => $pkg,
                        (
                            defined $version

# Stringify if it's a version object, otherwise fall back to stupid stringification
# Changes in Module::Metadata were causing inconsistencies in the return value,
# we are just trying to survive.
                            ? (
                                version => (
                                    ref $version eq 'version'
                                    ? $version->stringify
                                    : ( $version . q{} )
                                )
                                )
                            : ()
                        ),
                        indexed => $indexed,
                    );

                    $file->{modules} //= [];
                    push @{ $file->{modules} }, $module;
                    push @modules, $file;
                }
                alarm(0);
            };
        }
    }

    $self->{modules} = \@modules;
    return;
}

sub modules ($self) { $self->{modules} }

sub document_file ( $self, %args ) {
    my ( $child, $dist, $filename, $fpath, $stat )
        = @args{qw< child dist filename fpath stat >};

    my $indexed = $self->_should_index_file( $filename, $fpath );

    my $documnet = DlogS_trace {"adding file $_"} +{
        author  => $dist->cpanid,
        binary  => -B $child,
        content => $child->is_dir ? "" : ( scalar $child->slurp ),
        date    => DateTime->from_epoch( epoch => $child->stat->mtime ) . "",
        directory    => $child->is_dir,
        distribution => $dist->dist,
        indexed      => $indexed,
        local_path   => $child . "",
        maturity     => $dist->maturity,
        metadata     => $self->{metadata}->as_struct,
        name         => $filename,
        path         => $fpath,
        release      => $dist->distvname,
        download_url => $self->{download_url},
        stat         => $stat,
        status       => $self->{status},
        version      => $self->{version},
    };

    $self->_add_mime($documnet);

    return $documnet;
}

sub document_module ( $self, %args ) {
    my $name    = $args{name} or die "Can't create nameless modules\n";
    my $version = $args{version} // "";
    my $indexed = $args{indexed} // 1;

    my $document = DlogS_trace {"adding module $_"} +{
        associated_pod   => "",
        authorized       => 1,
        indexed          => $indexed,
        name             => $name,
        version          => $version,
        version_numified => numify_version($version),
    };
}

sub document_release ( $self, %args ) {
    my $st   = $self->{archive_path}->stat;
    my $stat = { map { $_ => $st->$_ } qw< mode size mtime > };
    my $dist = $self->{dist_info};
    my $meta = $self->{metadata};

    my $document = DlogS_trace {"adding release $_"} +{
        abstract        => strip_pod( $meta->abstract ),
        archive         => $self->{archive_path}->stringify,
        author          => $self->{author},
        checksum_md5    => $self->{archive}->file_digest_md5,
        checksum_sha256 => $self->{archive}->file_digest_sha256,
        date         => DateTime->from_epoch( epoch => $stat->{mtime} ) . "",
        dependency   => $self->dependencies,
        distribution => $dist->dist,

        # CPAN::Meta->license *must* be called in list context
        # (and *may* return multiple strings).
        license  => [ $meta->license ],
        maturity => $dist->maturity,
        metadata => $meta->as_struct,
        name     => $dist->distvname,
        provides => [],
        stat     => $stat,
        status   => $self->{status},

# Call in scalar context to make sure we only get one value (building a hash).
        ( map { ( $_ => scalar $meta->$_ ) } qw< version resources > ),
    };

    return $document;
}

sub dependencies ($self) {
    my $meta = $self->{metadata};

    log_debug {'Gathering dependencies'};
    my @dependencies;

    if ( my $prereqs = $meta->prereqs ) {
        while ( my ( $phase, $data ) = each %$prereqs ) {
            while ( my ( $relationship, $v ) = each %$data ) {
                while ( my ( $module, $version ) = each %$v ) {
                    push(
                        @dependencies,
                        Dlog_trace {"adding dependency $_"} +{
                            phase        => $phase,
                            relationship => $relationship,
                            module       => $module,
                            version      => $version,
                        }
                    );
                }
            }
        }
    }

    log_debug { 'Found ', scalar @dependencies, ' dependencies' };
    return \@dependencies;
}

sub _is_broken_file ( $self, $filename ) {
    return 1 if ( -p $filename || !-e $filename );

    if ( -l $filename ) {
        my $syml = readlink $filename;
        return 1 if ( !-e $filename && !-l $filename );
    }
    return 0;
}

sub _is_in_other_files ( $self, $file ) {
    my @other = qw<
        AUTHORS
        Build.PL
        Changelog
        ChangeLog
        CHANGELOG
        Changes
        CHANGES
        CONTRIBUTING
        CONTRIBUTING.md
        CONTRIBUTING.pod
        Copying
        COPYRIGHT
        cpanfile
        CREDITS
        dist.ini
        FAQ
        INSTALL
        INSTALL.md
        INSTALL.pod
        LICENSE
        Makefile.PL
        MANIFEST
        META.json
        META.yml
        NEWS
        README
        README.md
        README.pod
        THANKS
        Todo
        ToDo
        TODO
    >;

    return any { $file eq $_ } @other;
}

my $bom
    = qr/(?:\x00\x00\xfe\xff|\xff\xfe\x00\x00|\xfe\xff|\xff\xfe|\xef\xbb\xbf)/;

sub hide_from_pause ( $self, $content, $file_name, $pkg ) {
    return 0 if defined($file_name) && $file_name =~ m{\.pm\.PL\z};

    #    my $pkg = $self->name;

# This regexp is *almost* the same as $PKG_REGEXP in Module::Metadata.
# [b] We need to allow/ignore a possible BOM since we read in binary mode.
# Module::Metadata, for example, checks for a BOM and then sets the encoding.
# [s] We change `\s` to `\h` because we want to verify that it's on one line.
# [p] We replace $PKG_NAME_REGEXP with the specific package we're looking for.
# [v] Simplify the optional whitespace/version group ($V_NUM_REGEXP).
    return $content =~ /    # match a package declaration
      ^                     # start of line
       (?:\A$bom)?          # possible BOM at the start of the file [b]
       [\h\{;]*             # intro chars on a line [s]
      package               # the word 'package'
      \h+                   # whitespace [s]
      (\Q$pkg\E)            # a package name [p]
      (\h+ v?[0-9._]+)?     # optional version number (preceded by whitespace) [v]
      \h*                   # optional whitesapce [s]
      [;\{]                 # semicolon line terminator or block start
    /mx ? 0 : 1;
}

sub _should_index_module ( $self, $name, $file ) {
    return 0 if !$file->{indexed};
    return 0 if $name !~ /^[A-Za-z]/;
    return 0 if !$self->{metadata}->should_index_package($name);
    return 0
        if $self->hide_from_pause( $file->{content}, $file->{name}, $name );

    return 1;
}

sub _should_index_file ( $self, $file, $fpath ) {
    return 0 if !$self->{metadata}->should_index_file($fpath);

    # files listed under 'other files' are not shown in a search
    return 0 if $self->_is_in_other_files($file);

    # files under no_index directories should not be indexed
    return 0
        if grep { $fpath eq $_ or $fpath =~ m|^$_/| }
        @{ $self->{metadata}->no_index->{directory} };

    return 1;
}

sub _add_mime ( $self, $file ) {
    my $mime;

    if (  !$file->{directory}
        && $file->{name} !~ /\./
        && grep { $file->{name} ne $_ } @NOT_PERL_FILES )
    {
        $mime = "text/x-script.perl" if ( $file->{content} =~ /^#!.*?perl/ );
    }
    else {
        $mime = Plack::MIME->mime_type( $file->{name} ) || 'text/plain';
    }

    $file->{mime} = $mime;
}

1;

__END__
