package MetaCPAN::Release;

use strict;
use warnings;
use v5.36;

use DateTime         ();
use File::Find::Rule ();
use File::Spec       ();
use Module::Metadata 1.000012 ();    # Improved package detection.
use Path::Tiny qw< path >;
use Try::Tiny qw< catch try >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Ingest qw<
    download_url
    fix_version
    numify_version
>;

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
        archive_path => $archive_path,
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
        if ($last) {
            last;
        }
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
                +{ map { $_ => $s->$_ } qw(mode size mtime) };
            };
            return if ( $relative eq q{.} );
            ( my $fpath = "$relative" ) =~ s/^.*?\///;
            my $filename = $fpath;
            $child->is_dir
                ? $filename =~ s/^(.*\/)?(.+?)\/?$/$2/
                : $filename =~ s/.*\///;
            $fpath = q{}
                if $relative !~ /\// && !$self->{archive}->is_impolite;

            my $file = +{
                author  => $dist->cpanid,
                binary  => -B $child,
                content => $child->is_dir ? ""
                : ( scalar $child->slurp ),
                date => DateTime->from_epoch( epoch => $child->stat->mtime )
                    . "",
                directory    => $child->is_dir,
                distribution => $dist->dist,
                indexed => $self->{metadata}->should_index_file($fpath) ? 1
                : 0,
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

    foreach my $module_name ( sort keys %$provides ) {
        my $data = $provides->{$module_name};
        my $path = File::Spec->canonpath( $data->{file} );

        # Obey no_index and take the shortest path if multiple files match.
        my ($file) = sort { length( $a->{path} ) <=> length( $b->{path} ) }
            grep { $_->{indexed} && $_->{path} =~ /\Q$path\E$/ } @$files;

        next unless $file;

        my $module = $self->module_document(
            name    => $module_name,
            version => $data->{version},
        );

        $file->{modules} //= [];
        push @{ $file->{modules} }, $module;
    }

    return;
}

sub add_modules_from_files ($self) {
    my @perl_files = grep { $_->{name} =~ m{(?:\.pm|\.pm\.PL)\z} }
        grep { $_->{indexed} } @{ $self->{files} };

    foreach my $file (@perl_files) {
        if ( $file->{name} =~ m{\.PL\z} ) {
            my $parser = Parse::PMFile->new( $self->{metadata}->as_struct );

            # FIXME: Should there be a timeout on this
            # (like there is below for Module::Metadata)?
            my $info = $parser->parse( $file->{local_path} );
            next if !$info;

            foreach my $module_name ( keys %{$info} ) {
                my $module = $self->module_document(
                    name => $module_name,
                    (
                        defined $info->{$module_name}->{version}
                        ? ( version => $info->{$module_name}->{version} )
                        : ()
                    ),
                );

                $file->{modules} //= [];
                push @{ $file->{modules} }, $module;
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

                    my $module = $self->module_document(
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
                    );

                    $file->{modules} //= [];
                    push @{ $file->{modules} }, $module;
                }
                alarm(0);
            };
        }
    }
}

sub module_document ( $self, %args ) {
    my $name    = $args{name} or die "Can't create nameless modules\n";
    my $version = $args{version} // "";

    return +{
        associated_pod   => "",
        authorized       => 1,
        indexed          => 1,
        name             => $name,
        version          => $version,
        version_numified => numify_version($version),
    };
}

sub _is_broken_file ( $self, $filename ) {
    return 1 if ( -p $filename || !-e $filename );

    if ( -l $filename ) {
        my $syml = readlink $filename;
        return 1 if ( !-e $filename && !-l $filename );
    }
    return 0;
}

1;

__END__
