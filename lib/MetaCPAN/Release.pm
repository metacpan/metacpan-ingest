package MetaCPAN::Release;

use strict;
use warnings;
use v5.36;

use DateTime         ();
use File::Find::Rule ();
use File::Spec       ();
use Path::Tiny qw< path >;
use Try::Tiny qw< catch try >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Ingest qw<
    download_url
    fix_version
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
        || CPAN::Meta->new(
        {
            license  => 'unknown',
            name     => $dist->dist,
            no_index => { directory => [@$always_no_index_dirs] },
            version  => $dist->version || 0,
        }
        );
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
    return \@files;
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
